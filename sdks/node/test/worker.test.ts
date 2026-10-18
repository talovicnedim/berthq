import { describe, expect, it } from 'vitest';
import type { Delivery } from '../src/index.js';
import { eventually, openClient, uniqueName } from './helpers.js';

const client = openClient();
const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

describe('process', () => {
	it('acks messages the handler finishes', async () => {
		const name = uniqueName('worker');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }, { body: 3 }]);

		const controller = new AbortController();
		const seen: number[] = [];
		const worker = client.process<number>(
			name,
			async (message) => {
				seen.push(message.body);
				if (seen.length === 3) controller.abort();
			},
			{ concurrency: 2, signal: controller.signal, worker: 'test-worker' }
		);
		await worker;

		expect(seen.sort()).toEqual([1, 2, 3]);
		const queue = await client.queues.get(name);
		expect(queue.stats.backlog).toBe(0);
		expect(queue.stats.counters.acked).toBe(3);
	});

	it('works for a named consumer, next to other consumers', async () => {
		const name = uniqueName('named');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 'a' }, { body: 'b' }]);

		const controller = new AbortController();
		const seen: unknown[] = [];
		await client.process(
			name,
			(message) => {
				seen.push([message.consumer, message.body]);
				if (seen.length === 2) controller.abort();
			},
			{ consumer: 'billing', signal: controller.signal }
		);

		expect(seen).toEqual([
			['billing', 'a'],
			['billing', 'b']
		]);
		// Another consumer still gets both messages.
		expect((await client.consume(name, { consumer: 'audit', max: 10 })).messages.length).toBe(2);
	});

	it('auto-acks without settling, and reports handler errors', async () => {
		const name = uniqueName('autoack');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 'ok' }, { body: 'boom' }]);

		const controller = new AbortController();
		const errors: unknown[] = [];
		let handled = 0;
		await client.process(
			name,
			(message) => {
				handled += 1;
				if (handled === 2) controller.abort();
				if (message.body === 'boom') throw new Error('it exploded');
			},
			{ autoAck: true, signal: controller.signal, onError: (error) => errors.push(error) }
		);

		expect(errors).toMatchObject([{ message: 'it exploded' }]);
		const consumer = await client.consumers.get(name, 'default');
		expect(consumer.stats).toEqual({ waiting: 0, processing: 0, retrying: 0 });
		expect((await client.queues.get(name)).deadLetterQueue).toBeNull();
	});

	it('nacks with the error message when the handler throws', async () => {
		const name = uniqueName('failing');
		await client.queues.create(name, { maxAttempts: 1 });
		await client.publish(name, { body: 'boom' });

		const controller = new AbortController();
		const worker = client.process(
			name,
			() => {
				controller.abort();
				throw new Error('handler exploded');
			},
			{ signal: controller.signal }
		);
		await worker;

		const [dead] = await client.messages.list(`${name}.dlq`);
		expect(dead.lastError).toBe('handler exploded');
		expect(dead.deadLetter?.reason).toBe('maxAttempts');
	});

	it('extends the lease while a slow handler runs', async () => {
		const name = uniqueName('slow');
		await client.queues.create(name, { visibilityTimeoutMs: 600 });
		await client.publish(name, { body: 'slow' });

		const controller = new AbortController();
		const attempts: number[] = [];
		const worker = client.process(
			name,
			async (message) => {
				attempts.push(message.attempt);
				// Three lease lengths: without extending, the message would be redelivered.
				await sleep(1_800);
				controller.abort();
			},
			{ concurrency: 2, signal: controller.signal }
		);
		await worker;

		expect(attempts).toEqual([1]);
		expect((await client.queues.get(name)).stats.counters.acked).toBe(1);
	});

	it('stops on abort after finishing in-flight messages', async () => {
		const name = uniqueName('stop');
		await client.queues.create(name);
		await client.publish(name, { body: 'in flight' });

		const controller = new AbortController();
		let started: Delivery | undefined;
		let finished = false;
		const worker = client.process(
			name,
			async (message) => {
				started = message;
				await sleep(300);
				finished = true;
			},
			{ signal: controller.signal }
		);
		await eventually(async () => started);
		controller.abort();
		await worker;

		expect(finished).toBe(true);
		expect((await client.queues.get(name)).stats.backlog).toBe(0);
	});

	it('stops promptly while idle in a long-poll', async () => {
		const name = uniqueName('idle');
		await client.queues.create(name);
		const controller = new AbortController();
		const worker = client.process(name, () => undefined, { signal: controller.signal });
		await sleep(100);
		const stoppedAt = Date.now();
		controller.abort();
		await worker;
		expect(Date.now() - stoppedAt).toBeLessThan(1_000);
	});

	it('backs off and retries when the broker is unavailable', async () => {
		const name = uniqueName('unavailable');
		let failures = 0;
		// The first consume answers like a broker that is shutting down.
		const flaky = openClient({
			fetch: (input, init) => {
				if (String(input).endsWith('/consume') && failures === 0) {
					failures += 1;
					const body = { error: { kind: 'unavailable', message: 'shutting down' } };
					return Promise.resolve(Response.json(body, { status: 503 }));
				}
				return fetch(input, init);
			}
		});
		await client.queues.create(name);
		await client.publish(name, { body: 'eventually' });

		const controller = new AbortController();
		const startedAt = Date.now();
		await flaky.process(name, () => controller.abort(), { signal: controller.signal });

		expect(failures).toBe(1);
		expect(Date.now() - startedAt).toBeGreaterThanOrEqual(900);
		expect((await client.queues.get(name)).stats.backlog).toBe(0);
	});

	it('rejects when the queue does not exist', async () => {
		await expect(client.process(uniqueName('missing'), () => undefined)).rejects.toMatchObject({
			kind: 'notFound'
		});
	});
});
