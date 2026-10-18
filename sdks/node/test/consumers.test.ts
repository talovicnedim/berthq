import { describe, expect, it } from 'vitest';
import { drain, openClient, uniqueName } from './helpers.js';

const client = openClient();

describe('consumers', () => {
	it('gives every consumer every message, at its own pace', async () => {
		const name = uniqueName('fanout');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }]);

		expect(await drain(client, name, 'billing')).toEqual([1, 2]);
		expect(await drain(client, name, 'audit')).toEqual([1, 2]);
		expect(await drain(client, name, 'billing')).toEqual([]);

		const consumers = await client.consumers.list(name);
		expect(consumers.map((consumer) => consumer.name)).toEqual(['audit', 'billing']);
		expect((await client.queues.get(name)).consumers).toHaveLength(2);
	});

	it('starts new consumers at from, and replays', async () => {
		const name = uniqueName('replay');
		await client.queues.create(name);
		const { id: first } = await client.publish(name, { body: 'first' });
		await client.publish(name, { body: 'second' });

		expect((await client.consume(name, { consumer: 'late', from: 'end' })).messages).toEqual([]);
		await client.publish(name, { body: 'third' });
		expect(await drain(client, name, 'late')).toEqual(['third']);

		const replayed = await client.consumers.replay(name, 'late', 'start');
		expect(replayed.stats.waiting).toBe(3);
		expect(await drain(client, name, 'late')).toEqual(['first', 'second', 'third']);

		await client.consumers.replay(name, 'late', first);
		expect(await drain(client, name, 'late')).toEqual(['first', 'second', 'third']);
		await client.consumers.replay(name, 'late', 'end');
		expect(await drain(client, name, 'late')).toEqual([]);
	});

	it('creates, gets, pauses, resumes and removes a consumer', async () => {
		const name = uniqueName('consumers');
		await client.queues.create(name);
		await client.publish(name, { body: 'kept' });

		const created = await client.consumers.create(name, 'billing', 'end');
		expect(created).toMatchObject({
			queue: name,
			name: 'billing',
			paused: false,
			stats: { waiting: 0, processing: 0, retrying: 0 },
			idle: false,
			lastSeen: null,
			workers: []
		});
		expect(created).not.toHaveProperty('webhook');
		await expect(client.consumers.create(name, 'billing')).rejects.toMatchObject({
			kind: 'conflict',
			status: 409
		});

		expect((await client.consumers.pause(name, 'billing')).paused).toBe(true);
		await client.publish(name, { body: 'held' });
		expect((await client.consume(name, { consumer: 'billing' })).messages).toEqual([]);
		expect((await client.consumers.resume(name, 'billing')).paused).toBe(false);
		const [delivery] = (await client.consume(name, { consumer: 'billing', worker: 'b1' })).messages;
		expect(delivery.body).toBe('held');

		const seen = await client.consumers.get(name, 'billing');
		expect(seen.lastSeen).not.toBeNull();
		expect(seen.workers.map((worker) => worker.name)).toContain('b1');

		await client.consumers.remove(name, 'billing');
		await expect(client.consumers.get(name, 'billing')).rejects.toMatchObject({
			kind: 'notFound'
		});
		// The lease from before the removal is gone.
		expect((await client.ack(name, [delivery.lease!])).unknown).toEqual([delivery.lease]);
	});

	it('lists what a consumer is processing, retrying and gets next', async () => {
		const name = uniqueName('consumer-messages');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }, { body: 3 }]);
		const [first, second] = (await client.consume(name, { max: 2 })).messages;
		await client.nack(name, { leases: [first.lease!], error: 'later', delayMs: 60_000 });

		const processing = await client.consumers.messages(name, 'default', { state: 'processing' });
		expect(processing.map((message) => message.id)).toEqual([second.id]);
		const [retrying] = await client.consumers.messages(name, 'default', { state: 'retrying' });
		expect(retrying).toMatchObject({ id: first.id, attempts: 1, lastError: 'later' });
		expect(Date.parse(retrying.retryAt!)).toBeGreaterThan(Date.now() + 30_000);
		const waiting = await client.consumers.messages(name, 'default', {
			state: 'waiting',
			limit: 10
		});
		expect(waiting).toHaveLength(1);
		expect((await client.consumers.get(name, 'default')).stats).toEqual({
			waiting: 1,
			processing: 1,
			retrying: 1
		});
	});
});
