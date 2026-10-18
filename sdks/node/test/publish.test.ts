import { describe, expect, it } from 'vitest';
import { openClient, uniqueName } from './helpers.js';

const client = openClient();

describe('publish and consume', () => {
	it('round-trips a message through publish, consume and ack', async () => {
		const name = uniqueName('roundtrip');
		await client.queues.create(name);
		const published = await client.publish(name, {
			body: { user: 123 },
			priority: 5,
			dedupId: 'user-123'
		});
		expect(published.duplicate).toBe(false);

		const [delivery] = (await client.consume<{ user: number }>(name, { max: 10, worker: 'test' }))
			.messages;
		expect(delivery).toMatchObject({
			id: published.id,
			queue: name,
			consumer: 'default',
			body: { user: 123 },
			priority: 5,
			attempt: 1,
			dedupId: 'user-123',
			visibilityTimeoutMs: 30_000,
			lastError: null
		});
		expect(delivery).not.toHaveProperty('headers');
		const lease = delivery.lease!;

		expect(await client.ack(name, [lease])).toEqual({ settled: 1, unknown: [] });
		// A settled lease is reported as unknown, not an error.
		expect(await client.ack(name, [lease])).toEqual({ settled: 0, unknown: [lease] });
		expect((await client.consume(name)).messages).toEqual([]);
	});

	it('acks inside consume', async () => {
		const name = uniqueName('consume-ack');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }]);
		const [first] = (await client.consume(name)).messages;
		const lease = first.lease!;

		const next = await client.consume(name, { ack: [lease] });
		expect(next).toMatchObject({ settled: 1, unknown: [] });
		expect(next.messages.map((message) => message.body)).toEqual([2]);
		expect(await client.consume(name, { ack: [lease] })).toEqual({
			messages: [],
			settled: 0,
			unknown: [lease]
		});
	});

	it('publishes every message of a publisher once, in order', async () => {
		const name = uniqueName('publisher');
		await client.queues.create(name);
		const publisher = client.publisher(name);
		const numbers = Array.from({ length: 250 }, (_, n) => n);
		const results = await Promise.all(numbers.map((n) => publisher.publish({ body: n })));
		await publisher.close();
		expect(new Set(results.map((result) => result.id)).size).toBe(250);

		const bodies: unknown[] = [];
		for (;;) {
			const { messages } = await client.consume(name, { max: 100, autoAck: true });
			if (messages.length === 0) break;
			bodies.push(...messages.map((message) => message.body));
		}
		expect(bodies).toEqual(numbers);
	});

	it('long-polls until a message arrives', async () => {
		const name = uniqueName('longpoll');
		await client.queues.create(name);
		const consumed = client.consume(name, { waitMs: 5_000 });
		setTimeout(() => void client.publish(name, { body: 'late' }), 200);
		const [delivery] = (await consumed).messages;
		expect(delivery.body).toBe('late');
	});

	it('cancels a long-poll with an AbortSignal', async () => {
		const name = uniqueName('abort');
		await client.queues.create(name);
		const controller = new AbortController();
		const consumed = client.consume(name, { waitMs: 10_000 }, controller.signal);
		setTimeout(() => controller.abort(), 100);
		await expect(consumed).rejects.toMatchObject({ name: 'AbortError' });
	});

	it('extends a lease', async () => {
		const name = uniqueName('extend');
		await client.queues.create(name);
		await client.publish(name, { body: 'slow' });
		const [delivery] = (await client.consume(name, { visibilityTimeoutMs: 1_000, worker: 'w1' }))
			.messages;
		expect(delivery.visibilityTimeoutMs).toBe(1_000);
		const extended = await client.extend(name, [delivery.lease!], 60_000);
		expect(extended).toEqual({ settled: 1, unknown: [] });
		const [processing] = await client.consumers.messages(name, 'default');
		expect(processing).toMatchObject({ id: delivery.id, state: 'processing', worker: 'w1' });
		expect(Date.parse(processing.leaseExpiresAt!)).toBeGreaterThan(Date.now() + 30_000);
	});

	it('auto-acks: no lease, the position moves right away', async () => {
		const name = uniqueName('autoack');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }]);
		const deliveries = (await client.consume(name, { max: 10, autoAck: true })).messages;
		expect(
			deliveries.map((delivery) => [
				delivery.body,
				delivery.lease,
				delivery.leaseExpiresAt,
				delivery.visibilityTimeoutMs
			])
		).toEqual([
			[1, null, null, 0],
			[2, null, null, 0]
		]);
		expect((await client.consume(name)).messages).toEqual([]);
		expect((await client.consumers.get(name, 'default')).stats).toEqual({
			waiting: 0,
			processing: 0,
			retrying: 0
		});
	});

	it('deduplicates by dedupId', async () => {
		const name = uniqueName('dedup');
		await client.queues.create(name);
		const first = await client.publish(name, { body: 1, dedupId: 'order-1' });
		const second = await client.publish(name, { body: 2, dedupId: 'order-1' });
		expect(second).toEqual({ id: first.id, duplicate: true });
		expect(await client.messages.list(name)).toHaveLength(1);
	});

	it('publishes a batch in order', async () => {
		const name = uniqueName('batch');
		await client.queues.create(name);
		const results = await client.publishBatch(name, [
			{ body: 'a' },
			{ body: 'b', dedupId: 'x' },
			{ body: 'c', dedupId: 'x' }
		]);
		expect(results).toHaveLength(3);
		expect(results.map((result) => result.duplicate)).toEqual([false, false, true]);
		const deliveries = (await client.consume(name, { max: 10 })).messages;
		expect(deliveries.map((delivery) => delivery.body)).toEqual(['a', 'b']);
	});
});
