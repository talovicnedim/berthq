import { describe, expect, it } from 'vitest';
import { drain, openClient, uniqueName } from './helpers.js';

const client = openClient();

describe('queues', () => {
	it('creates, gets, updates, pauses and removes a queue', async () => {
		const name = uniqueName('queues');
		const created = await client.queues.create(name, { maxAttempts: 3, retentionMs: 60_000 });
		expect(created.name).toBe(name);
		expect(created.settings).toEqual({
			retentionMs: 60_000,
			visibilityTimeoutMs: 30_000,
			maxAttempts: 3,
			retryDelayMs: 1_000,
			maxMessages: null,
			paused: false
		});
		// The dead-letter queue is created on demand, when the first message fails for good.
		expect(created.deadLetterQueue).toBeNull();

		const listed = await client.queues.list();
		expect(listed.map((queue) => queue.name)).toContain(name);

		const details = await client.queues.get(name);
		expect(details.consumers).toEqual([]);
		expect(Array.isArray(details.history)).toBe(true);

		const updated = await client.queues.update(name, {
			visibilityTimeoutMs: 5_000,
			maxMessages: 9
		});
		expect(updated.settings).toMatchObject({ visibilityTimeoutMs: 5_000, maxMessages: 9 });
		const unlimited = await client.queues.update(name, { maxMessages: 0 });
		expect(unlimited.settings.maxMessages).toBeNull();

		expect((await client.queues.pause(name)).settings.paused).toBe(true);
		expect((await client.queues.resume(name)).settings.paused).toBe(false);

		await client.queues.remove(name);
		await expect(client.queues.get(name)).rejects.toMatchObject({ kind: 'notFound', status: 404 });
	});

	it('purges messages', async () => {
		const name = uniqueName('purge');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }]);
		expect(await client.queues.purge(name)).toBe(2);
		expect((await client.consume(name)).messages).toEqual([]);
	});

	it('counts messages, backlog and delayed ones', async () => {
		const name = uniqueName('stats');
		await client.queues.create(name);
		await client.publishBatch(name, [{ body: 1 }, { body: 2 }, { body: 3, delayMs: 60_000 }]);
		const before = (await client.queues.get(name)).stats;
		expect(before).toMatchObject({ messages: 3, backlog: 2, delayed: 1, processing: 0 });

		expect(await drain(client, name, 'default')).toEqual([1, 2]);
		// Acked messages stay for the queue's retention, so a replay can bring them back.
		const after = (await client.queues.get(name)).stats;
		expect(after).toMatchObject({ messages: 3, backlog: 0, consumers: 1 });
		expect(after.counters.acked).toBe(2);
	});
});

describe('messages', () => {
	it('lists, gets and removes messages', async () => {
		const name = uniqueName('messages');
		await client.queues.create(name);
		const { id } = await client.publish(name, { body: { big: true }, delayMs: 60_000 });
		const { id: now } = await client.publish(name, { body: 'now' });

		expect(await client.messages.list(name, { state: 'delayed' })).toMatchObject([
			{ id, state: 'delayed', body: null }
		]);
		expect(await client.messages.list(name, { limit: 1 })).toMatchObject([
			{ id: now, state: 'visible', attempts: 0, deadLetter: null }
		]);
		expect((await client.messages.get(name, id)).body).toEqual({ big: true });

		await client.messages.remove(name, id);
		await expect(client.messages.get(name, id)).rejects.toMatchObject({ kind: 'notFound' });
	});

	it('pages back in time with before', async () => {
		const name = uniqueName('paging');
		await client.queues.create(name);
		const published = await client.publishBatch(name, [{ body: 1 }, { body: 2 }, { body: 3 }]);
		const [newest] = await client.messages.list(name, { limit: 1 });
		expect(newest.id).toBe(published[2].id);
		const older = await client.messages.list(name, { before: newest.id });
		expect(older.map((message) => message.id)).toEqual([published[1].id, published[0].id]);
	});
});

describe('stats', () => {
	it('reports totals and free disk space', async () => {
		await client.queues.create(uniqueName('stats'));
		const stats = await client.stats();
		expect(stats.queues).toBeGreaterThan(0);
		expect(stats.disk.free).toBeGreaterThan(0);
		expect(stats.disk.minFree).toBeGreaterThan(0);
		expect(stats.disk.full).toBe(false);
	});
});
