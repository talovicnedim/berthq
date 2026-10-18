import { describe, expect, it } from 'vitest';
import { BerthqError } from '../src/index.js';
import { fakeBroker } from './fake.js';

const LONG = 60_000;

describe('publisher', () => {
	it('sends a batch as soon as maxBatch messages are waiting', async () => {
		const broker = fakeBroker();
		const publisher = broker.client.publisher('jobs', { maxBatch: 3, lingerMs: LONG });
		const results = [0, 1, 2, 3, 4, 5, 6].map((n) => publisher.publish({ body: n }));

		await Promise.all(results.slice(0, 6));
		expect(broker.batches()).toEqual([
			[0, 1, 2],
			[3, 4, 5]
		]);
		await publisher.flush();
		expect(broker.batches()).toEqual([[0, 1, 2], [3, 4, 5], [6]]);
		expect(await results[6]).toEqual({ id: 'id-6', duplicate: false });
	});

	it('waits lingerMs for more messages before sending', async () => {
		const broker = fakeBroker();
		const publisher = broker.client.publisher('jobs', { lingerMs: 30 });
		const started = Date.now();
		const results = [publisher.publish({ body: 'a' }), publisher.publish({ body: 'b' })];
		expect(broker.requests).toEqual([]);

		await Promise.all(results);
		expect(Date.now() - started).toBeGreaterThanOrEqual(25);
		expect(broker.batches()).toEqual([['a', 'b']]);
	});

	it('cuts batches before maxBytes and sends a bigger message alone', async () => {
		const broker = fakeBroker();
		// {"body":"s1"} is 13 bytes, so two small messages fit in 30 and a third does not.
		const publisher = broker.client.publisher('jobs', { maxBytes: 30, lingerMs: LONG });
		const big = 'x'.repeat(100);
		const results = ['s1', 's2', 's3', big, 's4'].map((body) => publisher.publish({ body }));

		await publisher.flush();
		await Promise.all(results);
		expect(broker.batches()).toEqual([['s1', 's2'], ['s3'], [big], ['s4']]);
	});

	it('keeps one batch in flight, so messages keep their order', async () => {
		const broker = fakeBroker({ delayMs: 5 });
		const publisher = broker.client.publisher('jobs', { maxBatch: 2, lingerMs: 0 });
		const numbers = Array.from({ length: 10 }, (_, n) => n);

		const results = await Promise.all(numbers.map((n) => publisher.publish({ body: n })));
		expect(results.map((result) => result.id)).toEqual(numbers.map((n) => `id-${n}`));
		expect(broker.maxInFlight()).toBe(1);
		expect(broker.batches().flat()).toEqual(numbers);
	});

	it('splits an invalid batch until the bad message is alone, so only it fails', async () => {
		const broker = fakeBroker();
		const publisher = broker.client.publisher('jobs', { lingerMs: LONG });
		const results = ['a', 'bad', 'c'].map((body) => publisher.publish({ body }));
		await publisher.flush();

		expect(await results[0]).toEqual({ id: 'id-a', duplicate: false });
		await expect(results[1]).rejects.toMatchObject({ kind: 'invalid', status: 400 });
		expect(await results[2]).toEqual({ id: 'id-c', duplicate: false });
		expect(broker.batches()).toEqual([['a', 'bad', 'c'], ['a'], ['bad', 'c'], ['bad'], ['c']]);
	});

	it('fails the whole batch on other errors', async () => {
		const broker = fakeBroker();
		const publisher = broker.client.publisher('jobs', { lingerMs: LONG });
		const results = ['x', 'full'].map((body) => publisher.publish({ body }));
		await publisher.flush();

		for (const result of results) await expect(result).rejects.toMatchObject({ kind: 'queueFull' });
		expect(broker.batches()).toEqual([['x', 'full']]);
	});

	it('fails the whole batch when the broker answers too few results', async () => {
		const broker = fakeBroker();
		const publisher = broker.client.publisher('jobs', { lingerMs: LONG });
		const results = ['a', 'short'].map((body) => publisher.publish({ body }));
		await publisher.flush();

		for (const result of results) {
			await expect(result).rejects.toMatchObject({
				kind: 'http',
				message: 'the broker answered 1 results for 2 messages'
			});
		}
	});

	it('sends bodies as plain UTF-8 without needless escapes', async () => {
		const broker = fakeBroker();
		const body = { text: 'é</>&' };
		await broker.client.publish('jobs', { body });
		await broker.client.publisher('jobs').publish({ body });

		expect(broker.requests.map((request) => request.json)).toEqual([
			'{"body":{"text":"é</>&"}}',
			'{"messages":[{"body":{"text":"é</>&"}}]}'
		]);
	});

	it('flushes on close and refuses messages after it', async () => {
		const broker = fakeBroker();
		const publisher = broker.client.publisher('jobs', { lingerMs: LONG });
		const result = publisher.publish({ body: 'last' });
		await publisher.close();

		expect(await result).toEqual({ id: 'id-last', duplicate: false });
		const late = await publisher.publish({ body: 'late' }).catch((err: unknown) => err);
		expect(late).toBeInstanceOf(BerthqError);
		expect(late).toMatchObject({ kind: 'invalid' });
		expect(broker.batches()).toEqual([['last']]);
	});

	it('resolves flush right away when nothing is waiting', async () => {
		const broker = fakeBroker();
		await broker.client.publisher('jobs').flush();
		expect(broker.requests).toEqual([]);
	});
});
