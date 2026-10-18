import { describe, expect, it } from 'vitest';
import { BerthqError, Client } from '../src/index.js';
import { openClient, uniqueName } from './helpers.js';

const client = openClient();

describe('errors', () => {
	it('throws BerthqError with kind, status and message', async () => {
		const name = uniqueName('errors');
		await client.queues.create(name);
		const error = await client.queues.create(name).catch((err: unknown) => err);
		expect(error).toBeInstanceOf(BerthqError);
		expect(error).toBeInstanceOf(Error);
		expect(error).toMatchObject({ name: 'BerthqError', kind: 'conflict', status: 409 });
		expect((error as BerthqError).message).toContain(name);

		await expect(client.publish(name, { body: 1, priority: 42 })).rejects.toMatchObject({
			kind: 'invalid',
			status: 400
		});
		await expect(client.queues.create('bad name!')).rejects.toMatchObject({ kind: 'invalid' });
		await expect(client.consumers.replay(name, 'nobody', 'start')).rejects.toMatchObject({
			kind: 'notFound'
		});
	});

	it('reports queueFull', async () => {
		const name = uniqueName('full');
		await client.queues.create(name, { maxMessages: 1 });
		await client.publish(name, { body: 1 });
		await expect(client.publish(name, { body: 2 })).rejects.toMatchObject({
			kind: 'queueFull',
			status: 507
		});
	});

	it('reports an unreachable broker as kind http with status 0', async () => {
		const offline = new Client('http://127.0.0.1:1');
		await expect(offline.stats()).rejects.toMatchObject({ kind: 'http', status: 0 });
	});

	it('uses a custom fetch', async () => {
		const urls: string[] = [];
		const spy = openClient({
			fetch: (input, init) => {
				urls.push(String(input));
				return fetch(input, init);
			}
		});
		const stats = await spy.stats();
		expect(stats.version).toBeTruthy();
		expect(urls).toEqual([`${spy.url}/v1/stats`]);
	});
});
