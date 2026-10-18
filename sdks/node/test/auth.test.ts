import { describe, expect, it } from 'vitest';
import { BerthqError } from '../src/index.js';
import { ADMIN_KEY, PUBLISH_KEY } from './broker.js';
import { authClient, uniqueName } from './helpers.js';

describe('api keys', () => {
	it('sends the key as a bearer token', async () => {
		const admin = authClient(ADMIN_KEY);
		const name = uniqueName('auth');
		await admin.queues.create(name);
		expect((await admin.queues.get(name)).name).toBe(name);
	});

	it('rejects a missing or unknown key with unauthorized (401)', async () => {
		await expect(authClient().queues.list()).rejects.toMatchObject({
			kind: 'unauthorized',
			status: 401
		});
		const error = await authClient('notarealkeyatall')
			.stats()
			.catch((err: unknown) => err);
		expect(error).toBeInstanceOf(BerthqError);
		expect(error).toMatchObject({ kind: 'unauthorized', status: 401 });
	});

	it('rejects a key without the right scope with forbidden (403)', async () => {
		const name = uniqueName('scoped');
		await authClient(ADMIN_KEY).queues.create(name);
		const publisher = authClient(PUBLISH_KEY);
		expect((await publisher.publish(name, { body: 'ok' })).duplicate).toBe(false);
		await expect(publisher.consume(name)).rejects.toMatchObject({
			kind: 'forbidden',
			status: 403
		});
		await expect(publisher.queues.list()).rejects.toMatchObject({ kind: 'forbidden' });
	});

	it('needs an admin key to manage consumers', async () => {
		const name = uniqueName('consumers');
		const admin = authClient(ADMIN_KEY);
		await admin.queues.create(name);
		await expect(authClient(PUBLISH_KEY).consumers.create(name, 'billing')).rejects.toMatchObject({
			kind: 'forbidden',
			status: 403
		});
		expect((await admin.consumers.create(name, 'billing')).name).toBe('billing');
	});
});
