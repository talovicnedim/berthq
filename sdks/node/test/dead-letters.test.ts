import { describe, expect, it } from 'vitest';
import { drain, openClient, uniqueName } from './helpers.js';

const client = openClient();

describe('retries and dead letters', () => {
	it('retries a nacked message, dead-letters it, then requeues it', async () => {
		const name = uniqueName('retry');
		await client.queues.create(name, { maxAttempts: 2 });
		const { id } = await client.publish(name, { body: 'flaky' });

		const [first] = (await client.consume(name)).messages;
		await client.nack(name, { leases: [first.lease!], error: 'smtp timeout', delayMs: 0 });

		const [second] = (await client.consume(name, { waitMs: 2_000 })).messages;
		expect(second).toMatchObject({ id, attempt: 2, lastError: 'smtp timeout' });
		await client.nack(name, { leases: [second.lease!], error: 'smtp down' });

		const dlq = `${name}.dlq`;
		expect((await client.queues.get(name)).deadLetterQueue).toBe(dlq);
		expect((await client.queues.get(dlq)).deadLetterOf).toBe(name);
		// A dead letter is a copy, so it is looked up in the DLQ rather than by the original id.
		const [listed] = await client.messages.list(dlq);
		const dead = await client.messages.get(dlq, listed.id);
		expect(dead).toMatchObject({
			body: 'flaky',
			attempts: 2,
			lastError: 'smtp down',
			deadLetter: { queue: name, consumer: 'default', messageId: id, reason: 'maxAttempts' }
		});

		expect(await client.requeue(dlq)).toEqual({ requeued: 1, skipped: 0 });
		const [again] = (await client.consume(name)).messages;
		expect(again).toMatchObject({ id, body: 'flaky', attempt: 1 });
	});

	it('dead-letters right away with deadLetter: true and requeues by id', async () => {
		const name = uniqueName('reject');
		await client.queues.create(name);
		await client.publish(name, { body: 'poison' });
		const [delivery] = (await client.consume(name)).messages;
		await client.nack(name, { leases: [delivery.lease!], deadLetter: true });

		const [dead] = await client.messages.list(`${name}.dlq`);
		expect(dead.deadLetter?.reason).toBe('rejected');
		expect(await client.requeue(`${name}.dlq`, [dead.id])).toEqual({ requeued: 1, skipped: 0 });
	});

	it('requeues a dead letter only to the consumer that failed it', async () => {
		const name = uniqueName('requeue-consumer');
		await client.queues.create(name);
		await client.publish(name, { body: 'job' });
		expect(await drain(client, name, 'audit')).toEqual(['job']);
		const [delivery] = (await client.consume(name, { consumer: 'billing' })).messages;
		await client.nack(name, { leases: [delivery.lease!], deadLetter: true });

		await client.requeue(`${name}.dlq`);
		expect(await drain(client, name, 'billing')).toEqual(['job']);
		expect(await drain(client, name, 'audit')).toEqual([]);
	});
});
