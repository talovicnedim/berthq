import { inject } from 'vitest';
import { Client, type ClientOptions } from '../src/index.js';

export function openClient(options?: ClientOptions): Client {
	return new Client(inject('url'), options);
}

export function authClient(apiKey?: string): Client {
	return new Client(inject('authUrl'), { apiKey });
}

/** Consumes and acks everything a consumer has right now; returns the bodies in order. */
export async function drain(client: Client, queue: string, consumer: string): Promise<unknown[]> {
	const messages = (await client.consume(queue, { consumer, max: 100 })).messages;
	const leases = messages.flatMap((message) => (message.lease ? [message.lease] : []));
	if (leases.length > 0) await client.ack(queue, leases);
	return messages.map((message) => message.body);
}

/** Test files share one broker and run at the same time, so every test uses its own queues. */
export function uniqueName(prefix: string): string {
	return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

/** Waits until `check` returns something truthy, for state the broker changes in the background. */
export async function eventually<T>(check: () => Promise<T>, timeoutMs = 5_000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const result = await check();
		if (result || Date.now() > deadline) return result;
		await new Promise((done) => setTimeout(done, 25));
	}
}
