import type { Client } from './client.js';
import { BerthqError } from './error.js';
import type { ConsumeOptions, Delivery, ReplayFrom } from './types.js';

export type Handler<T> = (message: Delivery<T>) => unknown;

export interface ProcessOptions {
	/** Handlers running at once (default 1). */
	concurrency?: number;
	/** Most messages leased per request (default: `concurrency`). Never more than free slots. */
	batch?: number;
	/** Stops the worker: no new messages are leased, in-flight ones finish and are settled. */
	signal?: AbortSignal;
	/** Whose position to use (default `default`). Workers of one consumer share its messages. */
	consumer?: string;
	/** Where the consumer starts if it doesn't exist yet (default `start`). */
	from?: ReplayFrom;
	/** Name shown in the dashboard. */
	worker?: string;
	/** Lease length (default: the queue's `visibilityTimeoutMs`). Leases are extended while a
	 * handler runs, so this only bounds how long a crashed worker holds a message. */
	visibilityTimeoutMs?: number;
	/** No leases: the position moves on delivery and nothing is acked, nacked or retried. Faster,
	 * but a crash or a throwing handler loses the message for this consumer. */
	autoAck?: boolean;
	/** Called when an ack, nack or extend fails (the message is then retried after its lease
	 * runs out), and with the handler's error in `autoAck` mode. Defaults to `console.error`. */
	onError?: (error: unknown, message: Delivery<unknown>) => void;
}

/** How long a consume request waits for a message before asking again. */
const WAIT_MS = 20_000;
/** Pause before asking again after the broker said it is unavailable or could not be reached. */
const BACKOFF_MS = 1_000;
/** The broker takes up to 1000 acks per request. */
const MAX_ACKS = 1000;

type OnError = (error: unknown, message: Delivery<unknown>) => void;

/**
 * Gets messages and runs `handler` on each until `options.signal` aborts. Finished messages are
 * acked with the next consume call; the rest of the acks are sent on their own when it stops.
 * Resolves once every in-flight message is settled. Rejects on errors retrying can't fix
 * (unknown queue, bad key).
 */
export async function process<T>(
	client: Client,
	queue: string,
	handler: Handler<T>,
	options: ProcessOptions = {}
): Promise<void> {
	const { signal, consumer, from, worker, visibilityTimeoutMs, autoAck } = options;
	const concurrency = Math.max(1, options.concurrency ?? 1);
	const batch = Math.max(1, options.batch ?? concurrency);
	const onError = options.onError ?? ((error) => console.error('berthq:', error));
	const acks = new PendingAcks();
	const running = new Set<Promise<void>>();
	// Resolves the loop's wait for a free slot; only the loop ever waits, so one is enough.
	let slotFreed: (() => void) | undefined;

	try {
		while (!signal?.aborted) {
			if (running.size >= concurrency) {
				await new Promise<void>((resolve) => (slotFreed = resolve));
				continue;
			}
			const max = Math.min(batch, concurrency - running.size);
			const request = {
				consumer,
				from,
				worker,
				max,
				waitMs: WAIT_MS,
				visibilityTimeoutMs,
				autoAck
			};
			for (const message of await nextBatch<T>(client, queue, request, acks, signal)) {
				const task = settle(client, message, handler, acks, onError).finally(() => {
					running.delete(task);
					slotFreed?.();
					slotFreed = undefined;
				});
				running.add(task);
			}
		}
	} finally {
		// Stopping (or failing) never abandons a message halfway: let running handlers settle.
		await Promise.all(running);
		await acks.flush(client, queue, onError);
	}
}

/**
 * The next messages, sending the waiting acks along. Empty when stopped, and after a failure
 * that may pass (the broker restarting or out of reach), once it has waited a moment.
 */
async function nextBatch<T>(
	client: Client,
	queue: string,
	request: ConsumeOptions,
	acks: PendingAcks,
	signal: AbortSignal | undefined
): Promise<Delivery<T>[]> {
	const ack = acks.next();
	try {
		const consumed = await client.consume<T>(
			queue,
			ack.length > 0 ? { ...request, ack } : request,
			signal
		);
		acks.sent(ack);
		return consumed.messages;
	} catch (err) {
		if (signal?.aborted) return [];
		if (!isTemporary(err)) throw err;
		await sleep(BACKOFF_MS, signal);
		return [];
	}
}

/**
 * Leases of finished messages, waiting to go out with the next consume. Their leases stay
 * extended until the broker has the ack, so a long-poll in between can't let them run out.
 */
class PendingAcks {
	readonly #waiting = new Map<string, { message: Delivery<unknown>; stopExtending: () => void }>();

	add(message: Delivery<unknown>, lease: string, stopExtending: () => void): void {
		this.#waiting.set(lease, { message, stopExtending });
	}

	/** Leases for the next request, at most what the broker takes at once. */
	next(): string[] {
		return [...this.#waiting.keys()].slice(0, MAX_ACKS);
	}

	/** The broker saved these acks. */
	sent(leases: string[]): void {
		for (const lease of leases) {
			this.#waiting.get(lease)?.stopExtending();
			this.#waiting.delete(lease);
		}
	}

	/** Sends what is left with `/ack`. A lease that can't be acked runs out and is redelivered. */
	async flush(client: Client, queue: string, onError: OnError): Promise<void> {
		while (this.#waiting.size > 0) {
			const batch = [...this.#waiting].slice(0, MAX_ACKS);
			const leases = batch.map(([lease]) => lease);
			try {
				await client.ack(queue, leases);
			} catch (err) {
				for (const [, { message }] of batch) onError(err, message);
			}
			this.sent(leases);
		}
	}
}

/** Runs the handler, keeps the lease alive meanwhile, then queues the ack or nacks. Never rejects. */
async function settle<T>(
	client: Client,
	message: Delivery<T>,
	handler: Handler<T>,
	acks: PendingAcks,
	onError: OnError
): Promise<void> {
	const { queue, lease } = message;
	if (lease === null) {
		// Auto-acked: the message is already done for this consumer, so a failure can only be reported.
		await Promise.resolve()
			.then(() => handler(message))
			.catch((err: unknown) => onError(err, message));
		return;
	}
	const stopExtending = keepLeaseAlive(client, message, lease, onError);
	try {
		await handler(message);
	} catch (err) {
		stopExtending();
		try {
			await client.nack(queue, { leases: [lease], error: errorMessage(err) });
		} catch (nackError) {
			onError(nackError, message);
		}
		return;
	}
	acks.add(message, lease, stopExtending);
}

/**
 * Extends the lease every half lease until stopped, so slow handlers (and finished messages
 * waiting for their ack) don't get redelivered to someone else. Returns a function that stops it.
 */
function keepLeaseAlive(
	client: Client,
	message: Delivery<unknown>,
	lease: string,
	onError: OnError
): () => void {
	// Each extend restarts the lease with the same length, so half of it leaves room for a slow
	// request. The length comes from the broker, so no clock comparison is needed.
	const { queue, visibilityTimeoutMs } = message;
	const timer = setInterval(async () => {
		try {
			const result = await client.extend(queue, [lease], visibilityTimeoutMs);
			// The lease is gone (it expired and the message went to someone else): stop trying.
			if (result.unknown.length > 0) clearInterval(timer);
		} catch (err) {
			onError(err, message);
		}
	}, visibilityTimeoutMs / 2);
	return () => clearInterval(timer);
}

/** Errors that go away by themselves: the broker restarting or shutting down. */
function isTemporary(err: unknown): boolean {
	return err instanceof BerthqError && (err.kind === 'unavailable' || err.kind === 'http');
}

function errorMessage(err: unknown): string {
	return err instanceof Error ? err.message : String(err);
}

/** Resolves after `ms`, or right away when `signal` aborts. */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		const done = () => {
			clearTimeout(timer);
			signal?.removeEventListener('abort', done);
			resolve();
		};
		const timer = setTimeout(done, ms);
		signal?.addEventListener('abort', done);
	});
}
