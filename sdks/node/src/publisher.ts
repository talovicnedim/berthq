import type { Client } from './client.js';
import { BerthqError } from './error.js';
import type { NewMessage, Published } from './types.js';

export interface PublisherOptions {
	/** Most messages per batch, 1-1000 (default 1000). */
	maxBatch?: number;
	/**
	 * How long the first waiting message waits for others before its batch is sent. The default,
	 * 0, sends right away: messages that arrive while a batch is on its way form the next one.
	 */
	lingerMs?: number;
	/** Most bytes of JSON per batch (default 4 MiB). A bigger message is sent alone. */
	maxBytes?: number;
}

/** The broker takes up to 1000 messages per batch. */
const BATCH_LIMIT = 1000;

/** A message encoded once, and where its result goes. */
interface Entry {
	seq: number;
	json: string;
	bytes: number;
	queuedAt: number;
	resolve: (published: Published) => void;
	reject: (error: unknown) => void;
}

interface FlushWaiter {
	seq: number;
	resolve: () => void;
}

/**
 * Collects messages and publishes them in batches of up to `maxBatch` (and `maxBytes`). One batch
 * is in flight at a time, so messages keep their order; what arrives meanwhile forms the next
 * batch. Never slower than one request per message, and up to about 10 times faster when many
 * are sent at once. Make one with `client.publisher(queue)`.
 */
export class Publisher {
	readonly queue: string;
	readonly #client: Client;
	readonly #maxBatch: number;
	readonly #lingerMs: number;
	readonly #maxBytes: number;
	/** Waiting messages start at `#head`, so taking a batch doesn't shift the whole backlog. */
	#waiting: Entry[] = [];
	#head = 0;
	readonly #flushes: FlushWaiter[] = [];
	#waitingBytes = 0;
	#seq = 0;
	#sending = false;
	#closed = false;
	#timer: ReturnType<typeof setTimeout> | undefined;

	constructor(client: Client, queue: string, options: PublisherOptions = {}) {
		this.#client = client;
		this.queue = queue;
		this.#maxBatch = Math.min(Math.max(1, options.maxBatch ?? BATCH_LIMIT), BATCH_LIMIT);
		this.#lingerMs = Math.max(0, options.lingerMs ?? 0);
		this.#maxBytes = Math.max(1, options.maxBytes ?? 4 * 1024 * 1024);
	}

	/**
	 * Queues a message. Resolves with its result once its batch is on disk, or rejects with the
	 * error that batch (or, after an `invalid` batch, this message alone) got.
	 */
	publish(message: NewMessage): Promise<Published> {
		if (this.#closed) {
			return Promise.reject(
				new BerthqError('invalid', `the publisher for ${this.queue} is closed`, 0)
			);
		}
		return new Promise((resolve, reject) => {
			const json = JSON.stringify(message);
			const bytes = utf8Length(json);
			this.#seq += 1;
			this.#waiting.push({ seq: this.#seq, json, bytes, queuedAt: Date.now(), resolve, reject });
			this.#waitingBytes += bytes;
			this.#pump();
		});
	}

	/** Sends every message queued so far without waiting for the linger; resolves once they are
	 * all settled. Failures reject their own `publish` promises, not this one. */
	flush(): Promise<void> {
		if (this.#head === this.#waiting.length && !this.#sending) return Promise.resolve();
		const seq = this.#seq;
		const flushed = new Promise<void>((resolve) => this.#flushes.push({ seq, resolve }));
		this.#pump();
		return flushed;
	}

	/** Flushes, then rejects further `publish` calls. */
	async close(): Promise<void> {
		this.#closed = true;
		await this.flush();
	}

	/** Sends the next batch when it is due and none is in flight; otherwise waits for the linger. */
	#pump(): void {
		if (this.#sending || this.#head === this.#waiting.length) return;
		const wait = this.#lingerLeft();
		if (wait > 0) {
			this.#timer ??= setTimeout(() => {
				this.#timer = undefined;
				this.#pump();
			}, wait);
			return;
		}
		clearTimeout(this.#timer);
		this.#timer = undefined;
		this.#sending = true;
		const batch = this.#takeBatch();
		void this.#send(batch).finally(() => {
			this.#sending = false;
			this.#settled(batch[batch.length - 1].seq);
			this.#pump();
		});
	}

	/** 0 when the next batch should go now: it is full, a flush asked for it, or its first
	 * message has waited long enough. */
	#lingerLeft(): number {
		const first = this.#waiting[this.#head];
		const full =
			this.#waiting.length - this.#head >= this.#maxBatch || this.#waitingBytes >= this.#maxBytes;
		const flushing = first.seq <= (this.#flushes.at(-1)?.seq ?? 0);
		if (full || this.#closed || flushing) return 0;
		return Math.max(0, first.queuedAt + this.#lingerMs - Date.now());
	}

	#takeBatch(): Entry[] {
		let end = this.#head;
		let bytes = 0;
		while (end < this.#waiting.length && end - this.#head < this.#maxBatch) {
			const entry = this.#waiting[end];
			if (end > this.#head && bytes + entry.bytes > this.#maxBytes) break;
			bytes += entry.bytes;
			end += 1;
		}
		const batch = this.#waiting.slice(this.#head, end);
		this.#head = end;
		this.#waitingBytes -= bytes;
		// Drop the sent part now and then, once it is most of the array.
		if (this.#head > 1024 && this.#head * 2 > this.#waiting.length) {
			this.#waiting = this.#waiting.slice(this.#head);
			this.#head = 0;
		}
		return batch;
	}

	/** Publishes a batch. One bad message fails the whole batch, so a batch rejected as
	 * `invalid` is split in half until the bad messages are alone: only they fail, and the rest
	 * still go in order. */
	async #send(batch: Entry[]): Promise<void> {
		try {
			const results = await this.#client.publishEncoded(
				this.queue,
				batch.map((entry) => entry.json)
			);
			if (results.length !== batch.length) {
				const message = `the broker answered ${results.length} results for ${batch.length} messages`;
				const error = new BerthqError('http', message, 0);
				for (const entry of batch) entry.reject(error);
				return;
			}
			batch.forEach((entry, i) => entry.resolve(results[i]));
		} catch (err) {
			if (batch.length > 1 && err instanceof BerthqError && err.kind === 'invalid') {
				const half = Math.floor(batch.length / 2);
				await this.#send(batch.slice(0, half));
				await this.#send(batch.slice(half));
				return;
			}
			for (const entry of batch) entry.reject(err);
		}
	}

	#settled(seq: number): void {
		for (let i = this.#flushes.length - 1; i >= 0; i -= 1) {
			if (this.#flushes[i].seq > seq) continue;
			this.#flushes[i].resolve();
			this.#flushes.splice(i, 1);
		}
	}
}

/** Bytes of `text` as UTF-8, counted without encoding it. */
function utf8Length(text: string): number {
	let bytes = text.length;
	for (let i = 0; i < text.length; i += 1) {
		const code = text.charCodeAt(i);
		// 2 bytes below 0x800 and per surrogate half (a pair is 4), 3 bytes for the rest.
		if (code >= 0x80) bytes += code < 0x800 || (code >= 0xd800 && code <= 0xdfff) ? 1 : 2;
	}
	return bytes;
}
