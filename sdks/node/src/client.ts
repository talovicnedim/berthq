import { consumerPath, DEFAULT_TIMEOUT_MS, queuePath, send, type Transport } from './http.js';
import { Publisher, type PublisherOptions } from './publisher.js';
import type {
	ConsumeOptions,
	Consumed,
	ConsumerInfo,
	ConsumerMessage,
	ListConsumerMessagesOptions,
	ListMessagesOptions,
	MessageInfo,
	NackOptions,
	NewMessage,
	Overview,
	Published,
	QueueDetails,
	QueueInfo,
	QueueSettingsPatch,
	ReplayFrom,
	Requeued,
	Settled
} from './types.js';
import { process, type Handler, type ProcessOptions } from './worker.js';

export const DEFAULT_URL = 'http://127.0.0.1:7331';

export interface ClientOptions {
	/** Sent as `Authorization: Bearer <key>` with every request. */
	apiKey?: string;
	/** A fetch implementation to use instead of the global one (tests, proxies, ...). */
	fetch?: typeof fetch;
}

/**
 * A client for one broker. Names match the HTTP API and the other SDKs: `queues.create`,
 * `publish`, `consume`, `ack`, `nack`, `consumers.replay`, `requeue`, ...
 */
export class Client {
	readonly url: string;
	readonly queues;
	readonly consumers;
	readonly messages;
	readonly #transport: Transport;

	constructor(url: string = DEFAULT_URL, options: ClientOptions = {}) {
		this.url = url.replace(/\/+$/, '');
		this.#transport = {
			url: this.url,
			apiKey: options.apiKey,
			fetch: options.fetch ?? globalThis.fetch
		};
		this.queues = this.#queues();
		this.consumers = this.#consumers();
		this.messages = this.#messages();
	}

	/** Publishes one message. Resolves once it is on disk. */
	publish(queue: string, message: NewMessage): Promise<Published> {
		return this.#send('POST', queuePath(queue, '/messages'), message);
	}

	/**
	 * A publisher for `queue`: it collects messages and sends them in batches, which is much
	 * faster than one `publish` per message. See `PublisherOptions`.
	 */
	publisher(queue: string, options: PublisherOptions = {}): Publisher {
		return new Publisher(this, queue, options);
	}

	/** Publishes up to 1000 messages atomically; results are in the same order. */
	async publishBatch(queue: string, messages: NewMessage[]): Promise<Published[]> {
		const path = queuePath(queue, '/batch');
		return (await this.#send<{ messages: Published[] }>('POST', path, { messages })).messages;
	}

	/**
	 * Publishes messages that are already JSON as one atomic batch, for `Publisher`.
	 * @internal
	 */
	async publishEncoded(queue: string, messages: string[]): Promise<Published[]> {
		const json = `{"messages":[${messages.join(',')}]}`;
		const path = queuePath(queue, '/batch');
		return (await send<{ messages: Published[] }>(this.#transport, 'POST', path, { json }))
			.messages;
	}

	/**
	 * Moves dead letters back to the queue and consumer that failed them: the given ids, or all
	 * of them. Call it on a dead-letter queue (`emails.dlq`).
	 */
	requeue(queue: string, ids?: string[]): Promise<Requeued> {
		return this.#send('POST', queuePath(queue, '/requeue'), ids ? { ids } : {});
	}

	/**
	 * Gets up to `max` messages for a consumer (default `default`), after saving the `ack`
	 * leases. With `waitMs`, waits for a message to arrive (long-polling). `signal` cancels the
	 * wait.
	 */
	async consume<T = unknown>(
		queue: string,
		options: ConsumeOptions = {},
		signal?: AbortSignal
	): Promise<Consumed<T>> {
		const response = await send<Consumed<T>>(
			this.#transport,
			'POST',
			queuePath(queue, '/consume'),
			// The request may legitimately take as long as the wait, plus time to answer.
			{ body: options, timeoutMs: (options.waitMs ?? 0) + DEFAULT_TIMEOUT_MS, signal }
		);
		return {
			messages: response.messages,
			settled: response.settled ?? 0,
			unknown: response.unknown ?? []
		};
	}

	/** Done for the lease's consumer. Unknown leases are reported, not an error. */
	ack(queue: string, leases: string[]): Promise<Settled> {
		return this.#send('POST', queuePath(queue, '/ack'), { leases });
	}

	/** Failed: retries after a backoff, or dead-letters when out of attempts. */
	nack(queue: string, options: NackOptions): Promise<Settled> {
		return this.#send('POST', queuePath(queue, '/nack'), options);
	}

	/** Restarts the lease clock (default: the queue's visibility timeout). */
	extend(queue: string, leases: string[], visibilityTimeoutMs?: number): Promise<Settled> {
		return this.#send('POST', queuePath(queue, '/extend'), { leases, visibilityTimeoutMs });
	}

	/** Berthq-wide totals, rates, free disk space and the last minute of throughput. */
	stats(): Promise<Overview> {
		return this.#send('GET', '/v1/stats');
	}

	/**
	 * Runs `handler` for every message of `queue` until `options.signal` aborts: acks when it
	 * returns, nacks with the error when it throws. See `ProcessOptions`.
	 */
	process<T = unknown>(
		queue: string,
		handler: Handler<T>,
		options: ProcessOptions = {}
	): Promise<void> {
		return process(this, queue, handler, options);
	}

	#send<T>(method: string, path: string, body?: unknown): Promise<T> {
		return send<T>(this.#transport, method, path, { body });
	}

	#queues() {
		return {
			list: async (): Promise<QueueInfo[]> =>
				(await this.#send<{ queues: QueueInfo[] }>('GET', '/v1/queues')).queues,
			/** Settings, stats, consumers and the last minute of throughput. */
			get: (queue: string): Promise<QueueDetails> => this.#send('GET', queuePath(queue)),
			create: (queue: string, settings: QueueSettingsPatch = {}): Promise<QueueInfo> =>
				this.#send('POST', '/v1/queues', { ...settings, name: queue }),
			update: (queue: string, settings: QueueSettingsPatch): Promise<QueueInfo> =>
				this.#send('PATCH', queuePath(queue), settings),
			/** Deletes the queue, its consumers and its messages. Its dead-letter queue stays. */
			remove: (queue: string): Promise<void> => this.#send('DELETE', queuePath(queue)),
			/** Deletes every message, including ones being processed; returns how many. */
			purge: async (queue: string): Promise<number> =>
				(await this.#send<{ purged: number }>('POST', queuePath(queue, '/purge'))).purged,
			/** A paused queue accepts publishes but delivers nothing to any consumer. */
			pause: (queue: string): Promise<QueueInfo> => this.#send('POST', queuePath(queue, '/pause')),
			resume: (queue: string): Promise<QueueInfo> => this.#send('POST', queuePath(queue, '/resume'))
		};
	}

	#consumers() {
		return {
			list: async (queue: string): Promise<ConsumerInfo[]> =>
				(await this.#send<{ consumers: ConsumerInfo[] }>('GET', queuePath(queue, '/consumers')))
					.consumers,
			get: (queue: string, consumer: string): Promise<ConsumerInfo> =>
				this.#send('GET', consumerPath(queue, consumer)),
			/** Creates a consumer starting at `from` (default `start`). Rejects with `conflict` if
			 * it exists. `consume` also creates consumers on first use. */
			create: (queue: string, consumer: string, from?: ReplayFrom): Promise<ConsumerInfo> =>
				this.#send('POST', queuePath(queue, '/consumers'), { name: consumer, from }),
			/** Removes the consumer with its position, leases and retries. */
			remove: (queue: string, consumer: string): Promise<void> =>
				this.#send('DELETE', consumerPath(queue, consumer)),
			/** Moves the position. Drops what the consumer was processing and retrying. */
			replay: (queue: string, consumer: string, from: ReplayFrom): Promise<ConsumerInfo> =>
				this.#send('POST', consumerPath(queue, consumer, '/replay'), { from }),
			/** A paused consumer gets nothing; other consumers continue. */
			pause: (queue: string, consumer: string): Promise<ConsumerInfo> =>
				this.#send('POST', consumerPath(queue, consumer, '/pause')),
			resume: (queue: string, consumer: string): Promise<ConsumerInfo> =>
				this.#send('POST', consumerPath(queue, consumer, '/resume')),
			/** What the consumer is processing (default), retrying, or hasn't received yet. No bodies. */
			messages: async (
				queue: string,
				consumer: string,
				options: ListConsumerMessagesOptions = {}
			): Promise<ConsumerMessage[]> => {
				const query = new URLSearchParams();
				if (options.state) query.set('state', options.state);
				if (options.limit !== undefined) query.set('limit', String(options.limit));
				const path = consumerPath(queue, consumer, `/messages?${query}`);
				return (await this.#send<{ messages: ConsumerMessage[] }>('GET', path)).messages;
			}
		};
	}

	#messages() {
		const messagePath = (queue: string, id: string) =>
			queuePath(queue, `/messages/${encodeURIComponent(id)}`);
		return {
			/** Browses stored messages, newest first, without consuming them or their bodies. */
			list: async (queue: string, options: ListMessagesOptions = {}): Promise<MessageInfo[]> => {
				const query = new URLSearchParams();
				if (options.state) query.set('state', options.state);
				if (options.limit !== undefined) query.set('limit', String(options.limit));
				if (options.before) query.set('before', options.before);
				const path = queuePath(queue, `/messages?${query}`);
				return (await this.#send<{ messages: MessageInfo[] }>('GET', path)).messages;
			},
			/** One message including its body. */
			get: (queue: string, id: string): Promise<MessageInfo> =>
				this.#send('GET', messagePath(queue, id)),
			/** Deletes it for every consumer. */
			remove: (queue: string, id: string): Promise<void> =>
				this.#send('DELETE', messagePath(queue, id))
		};
	}
}
