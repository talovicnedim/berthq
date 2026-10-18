// Request and response bodies of the HTTP API (see docs/api.md). Field names match the JSON.
// Durations are milliseconds (`...Ms`), timestamps are RFC 3339 strings.

export interface NewMessage {
	/** Any JSON value. Plain text is a JSON string. Up to 4 MB. */
	body: unknown;
	/** 0-9, higher is delivered first (default 0). */
	priority?: number;
	/** Hide the message for this long. */
	delayMs?: number;
	/** RFC 3339 time to deliver at; overrides `delayMs`. */
	deliverAt?: string;
	/** Ignore repeats of this id for an hour. */
	dedupId?: string;
}

export interface Published {
	id: string;
	/** True when the `dedupId` was already seen and nothing was stored. */
	duplicate: boolean;
}

export interface QueueSettings {
	/** How long messages stay after they become visible, for replay. */
	retentionMs: number;
	visibilityTimeoutMs: number;
	/** Deliveries before a message moves to the dead-letter queue; 0 retries forever. */
	maxAttempts: number;
	/** First retry delay, doubled per attempt up to 5 minutes. */
	retryDelayMs: number;
	/** Backlog limit: messages a consumer still has to process, plus delayed ones. */
	maxMessages: number | null;
	paused: boolean;
}

/** Settings to set; omitted fields keep their value. `maxMessages: 0` removes the limit. */
export interface QueueSettingsPatch {
	retentionMs?: number;
	visibilityTimeoutMs?: number;
	maxAttempts?: number;
	retryDelayMs?: number;
	maxMessages?: number;
}

export interface Counters {
	published: number;
	delivered: number;
	acked: number;
	retried: number;
	deadLettered: number;
	/** Ran out of attempts in a dead-letter queue, which has no dead-letter queue itself. */
	dropped: number;
}

export interface QueueStats {
	/** Everything stored, including messages kept only for replay and delayed ones. */
	messages: number;
	/** Messages at least one consumer still has to process. */
	backlog: number;
	/** Messages waiting for their `deliverAt`. */
	delayed: number;
	/** Messages leased right now, across all consumers. */
	processing: number;
	bytes: number;
	consumers: number;
	/** Workers seen in the last minute, across all consumers. */
	workers: number;
	/** Counted since the broker started. */
	counters: Counters;
	/** Messages per second over the last second. */
	publishRate: number;
	deliverRate: number;
	ackRate: number;
}

export interface QueueInfo {
	name: string;
	createdAt: string;
	settings: QueueSettings;
	stats: QueueStats;
	/** `<queue>.dlq` once it exists; it is created when the first message is dead-lettered. */
	deadLetterQueue: string | null;
	/** Set on a dead-letter queue: the queue its messages failed in. */
	deadLetterOf: string | null;
}

export interface StatsPoint {
	at: string;
	published: number;
	delivered: number;
	acked: number;
	failed: number;
}

export interface QueueDetails extends QueueInfo {
	consumers: ConsumerInfo[];
	/** Throughput per second for the last minute. */
	history: StatsPoint[];
}

export interface Overview {
	version: string;
	startedAt: string;
	queues: number;
	messages: number;
	backlog: number;
	processing: number;
	/** Messages in dead-letter queues. */
	failed: number;
	consumers: number;
	workers: number;
	publishRate: number;
	deliverRate: number;
	ackRate: number;
	counters: Counters;
	history: StatsPoint[];
	disk: DiskStats;
}

/** Free space on the broker's data disk, in bytes. */
export interface DiskStats {
	free: number;
	/** The broker's `--min-free-disk`; 0 when the check is off. */
	minFree: number;
	/** True while `free` is below `minFree`: publishes fail with `diskFull` until space is freed. */
	full: boolean;
}

/** Where a consumer starts or moves to: `start` (the oldest kept message), `end` (only messages
 * published from now on), an RFC 3339 time, or a message id (that message and everything after). */
export type ReplayFrom = 'start' | 'end' | (string & {});

export interface ConsumerStats {
	/** Messages after its position it hasn't received yet. */
	waiting: number;
	/** Its messages leased right now. */
	processing: number;
	/** Messages it received before that wait for their next attempt, backoff over or not. */
	retrying: number;
}

/** A process consuming for a consumer, seen in the last minute. */
export interface WorkerInfo {
	name: string;
	lastSeen: string;
	/** True while it is long-polling. */
	waiting: boolean;
}

/** A name with a saved position in a queue. Every consumer gets every message. */
export interface ConsumerInfo {
	queue: string;
	name: string;
	createdAt: string;
	paused: boolean;
	stats: ConsumerStats;
	/** No worker for an hour or more while it holds messages: remove it if it is not coming back. */
	idle: boolean;
	/** The last time a worker consumed for it; null if never. */
	lastSeen: string | null;
	workers: WorkerInfo[];
}

export interface ConsumeOptions {
	/** Whose position to use (default `default`); created if it doesn't exist. */
	consumer?: string;
	/** Where a new consumer starts (default `start`). Ignored if the consumer exists. */
	from?: ReplayFrom;
	/** Most messages to return, 1-100 (default 1). */
	max?: number;
	/** Long-poll: wait up to this long for a message (default 0, at most 60000). */
	waitMs?: number;
	/** Lease length for these messages (default: the queue's setting). */
	visibilityTimeoutMs?: number;
	/** No lease: the position moves right away, with no retries (default false). */
	autoAck?: boolean;
	/** Name shown in the dashboard (default `anonymous`). */
	worker?: string;
	/** Leases to ack before getting new messages (up to 1000), saving a separate `ack` call. */
	ack?: string[];
}

/** What `consume` returns: the messages, and how the `ack` leases were settled. */
export interface Consumed<T = unknown> extends Settled {
	messages: Delivery<T>[];
}

/** A delivered message. `T` is the type you expect the body to have; it is not checked. */
export interface Delivery<T = unknown> {
	/** The same on every redelivery and replay, so it works as an idempotency key. */
	id: string;
	/** Pass to `ack`, `nack` or `extend`. Null with `autoAck`. */
	lease: string | null;
	queue: string;
	consumer: string;
	body: T;
	priority: number;
	/** 1 on the first delivery. */
	attempt: number;
	publishedAt: string;
	/** Null with `autoAck`. */
	leaseExpiresAt: string | null;
	/** Length of this lease, to time `extend` calls without comparing clocks. 0 with `autoAck`. */
	visibilityTimeoutMs: number;
	dedupId: string | null;
	/** The error of the previous failed attempt. */
	lastError: string | null;
}

export interface NackOptions {
	leases: string[];
	/** Why processing failed; kept on the message and shown in the dashboard. */
	error?: string;
	/** Retry after this long instead of the queue's backoff. */
	delayMs?: number;
	/** Dead-letter right away. */
	deadLetter?: boolean;
}

export interface Settled {
	settled: number;
	/** Leases that no longer exist: settled, expired and redelivered, or dropped by a replay. */
	unknown: string[];
}

export type MessageState = 'visible' | 'delayed';

export interface ListMessagesOptions {
	/** Default `visible`. */
	state?: MessageState;
	/** Up to 500 (default 50). */
	limit?: number;
	/** A message id: list messages older than it (the list is newest first). */
	before?: string;
}

export type DeadLetterReason = 'maxAttempts' | 'rejected';

export interface MessageDeadLetter {
	/** The queue the message failed in. */
	queue: string;
	/** The consumer that failed it; `requeue` moves it back to this consumer. */
	consumer: string;
	/** The original message's id; a requeued message is delivered with it again. */
	messageId: string;
	reason: DeadLetterReason;
	at: string;
}

export interface MessageInfo {
	id: string;
	queue: string;
	state: MessageState;
	priority: number;
	publishedAt: string;
	deliverAt: string | null;
	dedupId: string | null;
	size: number;
	/** In a dead-letter queue: the failed consumer's attempts. 0 otherwise. */
	attempts: number;
	lastError: string | null;
	deadLetter: MessageDeadLetter | null;
	/** Only set by `messages.get`. */
	body: unknown;
}

export type ConsumerMessageState = 'waiting' | 'processing' | 'retrying';

export interface ListConsumerMessagesOptions {
	/** Default `processing`. */
	state?: ConsumerMessageState;
	/** Up to 500 (default 50). */
	limit?: number;
}

/** A message as one consumer sees it: not received yet, being processed, or waiting to retry. */
export interface ConsumerMessage {
	id: string;
	queue: string;
	consumer: string;
	state: ConsumerMessageState;
	priority: number;
	attempts: number;
	publishedAt: string;
	lastError: string | null;
	/** Set while processing. */
	worker: string | null;
	/** Set while processing. */
	leaseExpiresAt: string | null;
	/** Set while retrying. */
	retryAt: string | null;
	size: number;
}

export interface Requeued {
	requeued: number;
	/** The queue or consumer no longer exists, or the consumer already has the message pending. */
	skipped: number;
}
