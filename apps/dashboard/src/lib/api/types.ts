// Mirrors the broker's HTTP API (docs/api.md). Hand-written: see the "hand-written TypeScript
// types" decision in docs/spec.md.

export type DeadLetterReason = 'maxAttempts' | 'rejected';
/** A stored message: `visible` to consumers, or `delayed` until its `deliverAt`. */
export type MessageState = 'visible' | 'delayed';
/** What a consumer is doing with a message. */
export type ConsumerMessageState = 'waiting' | 'processing' | 'retrying';

export interface QueueSettings {
	/** How long messages stay after they become visible, for replay. */
	retentionMs: number;
	visibilityTimeoutMs: number;
	maxAttempts: number;
	retryDelayMs: number;
	/** Backlog limit: messages a consumer still has to process, plus delayed ones. */
	maxMessages: number | null;
	paused: boolean;
}

/** Settings to change; `maxMessages` of 0 removes the limit. */
export type QueueSettingsPatch = Partial<Omit<QueueSettings, 'paused' | 'maxMessages'>> & {
	maxMessages?: number;
};

export interface Counters {
	published: number;
	delivered: number;
	acked: number;
	retried: number;
	deadLettered: number;
	/** Ran out of attempts inside a dead-letter queue, which has no dead-letter queue of its own. */
	dropped: number;
}

export interface QueueStats {
	/** Everything stored, including messages kept only for replay and delayed ones. */
	messages: number;
	/** Messages at least one consumer still has to process. */
	backlog: number;
	delayed: number;
	/** Leased right now, across all consumers. */
	processing: number;
	bytes: number;
	consumers: number;
	/** Workers seen in the last minute, across all consumers. */
	workers: number;
	counters: Counters;
	publishRate: number;
	deliverRate: number;
	ackRate: number;
}

export interface QueueInfo {
	name: string;
	createdAt: string;
	settings: QueueSettings;
	stats: QueueStats;
	deadLetterQueue: string | null;
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
	history: StatsPoint[];
}

export interface WorkerInfo {
	name: string;
	lastSeen: string;
	/** True while the worker is long-polling. */
	waiting: boolean;
}

export interface ConsumerStats {
	/** Messages after its position it hasn't received yet (retries not included). */
	waiting: number;
	processing: number;
	/** Messages it received before that wait for their next attempt, backoff over or not. */
	retrying: number;
}

/** A name with a saved position in a queue. */
export interface ConsumerInfo {
	queue: string;
	name: string;
	createdAt: string;
	paused: boolean;
	stats: ConsumerStats;
	/** No worker for a while although it still holds messages; the broker decides when. */
	idle: boolean;
	/** The last time a worker consumed for it; `null` if never. */
	lastSeen: string | null;
	/** Workers seen in the last minute. */
	workers: WorkerInfo[];
}

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
	leaseExpiresAt: string | null;
	/** Set while retrying. */
	retryAt: string | null;
	size: number;
}

/** Free space on the disk that holds the data. */
export interface DiskStats {
	free: number;
	/** What Berthq keeps free (`--min-free-disk`); 0 turns the check off. */
	minFree: number;
	/** True while publishes are refused because `free` is below `minFree`. */
	full: boolean;
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

export interface MessageDeadLetter {
	/** The queue and consumer that failed it; `requeue` sends it back there. */
	queue: string;
	consumer: string;
	/** The original message's id; the consumer gets it back under this id after a requeue. */
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
	/** For dead letters: attempts and last error of the consumer that failed it. */
	attempts: number;
	lastError: string | null;
	deadLetter: MessageDeadLetter | null;
	/** Only in `GET …/messages/{id}`; `null` in lists. */
	body: unknown;
}

export interface NewMessage {
	body: unknown;
	priority?: number;
	delayMs?: number;
	deliverAt?: string;
	dedupId?: string;
}

export interface Published {
	id: string;
	duplicate: boolean;
}

export interface Requeued {
	requeued: number;
	skipped: number;
}

/** What `/v1/events` sends every second. */
export interface LiveStats {
	overview: Overview;
	queues: QueueInfo[];
}
