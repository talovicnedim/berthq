import { auth } from '#lib/state/auth.svelte.js';
import { keyProblem } from '#lib/utils/auth.js';
import type {
	ConsumerInfo,
	ConsumerMessage,
	ConsumerMessageState,
	MessageInfo,
	MessageState,
	NewMessage,
	Overview,
	Published,
	QueueDetails,
	QueueInfo,
	QueueSettingsPatch,
	Requeued
} from './types.js';

export type * from './types.js';

/** An error from the broker (`{ error: { kind, message } }`) or from reaching it. */
export interface ApiError {
	kind: string;
	message: string;
	/** The HTTP status, when the broker (or a proxy in front of it) answered. */
	status?: number;
}

export function toApiError(err: unknown): ApiError {
	if (typeof err === 'object' && err !== null && 'kind' in err && 'message' in err) {
		const status = 'status' in err && typeof err.status === 'number' ? err.status : undefined;
		return { kind: String(err.kind), message: String(err.message), status };
	}
	return { kind: 'http', message: err instanceof Error ? err.message : String(err) };
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
	const headers: Record<string, string> = {};
	if (body !== undefined) headers['content-type'] = 'application/json';
	if (auth.key) headers.authorization = `Bearer ${auth.key}`;
	let response: Response;
	try {
		response = await fetch(path, {
			method,
			headers,
			body: body === undefined ? undefined : JSON.stringify(body)
		});
	} catch {
		throw { kind: 'http', message: 'Could not reach the broker.' } satisfies ApiError;
	}
	const problem = keyProblem(method, response.status);
	if (problem) auth.ask(problem);
	if (response.status === 204) return undefined as T;
	const data: unknown = await response.json().catch(() => undefined);
	if (!response.ok) {
		const error = (data as { error?: ApiError } | undefined)?.error ?? {
			kind: 'http',
			message: `The broker answered ${response.status}.`
		};
		throw { ...error, status: response.status } satisfies ApiError;
	}
	return data as T;
}

function queuePath(queue: string, rest = ''): string {
	return `/v1/queues/${encodeURIComponent(queue)}${rest}`;
}

function consumerPath(queue: string, consumer: string, rest = ''): string {
	return queuePath(queue, `/consumers/${encodeURIComponent(consumer)}${rest}`);
}

function messagePath(queue: string, id: string): string {
	return queuePath(queue, `/messages/${encodeURIComponent(id)}`);
}

export interface MessageListOptions {
	state?: MessageState;
	/** A message id: list messages older than it. */
	before?: string;
	limit?: number;
}

/** Every call the dashboard makes. Names match the HTTP API and the broker's core. */
export const api = {
	overview: () => request<Overview>('GET', '/v1/stats'),
	queues: {
		list: async () => (await request<{ queues: QueueInfo[] }>('GET', '/v1/queues')).queues,
		get: (name: string) => request<QueueDetails>('GET', queuePath(name)),
		create: (name: string, settings: QueueSettingsPatch = {}) =>
			request<QueueInfo>('POST', '/v1/queues', { name, ...settings }),
		update: (name: string, settings: QueueSettingsPatch) =>
			request<QueueInfo>('PATCH', queuePath(name), settings),
		remove: (name: string) => request<void>('DELETE', queuePath(name)),
		purge: async (name: string) =>
			(await request<{ purged: number }>('POST', queuePath(name, '/purge'))).purged,
		pause: (name: string) => request<QueueInfo>('POST', queuePath(name, '/pause')),
		resume: (name: string) => request<QueueInfo>('POST', queuePath(name, '/resume'))
	},
	consumers: {
		list: async (queue: string) =>
			(await request<{ consumers: ConsumerInfo[] }>('GET', queuePath(queue, '/consumers')))
				.consumers,
		get: (queue: string, name: string) => request<ConsumerInfo>('GET', consumerPath(queue, name)),
		/** `from`: `start` (the default), `end`, an RFC 3339 time or a message id. */
		create: (queue: string, name: string, from?: string) =>
			request<ConsumerInfo>('POST', queuePath(queue, '/consumers'), { name, from }),
		remove: (queue: string, name: string) => request<void>('DELETE', consumerPath(queue, name)),
		replay: (queue: string, name: string, from: string) =>
			request<ConsumerInfo>('POST', consumerPath(queue, name, '/replay'), { from }),
		pause: (queue: string, name: string) =>
			request<ConsumerInfo>('POST', consumerPath(queue, name, '/pause')),
		resume: (queue: string, name: string) =>
			request<ConsumerInfo>('POST', consumerPath(queue, name, '/resume')),
		messages: async (queue: string, name: string, state: ConsumerMessageState, limit = 100) => {
			const query = new URLSearchParams({ state, limit: String(limit) });
			const path = consumerPath(queue, name, `/messages?${query}`);
			return (await request<{ messages: ConsumerMessage[] }>('GET', path)).messages;
		}
	},
	messages: {
		/** Newest first. */
		list: async (queue: string, options: MessageListOptions = {}) => {
			const query = new URLSearchParams({ limit: String(options.limit ?? 50) });
			if (options.state) query.set('state', options.state);
			if (options.before) query.set('before', options.before);
			const path = queuePath(queue, `/messages?${query}`);
			return (await request<{ messages: MessageInfo[] }>('GET', path)).messages;
		},
		get: (queue: string, id: string) => request<MessageInfo>('GET', messagePath(queue, id)),
		remove: (queue: string, id: string) => request<void>('DELETE', messagePath(queue, id)),
		publish: (queue: string, message: NewMessage) =>
			request<Published>('POST', queuePath(queue, '/messages'), message),
		requeue: (queue: string, ids?: string[]) =>
			request<Requeued>('POST', queuePath(queue, '/requeue'), ids ? { ids } : {})
	}
};
