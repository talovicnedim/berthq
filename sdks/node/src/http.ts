import { BerthqError, responseError } from './error.js';

/** Most requests answer right away; this only guards against a broker that hangs. */
export const DEFAULT_TIMEOUT_MS = 30_000;

export interface Transport {
	url: string;
	apiKey?: string;
	fetch: typeof fetch;
}

export interface SendOptions {
	body?: unknown;
	/** A body that is already JSON; sent as it is instead of `body`. */
	json?: string;
	timeoutMs?: number;
	/** Cancels the request; the promise then rejects with the signal's reason. */
	signal?: AbortSignal;
}

/** Sends one request and returns its JSON, or undefined for an empty (204) answer. */
export async function send<T>(
	transport: Transport,
	method: string,
	path: string,
	options: SendOptions = {}
): Promise<T> {
	const headers: Record<string, string> = {};
	if (transport.apiKey) headers.authorization = `Bearer ${transport.apiKey}`;
	const body =
		options.json ?? (options.body === undefined ? undefined : JSON.stringify(options.body));
	if (body !== undefined) headers['content-type'] = 'application/json';

	// One controller for both the timeout and the caller's signal, so we can tell them apart.
	// (AbortSignal.any would do this but needs Node 20.3.)
	const controller = new AbortController();
	let timedOut = false;
	const timer = setTimeout(() => {
		timedOut = true;
		controller.abort();
	}, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
	const onAbort = () => controller.abort(options.signal?.reason);
	options.signal?.addEventListener('abort', onAbort);
	if (options.signal?.aborted) onAbort();

	try {
		let response: Response;
		try {
			response = await transport.fetch(transport.url + path, {
				method,
				headers,
				body,
				signal: controller.signal
			});
		} catch (err) {
			throw transportError(transport, err, timedOut, options.signal);
		}
		if (!response.ok) throw await responseError(response);
		if (response.status === 204) return undefined as T;
		try {
			return (await response.json()) as T;
		} catch (err) {
			if (options.signal?.aborted) throw options.signal.reason;
			throw new BerthqError('http', 'invalid response from the broker', response.status, {
				cause: err
			});
		}
	} finally {
		clearTimeout(timer);
		options.signal?.removeEventListener('abort', onAbort);
	}
}

function transportError(
	transport: Transport,
	err: unknown,
	timedOut: boolean,
	signal?: AbortSignal
): unknown {
	// The caller cancelled: give them back their own reason, not a broker error.
	if (signal?.aborted && !timedOut) return signal.reason;
	if (timedOut) {
		return new BerthqError('http', `the broker at ${transport.url} did not answer in time`, 0);
	}
	// fetch hides why it failed (refused, DNS, TLS, ...) in `cause`.
	const cause = (err as { cause?: { message?: string } })?.cause?.message;
	const reason = cause ?? (err instanceof Error ? err.message : String(err));
	return new BerthqError(
		'http',
		`could not connect to the broker at ${transport.url} (${reason})`,
		0,
		{ cause: err }
	);
}

/** Percent-encodes a path segment so user input can't change the path. */
export function queuePath(queue: string, rest = ''): string {
	return `/v1/queues/${encodeURIComponent(queue)}${rest}`;
}

export function consumerPath(queue: string, consumer: string, rest = ''): string {
	return queuePath(queue, `/consumers/${encodeURIComponent(consumer)}${rest}`);
}
