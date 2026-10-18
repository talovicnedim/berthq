// A fake broker behind a custom fetch, for tests that check what the client sends.
import { Client } from '../src/index.js';

/** The request fields the fake looks at. */
export interface FakeBody {
	messages?: { body: unknown }[];
	ack?: string[];
	leases?: string[];
}

export interface FakeRequest {
	path: string;
	body: FakeBody;
	/** The body as it was sent. */
	json: string;
}

export interface FakeOptions {
	/** Answer every request after this long. */
	delayMs?: number;
	/** What each consume call returns, in order; later calls return no messages. */
	consumes?: unknown[][];
}

/** Batches with a `bad` body are `invalid`, batches with a `full` body hit `queueFull`, and
 * batches with a `short` body get one result too few. */
export function fakeBroker(options: FakeOptions = {}) {
	const requests: FakeRequest[] = [];
	const consumes = [...(options.consumes ?? [])];
	let inFlight = 0;
	let maxInFlight = 0;

	const fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
		const path = new URL(String(input)).pathname;
		const json = init?.body ? String(init.body) : '';
		const body: FakeBody = json ? JSON.parse(json) : {};
		requests.push({ path, body, json });
		inFlight += 1;
		maxInFlight = Math.max(maxInFlight, inFlight);
		try {
			if (options.delayMs) await new Promise((done) => setTimeout(done, options.delayMs));
			return answer(path, body);
		} finally {
			inFlight -= 1;
		}
	};

	function answer(path: string, body: FakeBody): Response {
		if (path.endsWith('/batch')) {
			const bodies = (body.messages ?? []).map((message) => message.body);
			if (bodies.includes('bad')) return error(400, 'invalid', 'body bad is not allowed');
			if (bodies.includes('full')) return error(507, 'queueFull', 'queue is full');
			const messages = bodies.map((value) => ({ id: `id-${String(value)}`, duplicate: false }));
			if (bodies.includes('short')) messages.pop();
			return Response.json({ messages }, { status: 201 });
		}
		if (path.endsWith('/messages')) {
			return Response.json({ id: 'id-1', duplicate: false }, { status: 201 });
		}
		if (path.endsWith('/consume')) {
			const settled = body.ack?.length ?? 0;
			return Response.json({ messages: consumes.shift() ?? [], settled, unknown: [] });
		}
		if (path.endsWith('/ack') || path.endsWith('/nack') || path.endsWith('/extend')) {
			return Response.json({ settled: body.leases?.length ?? 0, unknown: [] });
		}
		return error(404, 'notFound', `no route ${path}`);
	}

	return {
		client: new Client('http://fake.test', { fetch }),
		requests,
		maxInFlight: () => maxInFlight,
		batches: () =>
			requests
				.filter((request) => request.path.endsWith('/batch'))
				.map((request) => (request.body.messages ?? []).map((message) => message.body))
	};
}

/** A delivery as the broker sends it. */
export function delivery(lease: string, visibilityTimeoutMs = 30_000) {
	return {
		id: `id-${lease}`,
		lease,
		queue: 'jobs',
		consumer: 'default',
		body: lease,
		priority: 0,
		attempt: 1,
		publishedAt: '2026-10-14T09:00:00.000Z',
		leaseExpiresAt: '2026-10-14T09:00:30.000Z',
		visibilityTimeoutMs,
		dedupId: null,
		lastError: null
	};
}

function error(status: number, kind: string, message: string): Response {
	return Response.json({ error: { kind, message } }, { status });
}
