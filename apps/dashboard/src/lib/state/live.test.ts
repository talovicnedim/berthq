import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

class FakeEventSource {
	static all: FakeEventSource[] = [];
	url: string;
	closed = false;
	onerror: (() => void) | null = null;
	#listeners = new Map<string, (event: { data: string }) => void>();

	constructor(url: string) {
		this.url = url;
		FakeEventSource.all.push(this);
	}

	addEventListener(type: string, listener: (event: { data: string }) => void): void {
		this.#listeners.set(type, listener);
	}

	close(): void {
		this.closed = true;
	}

	send(data: unknown): void {
		this.#listeners.get('stats')?.({ data: JSON.stringify(data) });
	}

	fail(): void {
		this.onerror?.();
	}
}

const stats = { overview: {}, queues: [] };

function answer(status: number): void {
	const body = status === 200 ? {} : { error: { kind: 'x', message: `answered ${status}` } };
	vi.stubGlobal('fetch', async () => new Response(JSON.stringify(body), { status }));
}

function open(): FakeEventSource[] {
	return FakeEventSource.all.filter((source) => !source.closed);
}

async function load() {
	const { live } = await import('./live.svelte.js');
	const { auth } = await import('./auth.svelte.js');
	return { live, auth };
}

beforeEach(() => {
	vi.useFakeTimers();
	vi.resetModules();
	FakeEventSource.all = [];
	const storage = new Map<string, string>();
	vi.stubGlobal('localStorage', {
		getItem: (key: string) => storage.get(key) ?? null,
		setItem: (key: string, value: string) => storage.set(key, value),
		removeItem: (key: string) => storage.delete(key)
	});
	vi.stubGlobal('EventSource', FakeEventSource);
});

afterEach(() => {
	vi.useRealTimers();
	vi.unstubAllGlobals();
});

describe('live', () => {
	it('reconnects after an error answer, waiting longer each time', async () => {
		const { live } = await load();
		answer(502);
		const stop = live.connect('key');

		open()[0]?.fail();
		await vi.advanceTimersByTimeAsync(0);
		expect(live.status).toBe('down');
		expect(live.error?.message).toBe('answered 502');
		expect(open()).toHaveLength(0);

		await vi.advanceTimersByTimeAsync(1000);
		expect(open()).toHaveLength(1);
		open()[0]?.fail();
		await vi.advanceTimersByTimeAsync(1999);
		expect(open()).toHaveLength(0);
		await vi.advanceTimersByTimeAsync(1);
		expect(open()).toHaveLength(1);

		answer(200);
		open()[0]?.send(stats);
		expect(live.status).toBe('live');
		expect(live.error).toBeUndefined();
		open()[0]?.fail();
		await vi.advanceTimersByTimeAsync(1000);
		expect(open()).toHaveLength(1);
		stop();
		expect(open()).toHaveLength(0);
	});

	it('asks for a read key and stops reconnecting when reads are forbidden', async () => {
		const { live, auth } = await load();
		answer(403);
		live.connect('publish-key');

		open()[0]?.fail();
		await vi.advanceTimersByTimeAsync(60_000);
		expect(live.status).toBe('needsKey');
		expect(auth.prompt).toBe('scope');
		expect(FakeEventSource.all).toHaveLength(1);
	});

	it('asks for a key on 401', async () => {
		const { live, auth } = await load();
		answer(401);
		live.connect('');

		open()[0]?.fail();
		await vi.advanceTimersByTimeAsync(60_000);
		expect(live.status).toBe('needsKey');
		expect(auth.prompt).toBe('missing');
		expect(FakeEventSource.all).toHaveLength(1);
	});

	it('keeps one connection when Retry is pressed during a wait', async () => {
		const { live } = await load();
		answer(503);
		live.connect('key');

		open()[0]?.fail();
		await vi.advanceTimersByTimeAsync(0);
		live.retry();
		expect(live.status).toBe('connecting');
		await vi.advanceTimersByTimeAsync(60_000);
		expect(FakeEventSource.all).toHaveLength(2);
	});

	it('drops a pending reconnect when stopped', async () => {
		const { live } = await load();
		answer(502);
		const stop = live.connect('key');

		open()[0]?.fail();
		stop();
		await vi.advanceTimersByTimeAsync(60_000);
		expect(FakeEventSource.all).toHaveLength(1);
	});
});
