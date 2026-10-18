import { api, toApiError, type ApiError, type LiveStats } from '#lib/api/index.js';
import { keyProblem } from '#lib/utils/auth.js';
import { retryDelay } from '#lib/utils/backoff.js';

/** `down`: the stream closed and a reconnect is planned. `needsKey`: it waits for another key. */
export type LiveStatus = 'connecting' | 'live' | 'down' | 'needsKey';

/**
 * Live broker stats from `/v1/events` (server-sent events, one update per second). One
 * connection for the whole dashboard. The browser gives up for good when the stream answers
 * with an error status (a 403, or a 502 from a proxy while Berthq restarts), so this class
 * closes it on every error and reconnects itself.
 */
class Live {
	stats = $state<LiveStats>();
	status = $state<LiveStatus>('connecting');
	/** Why the stream is down. */
	error = $state<ApiError>();

	#key = '';
	#source?: EventSource;
	#timer?: ReturnType<typeof setTimeout>;
	#attempt = 0;

	/** Connects with the current API key; call again (it's an effect) when the key changes. */
	connect(key: string): () => void {
		this.#key = key;
		this.#open();
		return () => this.#close();
	}

	/** Reconnects now instead of waiting. */
	retry(): void {
		this.#attempt = 0;
		this.status = 'connecting';
		this.error = undefined;
		this.#open();
	}

	queue(name: string) {
		return this.stats?.queues.find((queue) => queue.name === name);
	}

	#open(): void {
		this.#close();
		const key = this.#key;
		const source = new EventSource(
			key ? `/v1/events?apiKey=${encodeURIComponent(key)}` : '/v1/events'
		);
		this.#source = source;
		source.addEventListener('stats', (event) => {
			this.stats = JSON.parse(event.data) as LiveStats;
			this.status = 'live';
			this.error = undefined;
			this.#attempt = 0;
		});
		source.onerror = () => {
			source.close();
			void this.#recover(source);
		};
	}

	/** EventSource hides the status code, so one request to `/v1/stats` decides what comes next. */
	async #recover(source: EventSource): Promise<void> {
		let error: ApiError = { kind: 'http', message: 'The live stream closed.' };
		try {
			await api.overview();
		} catch (err) {
			error = toApiError(err);
		}
		if (this.#source !== source) return;
		this.error = error;
		if (keyProblem('GET', error.status)) {
			this.status = 'needsKey';
			return;
		}
		this.status = 'down';
		this.#timer = setTimeout(() => this.#open(), retryDelay(this.#attempt++));
	}

	#close(): void {
		clearTimeout(this.#timer);
		this.#source?.close();
		this.#source = undefined;
	}
}

export const live = new Live();
