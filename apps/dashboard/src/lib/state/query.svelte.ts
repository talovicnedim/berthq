import { toApiError, type ApiError } from '#lib/api/index.js';

/**
 * Loading / data / error state for one backend call. Pages create one per resource and
 * render it with `<QueryView>`.
 */
export class Query<T> {
	data = $state<T>();
	error = $state<ApiError>();
	loading = $state(false);

	#fetch: () => Promise<T>;

	constructor(fetch: () => Promise<T>) {
		this.#fetch = fetch;
	}

	async refresh(): Promise<void> {
		this.loading = true;
		try {
			this.data = await this.#fetch();
			this.error = undefined;
		} catch (err) {
			this.error = toApiError(err);
		} finally {
			this.loading = false;
		}
	}

	/** Refreshes now and every `ms`. Returns a cleanup, so use it as `$effect(() => q.poll(5000))`. */
	poll(ms: number): () => void {
		void this.refresh();
		const timer = setInterval(() => void this.refresh(), ms);
		return () => clearInterval(timer);
	}
}
