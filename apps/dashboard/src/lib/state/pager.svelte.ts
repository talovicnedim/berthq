/** Pages through a newest-first list with `before` cursors (the last id of each page above). */
export class Pager {
	#cursors = $state<string[]>([]);

	/** The `before` id for the current page; `undefined` on the newest page. */
	get before(): string | undefined {
		return this.#cursors.at(-1);
	}

	get newest(): boolean {
		return this.#cursors.length === 0;
	}

	/** Moves to the page after `lastId`, the oldest message on the current page. */
	older(lastId: string): void {
		this.#cursors = [...this.#cursors, lastId];
	}

	newer(): void {
		this.#cursors = this.#cursors.slice(0, -1);
	}

	reset(): void {
		this.#cursors = [];
	}
}
