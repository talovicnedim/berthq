const FIRST_RETRY_MS = 1000;
const MAX_RETRY_MS = 30_000;

/** How long to wait before reconnect attempt `attempt` (0 for the first): doubles, up to 30 s. */
export function retryDelay(attempt: number): number {
	return Math.min(MAX_RETRY_MS, FIRST_RETRY_MS * 2 ** attempt);
}
