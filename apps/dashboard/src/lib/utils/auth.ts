/** `missing`: no key or an unknown one. `scope`: the key may not read. */
export type KeyProblem = 'missing' | 'scope';

/**
 * Whether an answer means the dashboard needs another API key. A 403 counts only for reads:
 * a `read` key may not change things, and that is no reason to ask for a new key.
 */
export function keyProblem(method: string, status: number | undefined): KeyProblem | undefined {
	if (status === 401) return 'missing';
	if (status === 403 && method === 'GET') return 'scope';
	return undefined;
}
