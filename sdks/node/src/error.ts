/**
 * Error kinds the broker answers with, plus `http` when it could not be reached or its answer
 * could not be read. Unknown kinds from newer brokers still come through as strings.
 */
export type ErrorKind =
	| 'invalid'
	| 'notFound'
	| 'conflict'
	| 'queueFull'
	| 'diskFull'
	| 'unavailable'
	| 'storage'
	| 'unauthorized'
	| 'forbidden'
	| 'http'
	| (string & {});

/** A failed request: the broker's `{ error: { kind, message } }` or a transport failure. */
export class BerthqError extends Error {
	readonly kind: ErrorKind;
	/** The HTTP status code, or 0 when the broker could not be reached. */
	readonly status: number;

	constructor(kind: ErrorKind, message: string, status: number, options?: { cause?: unknown }) {
		super(message, options);
		this.name = 'BerthqError';
		this.kind = kind;
		this.status = status;
	}
}

/** Builds the error for a non-2xx response, falling back to the status when the body isn't ours. */
export async function responseError(response: Response): Promise<BerthqError> {
	const data = (await response.json().catch(() => undefined)) as
		{ error?: { kind?: unknown; message?: unknown } } | undefined;
	const kind = typeof data?.error?.kind === 'string' ? data.error.kind : 'http';
	const message =
		typeof data?.error?.message === 'string'
			? data.error.message
			: `the broker answered ${response.status}`;
	return new BerthqError(kind, message, response.status);
}
