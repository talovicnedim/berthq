/** `82140` → `82,140`. */
export function number(n: number): string {
	return Math.round(n).toLocaleString('en-US');
}

/** `1536` → `1.5 KB`. */
export function bytes(n: number): string {
	if (n < 1024) return `${n} B`;
	const units = ['KB', 'MB', 'GB', 'TB'];
	let value = n / 1024;
	let unit = 0;
	while (value >= 1024 && unit < units.length - 1) {
		value /= 1024;
		unit += 1;
	}
	return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[unit]}`;
}

/** `90000` → `1m 30s`; `250` → `250ms`. */
export function duration(ms: number): string {
	if (ms < 1000) return `${ms}ms`;
	let seconds = Math.round(ms / 1000);
	const parts: string[] = [];
	for (const [unit, size] of [
		['d', 86_400],
		['h', 3_600],
		['m', 60],
		['s', 1]
	] as const) {
		if (seconds >= size) {
			parts.push(`${Math.floor(seconds / size)}${unit}`);
			seconds %= size;
		}
	}
	return parts.slice(0, 2).join(' ');
}

/** How long ago (or until) `iso`, e.g. `3m ago`, `in 20s`. */
export function relative(iso: string, now = Date.now()): string {
	const diff = Date.parse(iso) - now;
	if (Math.abs(diff) < 1000) return 'now';
	const text = duration(Math.abs(diff));
	return diff < 0 ? `${text} ago` : `in ${text}`;
}

/** Parses `30s`, `5m`, `500ms`, `1h`, `2d`, or a bare number of seconds, into milliseconds. */
export function parseDuration(text: string): number | undefined {
	const match = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?\s*$/.exec(text);
	if (!match) return undefined;
	const sizes: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };
	return Math.round(Number(match[1]) * sizes[match[2] ?? 's']);
}

/** A message body for display: strings as they are, everything else as pretty JSON. */
export function body(value: unknown): string {
	return typeof value === 'string' ? value : JSON.stringify(value, null, 2);
}

/** A one-line preview of a body. */
export function preview(value: unknown, max = 80): string {
	const text = typeof value === 'string' ? value : JSON.stringify(value);
	return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}
