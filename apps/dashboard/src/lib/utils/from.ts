/** Where a new consumer starts, or where a replay moves a consumer. */
export type FromChoice = 'start' | 'end' | 'time' | 'id';

/** The API's `from` value for a choice and the text typed for it, or what is wrong. */
export function fromValue(choice: FromChoice, text: string): { from: string } | { error: string } {
	if (choice === 'start' || choice === 'end') return { from: choice };
	const value = text.trim();
	if (choice === 'id') return value ? { from: value } : { error: 'Enter a message id' };
	// A `datetime-local` value has no time zone, so it is read as the browser's local time.
	const ms = Date.parse(value);
	if (Number.isNaN(ms)) return { error: 'Pick a date and time' };
	return { from: new Date(ms).toISOString() };
}

/** A `from` value in words, for messages like "Replaying billing from the start". */
export function fromLabel(from: string): string {
	if (from === 'start') return 'the start';
	if (from === 'end') return 'the end';
	const ms = Date.parse(from);
	return Number.isNaN(ms) ? `message ${from}` : new Date(ms).toLocaleString();
}
