import { describe, expect, it } from 'vitest';
import { fromLabel, fromValue } from './from.js';

describe('from', () => {
	it('passes start and end through', () => {
		expect(fromValue('start', 'ignored')).toEqual({ from: 'start' });
		expect(fromValue('end', '')).toEqual({ from: 'end' });
	});

	it('turns a picked time into RFC 3339', () => {
		expect(fromValue('time', '2026-10-14T09:00:00Z')).toEqual({ from: '2026-10-14T09:00:00.000Z' });
		expect(fromValue('time', '')).toEqual({ error: 'Pick a date and time' });
	});

	it('needs a message id', () => {
		expect(fromValue('id', ' 01JABC ')).toEqual({ from: '01JABC' });
		expect(fromValue('id', '  ')).toEqual({ error: 'Enter a message id' });
	});

	it('describes a from value in words', () => {
		expect(fromLabel('start')).toBe('the start');
		expect(fromLabel('end')).toBe('the end');
		expect(fromLabel('01JABCDEFGHJKMNPQRSTVWXYZ0')).toBe('message 01JABCDEFGHJKMNPQRSTVWXYZ0');
	});
});
