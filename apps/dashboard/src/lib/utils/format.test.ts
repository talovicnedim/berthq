import { describe, expect, it } from 'vitest';
import { bytes, duration, number, parseDuration, preview, relative } from './format.js';

describe('format', () => {
	it('formats counts with separators', () => {
		expect(number(82140)).toBe('82,140');
		expect(number(0)).toBe('0');
	});

	it('formats sizes', () => {
		expect(bytes(512)).toBe('512 B');
		expect(bytes(1536)).toBe('1.5 KB');
		expect(bytes(50 * 1024 * 1024)).toBe('50 MB');
	});

	it('formats durations with at most two units', () => {
		expect(duration(250)).toBe('250ms');
		expect(duration(90_000)).toBe('1m 30s');
		expect(duration(3_661_000)).toBe('1h 1m');
	});

	it('formats relative times both ways', () => {
		const now = Date.parse('2026-10-12T10:00:00Z');
		expect(relative('2026-10-12T09:57:00Z', now)).toBe('3m ago');
		expect(relative('2026-10-12T10:00:20Z', now)).toBe('in 20s');
		expect(relative('2026-10-12T10:00:00.400Z', now)).toBe('now');
	});

	it('parses durations like the CLI', () => {
		expect(parseDuration('30s')).toBe(30_000);
		expect(parseDuration('1.5m')).toBe(90_000);
		expect(parseDuration('500ms')).toBe(500);
		expect(parseDuration('15')).toBe(15_000);
		expect(parseDuration(' 2 h ')).toBe(7_200_000);
		expect(parseDuration('1d')).toBe(86_400_000);
		expect(parseDuration('5w')).toBeUndefined();
		expect(parseDuration('soon')).toBeUndefined();
	});

	it('previews bodies on one line', () => {
		expect(preview({ user: 123 })).toBe('{"user":123}');
		expect(preview('x'.repeat(100), 10)).toBe('xxxxxxxxx…');
	});
});
