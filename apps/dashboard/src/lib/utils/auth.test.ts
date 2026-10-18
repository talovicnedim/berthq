import { describe, expect, it } from 'vitest';
import { keyProblem } from './auth.js';

describe('keyProblem', () => {
	it('asks for a key on 401', () => {
		expect(keyProblem('GET', 401)).toBe('missing');
		expect(keyProblem('POST', 401)).toBe('missing');
	});

	it('asks for a read key when a read is forbidden', () => {
		expect(keyProblem('GET', 403)).toBe('scope');
	});

	it('keeps the key when only a change is forbidden', () => {
		expect(keyProblem('POST', 403)).toBeUndefined();
		expect(keyProblem('DELETE', 403)).toBeUndefined();
	});

	it('keeps the key for other answers and when the broker is unreachable', () => {
		for (const status of [200, 404, 502, 503, undefined]) {
			expect(keyProblem('GET', status)).toBeUndefined();
		}
	});
});
