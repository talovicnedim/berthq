import { describe, expect, it } from 'vitest';
import { retryDelay } from './backoff.js';

describe('retryDelay', () => {
	it('doubles from one second and stops growing at 30 seconds', () => {
		expect([0, 1, 2, 3, 4, 5, 6, 20].map(retryDelay)).toEqual([
			1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000
		]);
	});
});
