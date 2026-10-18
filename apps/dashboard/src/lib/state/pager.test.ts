import { describe, expect, it } from 'vitest';
import { Pager } from './pager.svelte.js';

describe('pager', () => {
	it('walks to older pages and back', () => {
		const pager = new Pager();
		expect(pager.before).toBeUndefined();
		expect(pager.newest).toBe(true);
		pager.older('01JB');
		pager.older('01JA');
		expect(pager.before).toBe('01JA');
		expect(pager.newest).toBe(false);
		pager.newer();
		expect(pager.before).toBe('01JB');
		pager.reset();
		expect(pager.before).toBeUndefined();
	});
});
