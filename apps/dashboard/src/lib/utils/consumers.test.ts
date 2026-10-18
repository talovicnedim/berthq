import { describe, expect, it } from 'vitest';
import type { ConsumerInfo } from '#lib/api/index.js';
import { held, idleHint } from './consumers.js';

const now = Date.parse('2026-10-14T12:00:00Z');

const billing: ConsumerInfo = {
	queue: 'orders',
	name: 'billing',
	createdAt: '2026-10-10T08:00:00Z',
	paused: false,
	stats: { waiting: 1200, processing: 30, retrying: 10 },
	idle: true,
	lastSeen: '2026-10-11T12:00:00Z',
	workers: []
};

describe('consumers', () => {
	it('counts what a consumer still has to finish', () => {
		expect(held(billing)).toBe(1240);
	});

	it('explains an idle consumer that holds messages back', () => {
		expect(idleHint(billing, now)).toBe(
			'Not seen for 3d. 1,240 messages wait for it; they are kept until it catches up or is removed.'
		);
	});

	it('counts from creation when no worker ever came', () => {
		const fresh = { ...billing, lastSeen: null, createdAt: '2026-10-14T10:00:00Z' };
		expect(idleHint(fresh, now)).toMatch(/^No worker since it was created 2h ago\./);
	});

	it('follows the broker: no hint unless it marks the consumer idle', () => {
		expect(idleHint({ ...billing, idle: false }, now)).toBeUndefined();
	});
});
