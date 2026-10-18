import { describe, expect, it } from 'vitest';
import type { QueueSettings } from '#lib/api/index.js';
import { settingsFields, settingsPatch } from './settings.js';

const settings: QueueSettings = {
	retentionMs: 86_400_000,
	visibilityTimeoutMs: 30_000,
	maxAttempts: 5,
	retryDelayMs: 1_000,
	maxMessages: null,
	paused: false
};

describe('queue settings form', () => {
	it('round-trips settings through the form', () => {
		const fields = settingsFields(settings);
		expect(fields.retention).toBe('1d');
		expect(fields.visibilityTimeout).toBe('30s');
		expect(fields.retryDelay).toBe('1s');
		expect(fields.maxMessages).toBe('');
		expect(settingsPatch(fields)).toEqual({
			retentionMs: 86_400_000,
			visibilityTimeoutMs: 30_000,
			maxAttempts: 5,
			retryDelayMs: 1_000,
			maxMessages: 0
		});
	});

	it('keeps a backlog limit that is set', () => {
		const fields = settingsFields({ ...settings, maxMessages: 1000 });
		expect(fields.maxMessages).toBe('1000');
		expect(settingsPatch(fields)).toMatchObject({ maxMessages: 1000 });
	});

	it('shows no retention as 0s', () => {
		const fields = settingsFields({ ...settings, retentionMs: 0 });
		expect(fields.retention).toBe('0s');
		expect(settingsPatch(fields)).toMatchObject({ retentionMs: 0 });
	});

	it('names the field that is wrong', () => {
		const fields = { ...settingsFields(settings), retryDelay: 'soon' };
		expect(settingsPatch(fields)).toEqual({ error: 'Retry delay: use e.g. 500ms, 30s, 5m or 1h' });
		const attempts = { ...settingsFields(settings), maxAttempts: '-1' };
		expect(settingsPatch(attempts)).toEqual({ error: 'Max attempts must be a whole number' });
		const retention = { ...settingsFields(settings), retention: 'a day' };
		expect(settingsPatch(retention)).toEqual({ error: 'Retention: use e.g. 500ms, 30s, 5m or 1h' });
		const limit = { ...settingsFields(settings), maxMessages: 'lots' };
		expect(settingsPatch(limit)).toEqual({ error: 'Max messages must be a whole number' });
	});
});
