import type { QueueSettings, QueueSettingsPatch } from '#lib/api/index.js';
import { duration, parseDuration } from './format.js';

/** The queue settings form, as the text the user typed. An empty max messages means "no limit". */
export interface SettingsFields {
	retention: string;
	visibilityTimeout: string;
	maxAttempts: string;
	retryDelay: string;
	maxMessages: string;
}

export function settingsFields(settings: QueueSettings): SettingsFields {
	return {
		retention: compact(settings.retentionMs),
		visibilityTimeout: compact(settings.visibilityTimeoutMs),
		maxAttempts: String(settings.maxAttempts),
		retryDelay: compact(settings.retryDelayMs),
		maxMessages: settings.maxMessages === null ? '' : String(settings.maxMessages)
	};
}

/** Turns the form into an API patch, or says which field is wrong. */
export function settingsPatch(fields: SettingsFields): QueueSettingsPatch | { error: string } {
	const durations: [keyof QueueSettingsPatch, string, string][] = [
		['retentionMs', 'Retention', fields.retention],
		['visibilityTimeoutMs', 'Visibility timeout', fields.visibilityTimeout],
		['retryDelayMs', 'Retry delay', fields.retryDelay]
	];
	const patch: QueueSettingsPatch = {};
	for (const [key, label, text] of durations) {
		const ms = parseDuration(text);
		if (ms === undefined) return { error: `${label}: use e.g. 500ms, 30s, 5m or 1h` };
		patch[key] = ms;
	}
	const counts: [keyof QueueSettingsPatch, string, string][] = [
		['maxAttempts', 'Max attempts', fields.maxAttempts],
		['maxMessages', 'Max messages', fields.maxMessages.trim() === '' ? '0' : fields.maxMessages]
	];
	for (const [key, label, text] of counts) {
		if (!/^\d+$/.test(text.trim())) return { error: `${label} must be a whole number` };
		patch[key] = Number(text);
	}
	return patch;
}

/** Durations as the form shows them: `30s`, `5m`, `1d` rather than `30000`. */
function compact(ms: number): string {
	return ms === 0 ? '0s' : duration(ms).replace(' ', '');
}
