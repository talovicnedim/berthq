import type { KeyProblem } from '#lib/utils/auth.js';

const STORAGE_KEY = 'berthq.apiKey';

/** Why the key dialog is open: the broker refused the key, or the user wants another one. */
export type KeyPrompt = KeyProblem | 'change';

/**
 * The API key the dashboard sends, kept in this browser's local storage. `prompt` is set when
 * the broker refuses the key (401, or 403 on a read), which shows the key dialog.
 */
class Auth {
	key = $state(localStorage.getItem(STORAGE_KEY) ?? '');
	prompt = $state<KeyPrompt>();

	ask(prompt: KeyPrompt): void {
		this.prompt = prompt;
	}

	/** Only a dialog the user opened can be closed: without a working key nothing loads. */
	dismiss(): void {
		if (this.prompt === 'change') this.prompt = undefined;
	}

	/** Saves `key`, or forgets the saved one when it is empty. */
	save(key: string): void {
		this.key = key.trim();
		this.prompt = undefined;
		if (this.key) localStorage.setItem(STORAGE_KEY, this.key);
		else localStorage.removeItem(STORAGE_KEY);
	}
}

export const auth = new Auth();
