export interface ConfirmRequest {
	title: string;
	description: string;
	confirmLabel: string;
	destructive?: boolean;
	/** When set, the user must type this exactly (used for deleting queues). */
	typeToConfirm?: string;
}

interface Pending extends ConfirmRequest {
	resolve: (confirmed: boolean) => void;
}

/** One confirmation at a time, rendered by `<ConfirmDialog>` in the root layout. */
class ConfirmState {
	pending = $state<Pending>();

	ask(request: ConfirmRequest): Promise<boolean> {
		this.pending?.resolve(false);
		return new Promise((resolve) => {
			this.pending = { ...request, resolve };
		});
	}

	answer(confirmed: boolean): void {
		this.pending?.resolve(confirmed);
		this.pending = undefined;
	}
}

export const confirmState = new ConfirmState();

/** Asks the user to confirm; resolves `false` on cancel. */
export function confirm(request: ConfirmRequest): Promise<boolean> {
	return confirmState.ask(request);
}
