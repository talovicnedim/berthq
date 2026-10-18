<script lang="ts">
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type ConsumerInfo } from '#lib/api/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { fromLabel, fromValue, type FromChoice } from '#lib/utils/from.js';
	import FromPicker from './FromPicker.svelte';

	interface Props {
		queue: string;
		/** The consumer to replay; the dialog is open while it is set. */
		consumer: ConsumerInfo | undefined;
		onclose: () => void;
		onreplayed: () => void;
	}

	let { queue, consumer, onclose, onreplayed }: Props = $props();

	let choice = $state<FromChoice>('start');
	let text = $state('');
	let saving = $state(false);

	function close() {
		choice = 'start';
		text = '';
		onclose();
	}

	async function replay(event: SubmitEvent) {
		event.preventDefault();
		if (!consumer) return;
		const value = fromValue(choice, text);
		if ('error' in value) {
			toast.error(value.error);
			return;
		}
		saving = true;
		try {
			await api.consumers.replay(queue, consumer.name, value.from);
			toast.success(`Replaying ${consumer.name} from ${fromLabel(value.from)}`);
			onreplayed();
			close();
		} catch (err) {
			toast.error(toApiError(err).message);
		} finally {
			saving = false;
		}
	}
</script>

<Dialog.Root open={consumer !== undefined} onOpenChange={(open) => !open && close()}>
	<Dialog.Content class="sm:max-w-md">
		{#if consumer}
			<form class="grid gap-4" onsubmit={replay}>
				<Dialog.Header>
					<Dialog.Title>Replay {consumer.name}</Dialog.Title>
					<Dialog.Description>
						Moves its position. What it is processing or retrying now is dropped, and messages it
						already got may come again, so its workers must handle repeats.
					</Dialog.Description>
				</Dialog.Header>
				<FromPicker id="replay" bind:choice bind:text />
				<Dialog.Footer>
					<Button type="button" variant="ghost" onclick={close}>Cancel</Button>
					<Button type="submit" disabled={saving}>Replay</Button>
				</Dialog.Footer>
			</form>
		{/if}
	</Dialog.Content>
</Dialog.Root>
