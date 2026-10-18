<script lang="ts">
	import { toast } from 'svelte-sonner';
	import { api, toApiError } from '#lib/api/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { fromValue, type FromChoice } from '#lib/utils/from.js';
	import FromPicker from './FromPicker.svelte';

	interface Props {
		queue: string;
		open: boolean;
		oncreated: () => void;
	}

	let { queue, open = $bindable(), oncreated }: Props = $props();

	let name = $state('');
	let choice = $state<FromChoice>('start');
	let text = $state('');
	let saving = $state(false);

	async function create(event: SubmitEvent) {
		event.preventDefault();
		const value = fromValue(choice, text);
		if ('error' in value) {
			toast.error(value.error);
			return;
		}
		saving = true;
		try {
			const consumer = await api.consumers.create(queue, name.trim(), value.from);
			toast.success(`Created ${consumer.name}`);
			oncreated();
			open = false;
			name = '';
			choice = 'start';
			text = '';
		} catch (err) {
			toast.error(toApiError(err).message);
		} finally {
			saving = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<form class="grid gap-4" onsubmit={create}>
			<Dialog.Header>
				<Dialog.Title>New consumer for {queue}</Dialog.Title>
				<Dialog.Description>
					A consumer gets every message at its own pace. Workers also create one on their first
					consume.
				</Dialog.Description>
			</Dialog.Header>
			<div class="grid gap-1">
				<Label for="consumer-name">Name</Label>
				<Input
					id="consumer-name"
					bind:value={name}
					placeholder="billing"
					autocomplete="off"
					autofocus
				/>
			</div>
			<div class="grid gap-1">
				<span class="text-sm font-medium">Starts at</span>
				<FromPicker id="new-consumer" bind:choice bind:text />
			</div>
			<Dialog.Footer>
				<Button type="button" variant="ghost" onclick={() => (open = false)}>Cancel</Button>
				<Button type="submit" disabled={saving || name.trim() === ''}>Create consumer</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>
