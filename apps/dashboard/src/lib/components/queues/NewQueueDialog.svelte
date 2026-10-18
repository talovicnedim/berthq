<script lang="ts">
	import { goto } from '$app/navigation';
	import { toast } from 'svelte-sonner';
	import { api, toApiError } from '#lib/api/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';

	interface Props {
		open: boolean;
	}

	let { open = $bindable() }: Props = $props();

	let name = $state('');
	let saving = $state(false);

	// Only the name: the broker fills in its defaults, and the queue's Settings tab shows them.
	async function create(event: SubmitEvent) {
		event.preventDefault();
		saving = true;
		try {
			const queue = await api.queues.create(name.trim());
			open = false;
			name = '';
			await goto(`/queues/${encodeURIComponent(queue.name)}`);
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
				<Dialog.Title>New queue</Dialog.Title>
				<Dialog.Description>
					The defaults suit most queues. Change them any time in the queue's Settings tab.
				</Dialog.Description>
			</Dialog.Header>
			<div class="grid gap-1">
				<Label for="queue-name">Name</Label>
				<Input
					id="queue-name"
					bind:value={name}
					placeholder="orders"
					autocomplete="off"
					autofocus
				/>
			</div>
			<Dialog.Footer>
				<Button type="button" variant="ghost" onclick={() => (open = false)}>Cancel</Button>
				<Button type="submit" disabled={saving || name.trim() === ''}>Create queue</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>
