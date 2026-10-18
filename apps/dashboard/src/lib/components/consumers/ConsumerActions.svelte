<script lang="ts">
	import PauseIcon from '@lucide/svelte/icons/pause';
	import PlayIcon from '@lucide/svelte/icons/play';
	import RotateCcwIcon from '@lucide/svelte/icons/rotate-ccw';
	import TrashIcon from '@lucide/svelte/icons/trash-2';
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type ConsumerInfo } from '#lib/api/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { confirm } from '#lib/state/confirm.svelte.js';

	interface Props {
		queue: string;
		consumer: ConsumerInfo;
		onreplay: () => void;
		onchanged: () => void;
	}

	let { queue, consumer, onreplay, onchanged }: Props = $props();

	async function run(action: () => Promise<unknown>, done: string) {
		try {
			await action();
			toast.success(done);
			onchanged();
		} catch (err) {
			toast.error(toApiError(err).message);
		}
	}

	async function remove() {
		const ok = await confirm({
			title: `Remove ${consumer.name}?`,
			description:
				'Removes its position, leases and retries. If a worker consumes as it again, it starts over from the oldest kept message and processes everything again.',
			confirmLabel: 'Remove consumer',
			destructive: true,
			typeToConfirm: consumer.name
		});
		if (ok) await run(() => api.consumers.remove(queue, consumer.name), `Removed ${consumer.name}`);
	}
</script>

<div class="flex justify-end gap-0.5">
	<Button size="icon-sm" variant="ghost" onclick={onreplay} aria-label="Replay" title="Replay">
		<RotateCcwIcon />
	</Button>
	{#if consumer.paused}
		<Button
			size="icon-sm"
			variant="ghost"
			aria-label="Resume"
			title="Resume"
			onclick={() =>
				run(() => api.consumers.resume(queue, consumer.name), `Resumed ${consumer.name}`)}
		>
			<PlayIcon />
		</Button>
	{:else}
		<Button
			size="icon-sm"
			variant="ghost"
			aria-label="Pause"
			title="Pause"
			onclick={() =>
				run(() => api.consumers.pause(queue, consumer.name), `Paused ${consumer.name}`)}
		>
			<PauseIcon />
		</Button>
	{/if}
	<Button size="icon-sm" variant="ghost" onclick={remove} aria-label="Remove" title="Remove">
		<TrashIcon />
	</Button>
</div>
