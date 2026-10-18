<script lang="ts">
	import { goto } from '$app/navigation';
	import ArrowLeftIcon from '@lucide/svelte/icons/arrow-left';
	import PauseIcon from '@lucide/svelte/icons/pause';
	import PlayIcon from '@lucide/svelte/icons/play';
	import SendIcon from '@lucide/svelte/icons/send';
	import TrashIcon from '@lucide/svelte/icons/trash-2';
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type QueueInfo } from '#lib/api/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import { confirm } from '#lib/state/confirm.svelte.js';
	import { number } from '#lib/utils/format.js';

	interface Props {
		name: string;
		info: QueueInfo | undefined;
		onpublish: () => void;
		onchanged: () => void;
	}

	let { name, info, onpublish, onchanged }: Props = $props();

	async function run(action: () => Promise<string>) {
		try {
			toast.success(await action());
			onchanged();
		} catch (err) {
			toast.error(toApiError(err).message);
		}
	}

	async function purge() {
		const ok = await confirm({
			title: `Purge ${name}?`,
			description:
				'Deletes every message, including ones being processed. Consumers keep their names and settings.',
			confirmLabel: 'Purge',
			destructive: true
		});
		if (ok) await run(async () => `Purged ${number(await api.queues.purge(name))} messages`);
	}

	async function remove() {
		const ok = await confirm({
			title: `Delete ${name}?`,
			description: 'Deletes the queue, its consumers and all its messages.',
			confirmLabel: 'Delete queue',
			destructive: true,
			typeToConfirm: name
		});
		if (!ok) return;
		try {
			await api.queues.remove(name);
			toast.success(`Deleted ${name}`);
			await goto('/');
		} catch (err) {
			toast.error(toApiError(err).message);
		}
	}
</script>

<div class="flex flex-wrap items-center gap-3">
	<Button variant="ghost" size="icon-sm" href="/" aria-label="Back to overview"
		><ArrowLeftIcon /></Button
	>
	<h1 class="text-xl font-semibold">{name}</h1>
	{#if info?.settings.paused}<Badge variant="secondary">paused</Badge>{/if}
	{#if info?.deadLetterOf}
		<Badge variant="outline">
			dead letters of <a
				class="ml-1 underline"
				href="/queues/{encodeURIComponent(info.deadLetterOf)}">{info.deadLetterOf}</a
			>
		</Badge>
	{/if}
	<div class="ml-auto flex flex-wrap gap-2">
		<Button size="sm" variant="outline" onclick={onpublish}><SendIcon /> Publish</Button>
		{#if info?.settings.paused}
			<Button
				size="sm"
				variant="outline"
				onclick={() => run(async () => (await api.queues.resume(name), `Resumed ${name}`))}
			>
				<PlayIcon /> Resume
			</Button>
		{:else}
			<Button
				size="sm"
				variant="outline"
				onclick={() => run(async () => (await api.queues.pause(name), `Paused ${name}`))}
			>
				<PauseIcon /> Pause
			</Button>
		{/if}
		<Button size="sm" variant="outline" onclick={purge}>Purge</Button>
		<Button size="sm" variant="destructive" onclick={remove}><TrashIcon /> Delete</Button>
	</div>
</div>
