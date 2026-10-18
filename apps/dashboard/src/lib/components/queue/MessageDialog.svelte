<script lang="ts">
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type MessageInfo } from '#lib/api/index.js';
	import StateBadge from '#lib/components/common/StateBadge.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { confirm } from '#lib/state/confirm.svelte.js';
	import { body, bytes } from '#lib/utils/format.js';

	interface Props {
		queue: string;
		/** The message picked in the list; the full message (with body) is loaded from it. */
		summary: MessageInfo | undefined;
		onclose: () => void;
		onchanged: () => void;
	}

	let { queue, summary, onclose, onchanged }: Props = $props();

	let full = $state<MessageInfo>();
	let error = $state<string>();

	$effect(() => {
		full = undefined;
		error = undefined;
		if (!summary) return;
		const id = summary.id;
		api.messages
			.get(queue, id)
			.then((message) => {
				if (summary?.id === id) full = message;
			})
			.catch((err: unknown) => (error = toApiError(err).message));
	});

	const message = $derived(full ?? summary);
	const deadLetter = $derived(message?.deadLetter);

	async function remove() {
		if (!message) return;
		const ok = await confirm({
			title: 'Delete this message?',
			description:
				'It is deleted for every consumer. Workers processing it right now can no longer ack it.',
			confirmLabel: 'Delete',
			destructive: true
		});
		if (!ok) return;
		try {
			await api.messages.remove(queue, message.id);
			toast.success('Message deleted');
			onchanged();
		} catch (err) {
			toast.error(toApiError(err).message);
		}
	}

	async function requeue() {
		if (!message || !deadLetter) return;
		try {
			const result = await api.messages.requeue(queue, [message.id]);
			if (result.requeued === 1) toast.success(`Requeued to ${deadLetter.consumer}`);
			else
				toast.error(
					'Not requeued: its queue or consumer is gone, or the consumer is already processing or retrying this message'
				);
			onchanged();
		} catch (err) {
			toast.error(toApiError(err).message);
		}
	}

	function when(iso: string | null): string | null {
		return iso && new Date(iso).toLocaleString();
	}

	const rows = $derived(
		message
			? [
					['Priority', String(message.priority)],
					['Published', when(message.publishedAt)],
					['Deliver at', when(message.deliverAt)],
					['Dedup id', message.dedupId],
					['Size', bytes(message.size)],
					['Attempts', message.attempts > 0 ? String(message.attempts) : null],
					[
						'Dead-lettered',
						deadLetter &&
							`${when(deadLetter.at)}: ${deadLetter.consumer} of ${deadLetter.queue} ${deadLetter.reason === 'rejected' ? 'rejected it' : 'ran out of attempts'}`
					]
				].filter((row): row is [string, string] => Boolean(row[1]))
			: []
	);
</script>

<Dialog.Root open={summary !== undefined} onOpenChange={(open) => !open && onclose()}>
	<Dialog.Content class="sm:max-w-2xl">
		{#if message}
			<Dialog.Header>
				<Dialog.Title class="font-mono text-sm">{message.id}</Dialog.Title>
				<Dialog.Description><StateBadge state={message.state} /></Dialog.Description>
			</Dialog.Header>

			<dl class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1 text-sm">
				{#each rows as [label, value] (label)}
					<dt class="text-muted-foreground">{label}</dt>
					<dd class="break-all">{value}</dd>
				{/each}
			</dl>

			{#if message.lastError}
				<div class="rounded-md border border-state-failed/40 bg-state-failed/5 p-3 text-sm">
					<div class="mb-1 font-medium">Last error</div>
					<pre class="font-mono text-xs whitespace-pre-wrap">{message.lastError}</pre>
				</div>
			{/if}

			<div class="grid gap-1">
				<div class="text-sm font-medium">Body</div>
				<pre
					class="max-h-80 overflow-auto rounded-md bg-muted p-3 font-mono text-xs select-text">{#if error}{error}{:else if full}{body(
							full.body
						)}{:else}Loading…{/if}</pre>
			</div>

			<Dialog.Footer>
				<Button variant="destructive" onclick={remove}>Delete</Button>
				{#if deadLetter}
					<Button onclick={requeue}>Requeue to {deadLetter.consumer}</Button>
				{/if}
			</Dialog.Footer>
		{/if}
	</Dialog.Content>
</Dialog.Root>
