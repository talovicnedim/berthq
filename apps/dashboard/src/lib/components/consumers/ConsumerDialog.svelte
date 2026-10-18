<script lang="ts">
	import type { ConsumerInfo } from '#lib/api/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import { number, relative } from '#lib/utils/format.js';
	import ConsumerMessages from './ConsumerMessages.svelte';

	interface Props {
		queue: string;
		/** The consumer to show; kept current by the queue page's polling. */
		consumer: ConsumerInfo | undefined;
		onclose: () => void;
	}

	let { queue, consumer, onclose }: Props = $props();
</script>

<Dialog.Root open={consumer !== undefined} onOpenChange={(open) => !open && onclose()}>
	<Dialog.Content class="sm:max-w-4xl">
		{#if consumer}
			{@const stats = consumer.stats}
			<Dialog.Header>
				<Dialog.Title class="flex items-center gap-2">
					{consumer.name}
					{#if consumer.paused}<Badge variant="secondary">paused</Badge>{/if}
				</Dialog.Title>
				<Dialog.Description>
					{number(stats.waiting)} waiting, {number(stats.processing)} processing, {number(
						stats.retrying
					)} retrying
				</Dialog.Description>
			</Dialog.Header>

			<div class="grid gap-1 text-sm">
				{#if consumer.workers.length === 0}
					<p class="text-muted-foreground">
						No workers in the last minute{consumer.lastSeen
							? `; last seen ${relative(consumer.lastSeen)}`
							: ''}.
					</p>
				{:else}
					<ul class="grid gap-0.5">
						{#each consumer.workers as worker (worker.name)}
							<li class="flex items-center gap-2">
								<span
									class="size-2 rounded-full {worker.waiting
										? 'bg-state-ready'
										: 'bg-state-leased'}"
								></span>
								<span class="font-medium">{worker.name}</span>
								<span class="text-xs text-muted-foreground">
									{worker.waiting ? 'waiting for messages' : 'working'}, seen {relative(
										worker.lastSeen
									)}
								</span>
							</li>
						{/each}
					</ul>
				{/if}
			</div>

			<Tabs.Root value="processing">
				<Tabs.List>
					<Tabs.Trigger value="waiting">Waiting ({number(stats.waiting)})</Tabs.Trigger>
					<Tabs.Trigger value="processing">Processing ({number(stats.processing)})</Tabs.Trigger>
					<Tabs.Trigger value="retrying">Retrying ({number(stats.retrying)})</Tabs.Trigger>
				</Tabs.List>
				{#each ['waiting', 'processing', 'retrying'] as const as state (state)}
					<Tabs.Content value={state}>
						<ConsumerMessages {queue} consumer={consumer.name} {state} />
					</Tabs.Content>
				{/each}
			</Tabs.Root>
		{/if}
	</Dialog.Content>
</Dialog.Root>
