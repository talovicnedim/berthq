<script lang="ts">
	import PlusIcon from '@lucide/svelte/icons/plus';
	import TriangleAlertIcon from '@lucide/svelte/icons/triangle-alert';
	import type { QueueDetails } from '#lib/api/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { idleHint } from '#lib/utils/consumers.js';
	import { number, relative } from '#lib/utils/format.js';
	import ConsumerActions from './ConsumerActions.svelte';
	import ConsumerDialog from './ConsumerDialog.svelte';
	import NewConsumerDialog from './NewConsumerDialog.svelte';
	import ReplayDialog from './ReplayDialog.svelte';

	interface Props {
		queue: QueueDetails;
		onchanged: () => void;
	}

	let { queue, onchanged }: Props = $props();

	let creating = $state(false);
	// Names, not objects, so the dialogs show the latest polled numbers.
	let viewing = $state<string>();
	let replaying = $state<string>();

	function find(name: string | undefined) {
		return name === undefined ? undefined : queue.consumers.find((c) => c.name === name);
	}
</script>

<div class="grid gap-3 pt-2">
	<div class="flex items-center gap-2">
		<p class="text-xs text-muted-foreground">
			Every consumer gets every message at its own pace; its workers share them.
		</p>
		<Button class="ml-auto" size="sm" onclick={() => (creating = true)}>
			<PlusIcon /> New consumer
		</Button>
	</div>

	{#if queue.consumers.length === 0}
		<div class="grid gap-2 py-12 text-center text-sm text-muted-foreground">
			<p>No consumers yet. A worker creates one on its first consume:</p>
			<p>
				<code class="rounded bg-muted px-1.5 py-0.5 font-mono"
					>berthq consume {queue.name} --consumer billing</code
				>
			</p>
		</div>
	{:else}
		<div class="rounded-xl border">
			<Table.Root>
				<Table.Header>
					<Table.Row>
						<Table.Head class="pl-4">Consumer</Table.Head>
						<Table.Head class="text-right" title="Not received yet">Waiting</Table.Head>
						<Table.Head class="text-right" title="Leased to a worker right now"
							>Processing</Table.Head
						>
						<Table.Head class="text-right" title="Received before, waiting for another attempt"
							>Retrying</Table.Head
						>
						<Table.Head class="text-right">Workers</Table.Head>
						<Table.Head>Last seen</Table.Head>
						<Table.Head class="pr-4"><span class="sr-only">Actions</span></Table.Head>
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each queue.consumers as consumer (consumer.name)}
						{@const hint = idleHint(consumer)}
						<Table.Row>
							<Table.Cell class="pl-4">
								<button
									class="font-medium hover:underline"
									onclick={() => (viewing = consumer.name)}>{consumer.name}</button
								>
								{#if consumer.paused}<Badge variant="secondary" class="ml-1.5">paused</Badge>{/if}
								{#if hint}
									<p class="mt-1 flex max-w-md items-start gap-1 text-xs text-state-scheduled">
										<TriangleAlertIcon class="mt-0.5 size-3 shrink-0" />{hint}
									</p>
								{/if}
							</Table.Cell>
							<Table.Cell class="text-right tabular-nums"
								>{number(consumer.stats.waiting)}</Table.Cell
							>
							<Table.Cell class="text-right tabular-nums"
								>{number(consumer.stats.processing)}</Table.Cell
							>
							<Table.Cell class="text-right tabular-nums"
								>{number(consumer.stats.retrying)}</Table.Cell
							>
							<Table.Cell
								class="text-right tabular-nums"
								title={consumer.workers.map((w) => w.name).join(', ') || undefined}
								>{consumer.workers.length}</Table.Cell
							>
							<Table.Cell class="text-muted-foreground">
								{consumer.lastSeen ? relative(consumer.lastSeen) : 'never'}
							</Table.Cell>
							<Table.Cell class="pr-4">
								<ConsumerActions
									queue={queue.name}
									{consumer}
									onreplay={() => (replaying = consumer.name)}
									{onchanged}
								/>
							</Table.Cell>
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
	{/if}
</div>

<NewConsumerDialog queue={queue.name} bind:open={creating} oncreated={onchanged} />
<ConsumerDialog queue={queue.name} consumer={find(viewing)} onclose={() => (viewing = undefined)} />
<ReplayDialog
	queue={queue.name}
	consumer={find(replaying)}
	onclose={() => (replaying = undefined)}
	onreplayed={onchanged}
/>
