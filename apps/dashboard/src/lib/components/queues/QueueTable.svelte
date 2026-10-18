<script lang="ts">
	import type { QueueInfo } from '#lib/api/index.js';
	import { Badge } from '#lib/components/ui/badge/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { bytes, number } from '#lib/utils/format.js';

	interface Props {
		queues: QueueInfo[];
	}

	let { queues }: Props = $props();
</script>

<Table.Root>
	<Table.Header>
		<Table.Row>
			<Table.Head class="pl-4">Queue</Table.Head>
			<Table.Head class="text-right">Backlog</Table.Head>
			<Table.Head class="text-right">Delayed</Table.Head>
			<Table.Head class="text-right">Processing</Table.Head>
			<Table.Head class="text-right">Consumers</Table.Head>
			<Table.Head class="text-right">In/s</Table.Head>
			<Table.Head class="text-right">Acked/s</Table.Head>
			<Table.Head class="text-right">Stored</Table.Head>
			<Table.Head class="pr-4 text-right">Size</Table.Head>
		</Table.Row>
	</Table.Header>
	<Table.Body>
		{#each queues as queue (queue.name)}
			<Table.Row class="relative">
				<Table.Cell class="pl-4 font-medium">
					<a href="/queues/{encodeURIComponent(queue.name)}" class="after:absolute after:inset-0">
						{queue.name}
					</a>
					{#if queue.deadLetterOf}
						<Badge variant="outline" class="ml-1.5 text-state-failed">dead letters</Badge>
					{/if}
					{#if queue.settings.paused}
						<Badge variant="secondary" class="ml-1.5">paused</Badge>
					{/if}
				</Table.Cell>
				<Table.Cell class="text-right tabular-nums">{number(queue.stats.backlog)}</Table.Cell>
				<Table.Cell class="text-right tabular-nums">{number(queue.stats.delayed)}</Table.Cell>
				<Table.Cell class="text-right tabular-nums">{number(queue.stats.processing)}</Table.Cell>
				<Table.Cell class="text-right tabular-nums">{number(queue.stats.consumers)}</Table.Cell>
				<Table.Cell class="text-right tabular-nums">{number(queue.stats.publishRate)}</Table.Cell>
				<Table.Cell class="text-right tabular-nums">{number(queue.stats.ackRate)}</Table.Cell>
				<Table.Cell class="text-right text-muted-foreground tabular-nums"
					>{number(queue.stats.messages)}</Table.Cell
				>
				<Table.Cell class="pr-4 text-right text-muted-foreground tabular-nums"
					>{bytes(queue.stats.bytes)}</Table.Cell
				>
			</Table.Row>
		{/each}
	</Table.Body>
</Table.Root>
