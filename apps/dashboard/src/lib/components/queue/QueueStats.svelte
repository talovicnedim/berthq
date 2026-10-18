<script lang="ts">
	import type { QueueStats } from '#lib/api/index.js';
	import StatTile from '#lib/components/common/StatTile.svelte';
	import { bytes, number } from '#lib/utils/format.js';

	interface Props {
		stats: QueueStats;
	}

	let { stats }: Props = $props();
</script>

<div class="grid grid-cols-2 gap-3 lg:grid-cols-5">
	<StatTile label="Backlog" value={number(stats.backlog)} hint="still to process">
		{#snippet marker()}<span class="size-2 rounded-full bg-state-ready"></span>{/snippet}
	</StatTile>
	<StatTile label="Delayed" value={number(stats.delayed)} hint="not visible yet">
		{#snippet marker()}<span class="size-2 rounded-full bg-state-scheduled"></span>{/snippet}
	</StatTile>
	<StatTile
		label="Processing"
		value={number(stats.processing)}
		hint="{number(stats.consumers)} consumers, {number(stats.workers)} workers"
	>
		{#snippet marker()}<span class="size-2 rounded-full bg-state-leased"></span>{/snippet}
	</StatTile>
	<StatTile
		label="Stored"
		value={number(stats.messages)}
		hint="{bytes(stats.bytes)}, including messages kept for replay"
	/>
	<StatTile
		label="Messages/sec"
		value={number(stats.publishRate)}
		hint="{number(stats.ackRate)} acked, {number(stats.counters.retried)} retried since start"
	/>
</div>
