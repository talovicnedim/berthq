<script lang="ts">
	import LoaderIcon from '@lucide/svelte/icons/loader-circle';
	import PlusIcon from '@lucide/svelte/icons/plus';
	import ThroughputChart from '#lib/components/charts/ThroughputChart.svelte';
	import LoadError from '#lib/components/common/LoadError.svelte';
	import StatTile from '#lib/components/common/StatTile.svelte';
	import NewQueueDialog from '#lib/components/queues/NewQueueDialog.svelte';
	import QueueTable from '#lib/components/queues/QueueTable.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import { live } from '#lib/state/live.svelte.js';
	import { number } from '#lib/utils/format.js';

	let creating = $state(false);

	const overview = $derived(live.stats?.overview);
</script>

{#if overview && live.stats}
	<div class="grid gap-6">
		<div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
			<StatTile
				label="Messages/sec"
				value={number(overview.publishRate)}
				hint="{number(overview.deliverRate)} delivered, {number(overview.ackRate)} acked"
			/>
			<StatTile
				label="Backlog"
				value={number(overview.backlog)}
				hint="still to process, {number(overview.messages)} stored"
			>
				{#snippet marker()}<span class="size-2 rounded-full bg-state-ready"></span>{/snippet}
			</StatTile>
			<StatTile
				label="Processing"
				value={number(overview.processing)}
				hint="{number(overview.consumers)} consumers, {number(overview.workers)} workers"
			>
				{#snippet marker()}<span class="size-2 rounded-full bg-state-leased"></span>{/snippet}
			</StatTile>
			<StatTile label="Failed" value={number(overview.failed)} hint="in dead-letter queues">
				{#snippet marker()}<span class="size-2 rounded-full bg-state-failed"></span>{/snippet}
			</StatTile>
		</div>

		<section class="rounded-xl border bg-card p-4">
			<h2 class="mb-3 text-sm font-medium">Throughput</h2>
			<ThroughputChart points={overview.history} />
		</section>

		<section class="rounded-xl border bg-card">
			<div class="flex items-center justify-between border-b px-4 py-3">
				<h2 class="text-sm font-medium">Queues</h2>
				<Button size="sm" onclick={() => (creating = true)}><PlusIcon /> New queue</Button>
			</div>
			{#if live.stats.queues.length === 0}
				<div class="grid gap-2 px-4 py-12 text-center text-sm text-muted-foreground">
					<p>No queues yet.</p>
					<p>
						Create one here or with
						<code class="rounded bg-muted px-1.5 py-0.5 font-mono">berthq queue create emails</code>
					</p>
				</div>
			{:else}
				<QueueTable queues={live.stats.queues} />
			{/if}
		</section>
	</div>
{:else if live.error}
	<LoadError message={live.error.message} onretry={() => live.retry()} />
{:else}
	<div class="flex items-center justify-center gap-2 py-24 text-sm text-muted-foreground">
		<LoaderIcon class="size-4 animate-spin" /> Connecting to the broker…
	</div>
{/if}

<NewQueueDialog bind:open={creating} />
