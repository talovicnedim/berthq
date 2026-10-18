<script lang="ts">
	import { page } from '$app/state';
	import { api } from '#lib/api/index.js';
	import ThroughputChart from '#lib/components/charts/ThroughputChart.svelte';
	import QueryView from '#lib/components/common/QueryView.svelte';
	import ConsumersTab from '#lib/components/consumers/ConsumersTab.svelte';
	import DeadLettersTab from '#lib/components/queue/DeadLettersTab.svelte';
	import MessagesTab from '#lib/components/queue/MessagesTab.svelte';
	import PublishDialog from '#lib/components/queue/PublishDialog.svelte';
	import QueueHeader from '#lib/components/queue/QueueHeader.svelte';
	import QueueStats from '#lib/components/queue/QueueStats.svelte';
	import SettingsTab from '#lib/components/queue/SettingsTab.svelte';
	import * as Tabs from '#lib/components/ui/tabs/index.js';
	import { live } from '#lib/state/live.svelte.js';
	import { Query } from '#lib/state/query.svelte.js';
	import { number } from '#lib/utils/format.js';

	const name = $derived(page.params.name ?? '');
	// Consumers and per-queue history aren't in the live stream; fetch them while the page is open.
	const details = $derived(new Query(() => api.queues.get(name)));
	$effect(() => details.poll(2000));

	let publishing = $state(false);
	let messagesVersion = $state(0);

	const info = $derived(live.queue(name) ?? details.data);

	function changed() {
		messagesVersion += 1;
		void details.refresh();
	}
</script>

<div class="grid gap-6">
	<QueueHeader {name} {info} onpublish={() => (publishing = true)} onchanged={changed} />

	<QueryView query={details}>
		{#snippet children(queue)}
			{@const lettersIn = queue.deadLetterOf ? name : queue.deadLetterQueue}
			{@const deadLetters = lettersIn ? (live.queue(lettersIn)?.stats.messages ?? 0) : 0}
			<div class="grid gap-6">
				<QueueStats stats={info?.stats ?? queue.stats} />

				<section class="rounded-xl border bg-card p-4">
					<h2 class="mb-3 text-sm font-medium">Throughput</h2>
					<ThroughputChart points={queue.history} />
				</section>

				<Tabs.Root value={queue.deadLetterOf ? 'dead-letters' : 'messages'}>
					<Tabs.List>
						{#if !queue.deadLetterOf}
							<Tabs.Trigger value="messages">Messages</Tabs.Trigger>
						{/if}
						<Tabs.Trigger value="consumers">Consumers ({queue.consumers.length})</Tabs.Trigger>
						{#if lettersIn}
							<Tabs.Trigger value="dead-letters">
								{#if deadLetters > 0}<span class="mr-1.5 size-2 rounded-full bg-state-failed"
									></span>{/if}
								Dead letters ({number(deadLetters)})
							</Tabs.Trigger>
						{/if}
						<Tabs.Trigger value="settings">Settings</Tabs.Trigger>
					</Tabs.List>
					{#if !queue.deadLetterOf}
						<Tabs.Content value="messages">
							{#key messagesVersion}
								<MessagesTab queue={name} />
							{/key}
						</Tabs.Content>
					{/if}
					<Tabs.Content value="consumers">
						<ConsumersTab {queue} onchanged={() => details.refresh()} />
					</Tabs.Content>
					{#if lettersIn}
						<Tabs.Content value="dead-letters">
							{#key messagesVersion}
								<DeadLettersTab dlq={lettersIn} onchanged={() => details.refresh()} />
							{/key}
						</Tabs.Content>
					{/if}
					<Tabs.Content value="settings">
						<SettingsTab {queue} onsaved={() => details.refresh()} />
					</Tabs.Content>
				</Tabs.Root>
			</div>
		{/snippet}
	</QueryView>
</div>

<PublishDialog queue={name} bind:open={publishing} onpublished={() => (messagesVersion += 1)} />
