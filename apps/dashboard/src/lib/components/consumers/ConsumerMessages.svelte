<script lang="ts">
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import { api, type ConsumerMessage, type ConsumerMessageState } from '#lib/api/index.js';
	import QueryView from '#lib/components/common/QueryView.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Query } from '#lib/state/query.svelte.js';
	import { bytes, relative } from '#lib/utils/format.js';

	interface Props {
		queue: string;
		consumer: string;
		state: ConsumerMessageState;
	}

	let { queue, consumer, state }: Props = $props();

	const help: Record<ConsumerMessageState, string> = {
		waiting: 'Not received yet, in delivery order',
		processing: 'Leased to a worker right now',
		retrying: 'Received before, waiting for another attempt'
	};
	const empty: Record<ConsumerMessageState, string> = {
		waiting: 'Nothing waiting: it has received every message.',
		processing: 'Nothing being processed right now.',
		retrying: 'Nothing waiting for another attempt.'
	};
	const detailLabel: Record<ConsumerMessageState, string> = {
		waiting: 'Size',
		processing: 'Worker',
		retrying: 'Next attempt'
	};

	const messages = $derived(new Query(() => api.consumers.messages(queue, consumer, state)));
	$effect(() => void messages.refresh());

	function detail(message: ConsumerMessage): string {
		if (message.state === 'processing') {
			const lease = message.leaseExpiresAt
				? `, lease ends ${relative(message.leaseExpiresAt)}`
				: '';
			return `${message.worker ?? '-'}${lease}`;
		}
		if (message.state === 'retrying') return message.retryAt ? nextAttempt(message.retryAt) : '-';
		return bytes(message.size);
	}

	/** A retry whose backoff is over goes out with the next consume. */
	function nextAttempt(retryAt: string): string {
		return Date.parse(retryAt) <= Date.now() ? 'due now' : relative(retryAt);
	}
</script>

<div class="grid gap-2 pt-2">
	<div class="flex items-center">
		<span class="text-xs text-muted-foreground">{help[state]} (first 100)</span>
		<Button
			class="ml-auto"
			size="icon-sm"
			variant="ghost"
			onclick={() => messages.refresh()}
			aria-label="Refresh"
		>
			<RefreshCwIcon class={messages.loading ? 'animate-spin' : ''} />
		</Button>
	</div>
	<QueryView query={messages} empty={empty[state]} isEmpty={(list) => list.length === 0}>
		{#snippet children(list)}
			<div class="max-h-96 overflow-auto rounded-xl border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head class="pl-4">Id</Table.Head>
							<Table.Head class="text-right">Priority</Table.Head>
							<Table.Head class="text-right">Attempts</Table.Head>
							<Table.Head>Published</Table.Head>
							<Table.Head>{detailLabel[state]}</Table.Head>
							<Table.Head class="pr-4">Last error</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each list as message (message.id)}
							<Table.Row>
								<Table.Cell class="pl-4 font-mono text-xs">{message.id}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{message.priority}</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{message.attempts}</Table.Cell>
								<Table.Cell class="text-muted-foreground"
									>{relative(message.publishedAt)}</Table.Cell
								>
								<Table.Cell class="text-muted-foreground">{detail(message)}</Table.Cell>
								<Table.Cell
									class="max-w-64 truncate pr-4 text-muted-foreground"
									title={message.lastError ?? undefined}>{message.lastError ?? '-'}</Table.Cell
								>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
		{/snippet}
	</QueryView>
</div>
