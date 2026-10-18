<script lang="ts">
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import { api, type MessageInfo, type MessageState } from '#lib/api/index.js';
	import PagerButtons from '#lib/components/common/PagerButtons.svelte';
	import QueryView from '#lib/components/common/QueryView.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { Pager } from '#lib/state/pager.svelte.js';
	import { Query } from '#lib/state/query.svelte.js';
	import { bytes, relative } from '#lib/utils/format.js';
	import MessageDialog from './MessageDialog.svelte';

	interface Props {
		queue: string;
	}

	let { queue }: Props = $props();

	const PAGE = 50;
	const states: { label: string; state: MessageState }[] = [
		{ label: 'Visible', state: 'visible' },
		{ label: 'Delayed', state: 'delayed' }
	];

	let shown = $state<MessageState>('visible');
	let selected = $state<MessageInfo>();
	const pager = new Pager();
	const messages = $derived(
		new Query(() => api.messages.list(queue, { state: shown, before: pager.before, limit: PAGE }))
	);
	$effect(() => void messages.refresh());

	function show(next: MessageState) {
		shown = next;
		pager.reset();
	}
</script>

<div class="grid gap-3 pt-2">
	<div class="flex items-center gap-1">
		{#each states as option (option.state)}
			<Button
				size="sm"
				variant={shown === option.state ? 'secondary' : 'ghost'}
				onclick={() => show(option.state)}>{option.label}</Button
			>
		{/each}
		<span class="ml-auto text-xs text-muted-foreground">Newest first, {PAGE} per page</span>
		<Button size="icon-sm" variant="ghost" onclick={() => messages.refresh()} aria-label="Refresh">
			<RefreshCwIcon class={messages.loading ? 'animate-spin' : ''} />
		</Button>
	</div>

	<QueryView
		query={messages}
		empty={shown === 'delayed' ? 'No delayed messages.' : 'No messages here.'}
		isEmpty={(list) => list.length === 0 && pager.newest}
	>
		{#snippet children(list)}
			<div class="rounded-xl border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head class="pl-4">Id</Table.Head>
							<Table.Head class="text-right">Priority</Table.Head>
							<Table.Head>{shown === 'delayed' ? 'Deliver at' : 'Published'}</Table.Head>
							<Table.Head>Dedup id</Table.Head>
							<Table.Head class="pr-4 text-right">Size</Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each list as message (message.id)}
							<Table.Row>
								<Table.Cell class="pl-4">
									<button
										class="font-mono text-xs hover:underline"
										onclick={() => (selected = message)}>{message.id}</button
									>
								</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{message.priority}</Table.Cell>
								<Table.Cell class="text-muted-foreground">
									{relative(
										shown === 'delayed' && message.deliverAt
											? message.deliverAt
											: message.publishedAt
									)}
								</Table.Cell>
								<Table.Cell class="max-w-48 truncate text-muted-foreground"
									>{message.dedupId ?? '-'}</Table.Cell
								>
								<Table.Cell class="pr-4 text-right text-muted-foreground tabular-nums"
									>{bytes(message.size)}</Table.Cell
								>
							</Table.Row>
						{/each}
					</Table.Body>
				</Table.Root>
			</div>
			<PagerButtons {pager} next={list.length === PAGE ? list.at(-1)?.id : undefined} />
		{/snippet}
	</QueryView>
</div>

<MessageDialog
	{queue}
	summary={selected}
	onclose={() => (selected = undefined)}
	onchanged={() => {
		selected = undefined;
		void messages.refresh();
	}}
/>
