<script lang="ts">
	import RefreshCwIcon from '@lucide/svelte/icons/refresh-cw';
	import RotateCcwIcon from '@lucide/svelte/icons/rotate-ccw';
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type MessageInfo } from '#lib/api/index.js';
	import PagerButtons from '#lib/components/common/PagerButtons.svelte';
	import QueryView from '#lib/components/common/QueryView.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Table from '#lib/components/ui/table/index.js';
	import { confirm } from '#lib/state/confirm.svelte.js';
	import { Pager } from '#lib/state/pager.svelte.js';
	import { Query } from '#lib/state/query.svelte.js';
	import { number, relative } from '#lib/utils/format.js';
	import MessageDialog from './MessageDialog.svelte';

	interface Props {
		/** The dead-letter queue to show, e.g. `orders.dlq`. */
		dlq: string;
		onchanged: () => void;
	}

	let { dlq, onchanged }: Props = $props();

	const PAGE = 50;
	let selected = $state<MessageInfo>();
	const pager = new Pager();
	const letters = $derived(
		new Query(() => api.messages.list(dlq, { before: pager.before, limit: PAGE }))
	);
	$effect(() => void letters.refresh());

	async function requeue(ids?: string[]) {
		if (!ids) {
			const ok = await confirm({
				title: `Requeue every dead letter in ${dlq}?`,
				description: 'Each goes back to the consumer that failed it, with attempts reset.',
				confirmLabel: 'Requeue all'
			});
			if (!ok) return;
		}
		try {
			const result = await api.messages.requeue(dlq, ids);
			const skipped = result.skipped ? `, ${number(result.skipped)} skipped` : '';
			toast.success(`Requeued ${number(result.requeued)}${skipped}`);
			pager.reset();
			void letters.refresh();
			onchanged();
		} catch (err) {
			toast.error(toApiError(err).message);
		}
	}
</script>

<div class="grid gap-3 pt-2">
	<div class="flex items-center gap-2">
		<p class="text-xs text-muted-foreground">
			Messages a consumer gave up on. Requeue sends one back to that consumer only.
		</p>
		<Button class="ml-auto" size="sm" onclick={() => requeue()}
			><RotateCcwIcon /> Requeue all</Button
		>
		<Button size="icon-sm" variant="ghost" onclick={() => letters.refresh()} aria-label="Refresh">
			<RefreshCwIcon class={letters.loading ? 'animate-spin' : ''} />
		</Button>
	</div>

	<QueryView
		query={letters}
		empty="No dead letters."
		isEmpty={(list) => list.length === 0 && pager.newest}
	>
		{#snippet children(list)}
			<div class="rounded-xl border">
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.Head class="pl-4">Id</Table.Head>
							<Table.Head>Consumer</Table.Head>
							<Table.Head>Reason</Table.Head>
							<Table.Head class="text-right">Attempts</Table.Head>
							<Table.Head>Failed</Table.Head>
							<Table.Head>Last error</Table.Head>
							<Table.Head class="pr-4"><span class="sr-only">Requeue</span></Table.Head>
						</Table.Row>
					</Table.Header>
					<Table.Body>
						{#each list as letter (letter.id)}
							<Table.Row>
								<Table.Cell class="pl-4">
									<button
										class="font-mono text-xs hover:underline"
										onclick={() => (selected = letter)}>{letter.id}</button
									>
								</Table.Cell>
								<Table.Cell class="font-medium">{letter.deadLetter?.consumer ?? '-'}</Table.Cell>
								<Table.Cell class="text-muted-foreground">
									{letter.deadLetter?.reason === 'rejected' ? 'Rejected' : 'Out of attempts'}
								</Table.Cell>
								<Table.Cell class="text-right tabular-nums">{letter.attempts}</Table.Cell>
								<Table.Cell class="text-muted-foreground">
									{letter.deadLetter ? relative(letter.deadLetter.at) : '-'}
								</Table.Cell>
								<Table.Cell class="max-w-72 truncate text-muted-foreground"
									>{letter.lastError ?? '-'}</Table.Cell
								>
								<Table.Cell class="pr-4 text-right">
									<Button size="sm" variant="ghost" onclick={() => requeue([letter.id])}
										>Requeue</Button
									>
								</Table.Cell>
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
	queue={dlq}
	summary={selected}
	onclose={() => (selected = undefined)}
	onchanged={() => {
		selected = undefined;
		void letters.refresh();
		onchanged();
	}}
/>
