<script lang="ts">
	import ChevronLeftIcon from '@lucide/svelte/icons/chevron-left';
	import ChevronRightIcon from '@lucide/svelte/icons/chevron-right';
	import { Button } from '#lib/components/ui/button/index.js';
	import type { Pager } from '#lib/state/pager.svelte.js';

	interface Props {
		pager: Pager;
		/** The oldest id on the current page when the page is full, so there may be more. */
		next: string | undefined;
	}

	let { pager, next }: Props = $props();
</script>

<div class="flex items-center justify-end gap-1">
	<Button size="sm" variant="ghost" disabled={pager.newest} onclick={() => pager.newer()}>
		<ChevronLeftIcon /> Newer
	</Button>
	<Button
		size="sm"
		variant="ghost"
		disabled={next === undefined}
		onclick={() => next && pager.older(next)}
	>
		Older <ChevronRightIcon />
	</Button>
</div>
