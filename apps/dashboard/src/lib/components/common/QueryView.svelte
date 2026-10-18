<script lang="ts" generics="T">
	import type { Snippet } from 'svelte';
	import LoaderIcon from '@lucide/svelte/icons/loader-circle';
	import LoadError from '#lib/components/common/LoadError.svelte';
	import type { Query } from '#lib/state/query.svelte.js';

	interface Props {
		query: Query<T>;
		/** Text shown when `isEmpty(data)` is true. */
		empty?: string;
		isEmpty?: (data: T) => boolean;
		children: Snippet<[T]>;
	}

	let { query, empty = 'Nothing here yet.', isEmpty, children }: Props = $props();
</script>

{#if query.error}
	<LoadError message={query.error.message} onretry={() => query.refresh()} />
{/if}

{#if query.data !== undefined}
	{#if isEmpty?.(query.data)}
		<p class="py-12 text-center text-sm text-muted-foreground">{empty}</p>
	{:else}
		{@render children(query.data)}
	{/if}
{:else if !query.error}
	<div class="flex items-center justify-center gap-2 py-12 text-sm text-muted-foreground">
		<LoaderIcon class="size-4 animate-spin" /> Loading…
	</div>
{/if}
