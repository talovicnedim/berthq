<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		label: string;
		value: string;
		hint?: string;
		/** Small colored marker shown before the label, e.g. a state dot or icon. */
		marker?: Snippet;
	}

	let { label, value, hint, marker }: Props = $props();
</script>

<div class="rounded-xl border bg-card p-4">
	<div class="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
		{@render marker?.()}
		{label}
	</div>
	<div class="mt-1 text-2xl font-semibold tabular-nums">{value}</div>
	{#if hint}
		<div class="mt-0.5 text-xs text-muted-foreground">{hint}</div>
	{/if}
</div>
