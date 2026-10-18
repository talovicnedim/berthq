<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import type { FromChoice } from '#lib/utils/from.js';

	interface Props {
		choice: FromChoice;
		/** The time or message id typed for `time` and `id`. */
		text: string;
		/** Prefix for the input's id, unique on the page. */
		id: string;
	}

	let { choice = $bindable(), text = $bindable(), id }: Props = $props();

	const options: { choice: FromChoice; label: string; hint: string }[] = [
		{ choice: 'start', label: 'Start', hint: 'From the oldest message still kept.' },
		{ choice: 'end', label: 'End', hint: 'Only messages published from now on.' },
		{
			choice: 'time',
			label: 'A time',
			hint: 'Messages that became visible at or after this time.'
		},
		{ choice: 'id', label: 'A message', hint: 'That message and everything after it.' }
	];

	const hint = $derived(options.find((option) => option.choice === choice)?.hint);

	function pick(next: FromChoice) {
		choice = next;
		text = '';
	}
</script>

<div class="grid gap-2">
	<div class="flex flex-wrap gap-1" role="radiogroup" aria-label="Start from">
		{#each options as option (option.choice)}
			<Button
				type="button"
				size="sm"
				role="radio"
				aria-checked={choice === option.choice}
				variant={choice === option.choice ? 'secondary' : 'outline'}
				onclick={() => pick(option.choice)}>{option.label}</Button
			>
		{/each}
	</div>
	{#if choice === 'time'}
		<Input id="{id}-time" type="datetime-local" step="1" bind:value={text} aria-label="Time" />
	{:else if choice === 'id'}
		<Input
			id="{id}-id"
			bind:value={text}
			placeholder="01J…"
			class="font-mono"
			autocomplete="off"
			aria-label="Message id"
		/>
	{/if}
	<p class="text-xs text-muted-foreground">{hint}</p>
</div>
