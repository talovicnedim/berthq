<script lang="ts">
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import type { SettingsFields } from '#lib/utils/settings.js';

	interface Props {
		fields: SettingsFields;
	}

	let { fields = $bindable() }: Props = $props();

	const rows = [
		[
			'retention',
			'Retention',
			'How long messages stay for replay; always until every consumer has acked them'
		],
		['visibilityTimeout', 'Visibility timeout', 'Time to ack before a retry'],
		['maxAttempts', 'Max attempts', 'Deliveries before dead-lettering (0 = forever)'],
		['retryDelay', 'Retry delay', 'Doubled for every further attempt, up to 5 minutes'],
		[
			'maxMessages',
			'Max messages',
			'Backlog limit; new messages are refused when full (empty = none)'
		]
	] as const;
</script>

<div class="grid gap-3 sm:grid-cols-2">
	{#each rows as [key, label, hint] (key)}
		<div class="grid gap-1">
			<Label for="setting-{key}">{label}</Label>
			<Input id="setting-{key}" bind:value={fields[key]} autocomplete="off" />
			<span class="text-xs text-muted-foreground">{hint}</span>
		</div>
	{/each}
</div>
