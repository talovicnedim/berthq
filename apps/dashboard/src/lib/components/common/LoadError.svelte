<script lang="ts">
	import CircleAlertIcon from '@lucide/svelte/icons/circle-alert';
	import * as Alert from '#lib/components/ui/alert/index.js';
	import { Button } from '#lib/components/ui/button/index.js';

	interface Props {
		message: string;
		onretry: () => void;
	}

	let { message, onretry }: Props = $props();
</script>

<Alert.Root variant="destructive" class="mb-4">
	<CircleAlertIcon />
	<Alert.Title>Couldn't load data</Alert.Title>
	<Alert.Description>{message}</Alert.Description>
	<Alert.Action>
		<Button size="sm" variant="outline" onclick={onretry}>Retry</Button>
	</Alert.Action>
</Alert.Root>
