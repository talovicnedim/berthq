<script lang="ts">
	import type { ConsumerMessageState, MessageState } from '#lib/api/index.js';

	interface Props {
		state: MessageState | ConsumerMessageState;
	}

	let { state }: Props = $props();

	const styles: Record<MessageState | ConsumerMessageState, { label: string; dot: string }> = {
		visible: { label: 'Visible', dot: 'bg-state-ready' },
		delayed: { label: 'Delayed', dot: 'bg-state-scheduled' },
		waiting: { label: 'Waiting', dot: 'bg-state-ready' },
		processing: { label: 'Processing', dot: 'bg-state-leased' },
		retrying: { label: 'Retrying', dot: 'bg-state-scheduled' }
	};
</script>

<span class="inline-flex items-center gap-1.5 text-xs">
	<span class="size-2 rounded-full {styles[state].dot}"></span>
	{styles[state].label}
</span>
