<script lang="ts">
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type NewMessage } from '#lib/api/index.js';
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { Label } from '#lib/components/ui/label/index.js';
	import { Textarea } from '#lib/components/ui/textarea/index.js';
	import { parseDuration } from '#lib/utils/format.js';

	interface Props {
		queue: string;
		open: boolean;
		onpublished: () => void;
	}

	let { queue, open = $bindable(), onpublished }: Props = $props();

	let text = $state('{\n  "hello": "world"\n}');
	let priority = $state('0');
	let delay = $state('');
	let dedupId = $state('');
	let sending = $state(false);

	/** JSON when it parses, otherwise plain text, like `berthq publish`. */
	function parseBody(input: string): unknown {
		try {
			return JSON.parse(input);
		} catch {
			return input;
		}
	}

	async function publish(event: SubmitEvent) {
		event.preventDefault();
		const message: NewMessage = { body: parseBody(text), priority: Number(priority) };
		if (delay.trim()) {
			const ms = parseDuration(delay);
			if (ms === undefined) {
				toast.error('Delay: use e.g. 10s, 5m or 1h');
				return;
			}
			message.delayMs = ms;
		}
		if (dedupId.trim()) message.dedupId = dedupId.trim();
		sending = true;
		try {
			const published = await api.messages.publish(queue, message);
			toast.success(
				published.duplicate ? 'Duplicate: already published within the dedup window' : 'Published'
			);
			onpublished();
			open = false;
		} catch (err) {
			toast.error(toApiError(err).message);
		} finally {
			sending = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-lg">
		<form class="grid gap-4" onsubmit={publish}>
			<Dialog.Header>
				<Dialog.Title>Publish to {queue}</Dialog.Title>
				<Dialog.Description>JSON, or plain text sent as a string.</Dialog.Description>
			</Dialog.Header>
			<Textarea bind:value={text} rows={8} class="font-mono text-xs" />
			<div class="grid grid-cols-3 gap-3">
				<div class="grid gap-1">
					<Label for="publish-priority">Priority</Label>
					<select
						id="publish-priority"
						bind:value={priority}
						class="h-9 rounded-md border border-input bg-transparent px-2 text-sm dark:bg-input/30"
					>
						{#each Array.from({ length: 10 }, (_, i) => String(i)) as value (value)}
							<option {value}>{value}</option>
						{/each}
					</select>
				</div>
				<div class="grid gap-1">
					<Label for="publish-delay">Delay</Label>
					<Input id="publish-delay" bind:value={delay} placeholder="none" />
				</div>
				<div class="grid gap-1">
					<Label for="publish-dedup">Dedup id</Label>
					<Input id="publish-dedup" bind:value={dedupId} placeholder="none" />
				</div>
			</div>
			<Dialog.Footer>
				<Button type="button" variant="ghost" onclick={() => (open = false)}>Cancel</Button>
				<Button type="submit" disabled={sending}>Publish</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>
