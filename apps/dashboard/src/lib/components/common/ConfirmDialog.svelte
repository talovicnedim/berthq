<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { confirmState } from '#lib/state/confirm.svelte.js';

	let typed = $state('');

	const request = $derived(confirmState.pending);
	const allowed = $derived(!request?.typeToConfirm || typed === request.typeToConfirm);

	function answer(confirmed: boolean) {
		typed = '';
		confirmState.answer(confirmed);
	}
</script>

<Dialog.Root bind:open={() => request !== undefined, (open) => !open && answer(false)}>
	<!-- Above other dialogs: confirmations are often asked from inside a form dialog. -->
	<Dialog.Content class="z-60 shadow-2xl sm:max-w-md">
		{#if request}
			<form
				class="grid gap-4"
				onsubmit={(e) => {
					e.preventDefault();
					if (allowed) answer(true);
				}}
			>
				<Dialog.Header>
					<Dialog.Title>{request.title}</Dialog.Title>
					<Dialog.Description>{request.description}</Dialog.Description>
				</Dialog.Header>

				{#if request.typeToConfirm}
					<div class="grid gap-1.5 text-sm">
						<label for="confirm-text">
							Type
							<code class="rounded bg-muted px-1 font-semibold">{request.typeToConfirm}</code> to continue.
						</label>
						<Input id="confirm-text" bind:value={typed} autocomplete="off" autofocus />
					</div>
				{/if}

				<Dialog.Footer class="gap-2">
					<Button type="button" variant="ghost" onclick={() => answer(false)}>Cancel</Button>
					<Button
						type="submit"
						variant={request.destructive ? 'destructive' : 'default'}
						disabled={!allowed}
					>
						{request.confirmLabel}
					</Button>
				</Dialog.Footer>
			</form>
		{/if}
	</Dialog.Content>
</Dialog.Root>
