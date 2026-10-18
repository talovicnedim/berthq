<script lang="ts">
	import { Button } from '#lib/components/ui/button/index.js';
	import * as Dialog from '#lib/components/ui/dialog/index.js';
	import { Input } from '#lib/components/ui/input/index.js';
	import { auth, type KeyPrompt } from '#lib/state/auth.svelte.js';

	const titles: Record<KeyPrompt, string> = {
		missing: 'API key needed',
		scope: 'This key cannot read the dashboard',
		change: 'Change API key'
	};

	let key = $state('');

	function use(next: string): void {
		auth.save(next);
		location.reload();
	}
</script>

<Dialog.Root bind:open={() => auth.prompt !== undefined, (open) => !open && auth.dismiss()}>
	<Dialog.Content class="sm:max-w-md" showCloseButton={auth.prompt === 'change'}>
		<form
			class="grid gap-4"
			onsubmit={(e) => {
				e.preventDefault();
				use(key);
			}}
		>
			<Dialog.Header>
				<Dialog.Title>{auth.prompt ? titles[auth.prompt] : ''}</Dialog.Title>
				<Dialog.Description>
					{#if auth.prompt === 'scope'}
						The broker refused this key. The dashboard needs a key with the <code>read</code> scope,
						or <code>admin</code> to also change things.
					{:else if auth.prompt === 'missing'}
						This broker requires an API key. A <code>read</code> key shows everything; an
						<code>admin</code> key can also change things.
					{:else}
						Enter another key, or forget the saved one. A <code>read</code> key shows everything; an
						<code>admin</code> key can also change things.
					{/if}
					The key is stored in this browser only.
				</Dialog.Description>
			</Dialog.Header>
			<Input type="password" bind:value={key} placeholder="API key" autocomplete="off" autofocus />
			<Dialog.Footer>
				{#if auth.key}
					<Button type="button" variant="ghost" onclick={() => use('')}>Forget saved key</Button>
				{/if}
				<Button type="submit" disabled={key.trim() === ''}>Continue</Button>
			</Dialog.Footer>
		</form>
	</Dialog.Content>
</Dialog.Root>
