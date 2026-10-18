<script lang="ts">
	import { untrack } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { api, toApiError, type QueueInfo } from '#lib/api/index.js';
	import SettingsForm from '#lib/components/queues/SettingsForm.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import { settingsFields, settingsPatch } from '#lib/utils/settings.js';

	interface Props {
		queue: QueueInfo;
		onsaved: () => void;
	}

	let { queue, onsaved }: Props = $props();

	// Edits start from the settings when the tab opens; live updates don't overwrite typing.
	let fields = $state(settingsFields(untrack(() => queue.settings)));
	let saving = $state(false);

	async function save(event: SubmitEvent) {
		event.preventDefault();
		const patch = settingsPatch(fields);
		if ('error' in patch) {
			toast.error(patch.error);
			return;
		}
		saving = true;
		try {
			const updated = await api.queues.update(queue.name, patch);
			fields = settingsFields(updated.settings);
			toast.success('Settings saved');
			onsaved();
		} catch (err) {
			toast.error(toApiError(err).message);
		} finally {
			saving = false;
		}
	}
</script>

<form class="grid max-w-2xl gap-4 pt-2" onsubmit={save}>
	<SettingsForm bind:fields />
	<div class="flex gap-2">
		<Button type="submit" disabled={saving}>Save settings</Button>
		<Button type="button" variant="ghost" onclick={() => (fields = settingsFields(queue.settings))}
			>Reset</Button
		>
	</div>
</form>
