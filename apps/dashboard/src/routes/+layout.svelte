<script lang="ts">
	import './layout.css';
	import KeyRoundIcon from '@lucide/svelte/icons/key-round';
	import MoonIcon from '@lucide/svelte/icons/moon';
	import SunIcon from '@lucide/svelte/icons/sun';
	import TriangleAlertIcon from '@lucide/svelte/icons/triangle-alert';
	import { ModeWatcher, toggleMode } from 'mode-watcher';
	import ConfirmDialog from '#lib/components/common/ConfirmDialog.svelte';
	import { Button } from '#lib/components/ui/button/index.js';
	import { Toaster } from '#lib/components/ui/sonner/index.js';
	import ApiKeyDialog from '#lib/components/common/ApiKeyDialog.svelte';
	import { auth } from '#lib/state/auth.svelte.js';
	import { live, type LiveStatus } from '#lib/state/live.svelte.js';
	import { bytes } from '#lib/utils/format.js';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();

	const statuses: Record<LiveStatus, { label: string; dot: string }> = {
		connecting: { label: 'Connecting', dot: 'bg-muted-foreground/40' },
		live: { label: 'Live', dot: 'bg-state-ready' },
		down: { label: 'Reconnecting', dot: 'bg-state-failed' },
		needsKey: { label: 'Needs an API key', dot: 'bg-state-failed' }
	};

	const disk = $derived(live.stats?.overview.disk);
	const status = $derived(statuses[live.status]);

	$effect(() => live.connect(auth.key));
</script>

<ModeWatcher />
<Toaster richColors />
<ConfirmDialog />
<ApiKeyDialog />

<svelte:head>
	<title>Berthq</title>
</svelte:head>

<div class="min-h-full">
	<header class="border-b bg-card/50">
		<div class="mx-auto flex h-14 max-w-7xl items-center gap-4 px-4 sm:px-6">
			<a href="/" class="flex items-center gap-2 font-semibold">
				<img src="/favicon.svg" alt="" class="size-8" />
				Berthq
			</a>
			<nav class="flex items-center gap-1 text-sm">
				<a
					href="/"
					class="rounded-md px-2.5 py-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
					>Queues</a
				>
			</nav>
			<div class="ml-auto flex items-center gap-3 text-xs text-muted-foreground">
				<span class="flex items-center gap-1.5">
					<span class="size-2 rounded-full {status.dot}"></span>
					{status.label}
				</span>
				{#if live.stats}
					<span>v{live.stats.overview.version}</span>
				{/if}
				{#if auth.key}
					<Button
						variant="ghost"
						size="icon-sm"
						onclick={() => auth.ask('change')}
						aria-label="Change API key"
						title="Change API key"
					>
						<KeyRoundIcon />
					</Button>
				{/if}
				<Button variant="ghost" size="icon-sm" onclick={toggleMode} aria-label="Toggle dark mode">
					<SunIcon class="hidden dark:block" />
					<MoonIcon class="dark:hidden" />
				</Button>
			</div>
		</div>
	</header>
	{#if disk?.full}
		<div role="alert" class="border-b border-state-failed/40 bg-state-failed/5">
			<p class="mx-auto flex max-w-7xl items-center gap-2 px-4 py-2 text-sm sm:px-6">
				<TriangleAlertIcon class="size-4 shrink-0 text-state-failed" />
				<span>
					Publishing is paused: the disk is almost full ({bytes(disk.free)} free, Berthq keeps
					{bytes(disk.minFree)} free). It resumes once space is freed.
				</span>
			</p>
		</div>
	{/if}
	<main class="mx-auto max-w-7xl px-4 py-6 sm:px-6">
		{@render children()}
	</main>
</div>
