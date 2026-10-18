<script lang="ts">
	import type { StatsPoint } from '#lib/api/index.js';
	import { number } from '#lib/utils/format.js';

	interface Props {
		points: StatsPoint[];
		height?: number;
	}

	let { points, height = 180 }: Props = $props();

	// Fixed order, so a series keeps its color everywhere (dataviz palette slots 1-3).
	const series = [
		{ key: 'published', label: 'Published', color: 'var(--series-published)' },
		{ key: 'acked', label: 'Acked', color: 'var(--series-acked)' },
		{ key: 'failed', label: 'Failed', color: 'var(--series-failed)' }
	] as const;

	const pad = { top: 8, right: 8, bottom: 22, left: 44 };
	let width = $state(600);
	let hover = $state<number>();

	const plotWidth = $derived(Math.max(width - pad.left - pad.right, 10));
	const plotHeight = $derived(height - pad.top - pad.bottom);
	// At least 2, so the middle tick is a whole number.
	const max = $derived(niceMax(Math.max(2, ...points.flatMap((p) => series.map((s) => p[s.key])))));
	const ticks = $derived([0, max / 2, max]);

	function x(index: number): number {
		const slots = Math.max(points.length - 1, 1);
		return pad.left + (index / slots) * plotWidth;
	}

	function y(value: number): number {
		return pad.top + plotHeight - (value / max) * plotHeight;
	}

	function path(key: (typeof series)[number]['key']): string {
		return points.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i)},${y(p[key])}`).join(' ');
	}

	/** Rounds up to 1, 2 or 4 times a power of ten, so the axis and its midpoint are round numbers. */
	function niceMax(value: number): number {
		const power = 10 ** Math.floor(Math.log10(value));
		const step = [1, 2, 4, 10].find((m) => m * power >= value) ?? 10;
		return step * power;
	}

	function track(event: PointerEvent & { currentTarget: SVGSVGElement }) {
		const box = event.currentTarget.getBoundingClientRect();
		const relative = (event.clientX - box.left - pad.left) / plotWidth;
		const index = Math.round(relative * (points.length - 1));
		hover = points.length ? Math.min(Math.max(index, 0), points.length - 1) : undefined;
	}

	const hovered = $derived(hover === undefined ? undefined : points[hover]);
</script>

<div class="grid gap-2">
	<div class="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
		{#each series as s (s.key)}
			<span class="flex items-center gap-1.5">
				<span class="h-0.5 w-3 rounded" style:background={s.color}></span>
				{s.label}
			</span>
		{/each}
		<span class="ml-auto">messages per second, last {points.length}s</span>
	</div>

	<div class="relative" bind:clientWidth={width}>
		<svg
			{width}
			{height}
			role="img"
			aria-label="Throughput: published, acked and failed messages per second"
			onpointermove={track}
			onpointerleave={() => (hover = undefined)}
		>
			{#each ticks as tick (tick)}
				<line
					x1={pad.left}
					x2={pad.left + plotWidth}
					y1={y(tick)}
					y2={y(tick)}
					class="stroke-border"
					stroke-width="1"
				/>
				<text
					x={pad.left - 6}
					y={y(tick)}
					dy="0.32em"
					text-anchor="end"
					class="fill-muted-foreground text-[10px] tabular-nums">{number(tick)}</text
				>
			{/each}
			<text x={pad.left} y={height - 6} class="fill-muted-foreground text-[10px]"
				>{points.length}s ago</text
			>
			<text
				x={pad.left + plotWidth}
				y={height - 6}
				text-anchor="end"
				class="fill-muted-foreground text-[10px]">now</text
			>

			{#if points.length > 1}
				{#each series as s (s.key)}
					<path
						d={path(s.key)}
						fill="none"
						stroke={s.color}
						stroke-width="2"
						stroke-linejoin="round"
						stroke-linecap="round"
					/>
				{/each}
			{/if}

			{#if hover !== undefined && hovered}
				<line
					x1={x(hover)}
					x2={x(hover)}
					y1={pad.top}
					y2={pad.top + plotHeight}
					class="stroke-muted-foreground"
					stroke-width="1"
				/>
				{#each series as s (s.key)}
					<circle
						cx={x(hover)}
						cy={y(hovered[s.key])}
						r="4"
						fill={s.color}
						class="stroke-card"
						stroke-width="2"
					/>
				{/each}
			{/if}
		</svg>

		{#if hover !== undefined && hovered}
			<div
				class="pointer-events-none absolute top-2 rounded-md border bg-popover px-2.5 py-2 text-xs shadow-md"
				style:left="{Math.min(x(hover) + 12, width - 150)}px"
			>
				<div class="mb-1 text-muted-foreground">{new Date(hovered.at).toLocaleTimeString()}</div>
				{#each series as s (s.key)}
					<div class="flex items-center gap-2">
						<span class="h-0.5 w-3 rounded" style:background={s.color}></span>
						<span class="font-semibold tabular-nums">{number(hovered[s.key])}/s</span>
						<span class="text-muted-foreground">{s.label}</span>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
