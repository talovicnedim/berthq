import { defineConfig } from 'vitest/config';
import tailwindcss from '@tailwindcss/vite';
import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';

// In development the dashboard runs on Vite and talks to a broker on the default port.
const broker = process.env.BERTHQ_URL ?? 'http://127.0.0.1:7331';

export default defineConfig({
	server: {
		port: 5176,
		proxy: { '/v1': broker, '/metrics': broker }
	},
	plugins: [
		tailwindcss(),
		sveltekit({
			compilerOptions: {
				// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true
			},
			adapter: adapter({ fallback: 'index.html' })
		})
	],
	test: {
		expect: { requireAssertions: true },
		include: ['src/**/*.test.ts']
	}
});
