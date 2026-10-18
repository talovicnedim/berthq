import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		globalSetup: ['./test/broker.ts'],
		testTimeout: 20_000
	}
});
