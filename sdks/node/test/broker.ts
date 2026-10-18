// Starts two real brokers for the tests: an open one and one that requires api keys.
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { TestProject } from 'vitest/node';

export const ADMIN_KEY = 'adminadminadmin1';
export const PUBLISH_KEY = 'publishpublish12';

declare module 'vitest' {
	export interface ProvidedContext {
		url: string;
		authUrl: string;
	}
}

const binary =
	process.env.BERTHQ_BINARY ?? resolve(import.meta.dirname, '../../../target/debug/berthq');

export default async function setup(project: TestProject) {
	if (!existsSync(binary)) {
		throw new Error(`no berthq binary at ${binary}: run cargo build -p berthq-cli first`);
	}
	const open = await start([]);
	const auth = await start(['--api-key', ADMIN_KEY, '--api-key', `publish:${PUBLISH_KEY}`]);
	project.provide('url', open.url);
	project.provide('authUrl', auth.url);
	return async () => {
		await Promise.all([open.stop(), auth.stop()]);
	};
}

async function start(args: string[]) {
	const data = await mkdtemp(join(tmpdir(), 'berthq-node-sdk-'));
	const port = await freePort();
	const child = spawn(
		binary,
		['start', '--port', String(port), '--data', data, '--shutdown-timeout', '1s', ...args],
		{ stdio: ['ignore', 'ignore', 'pipe'] }
	);
	// Keep the broker's log out of the test output unless it fails to start.
	let log = '';
	child.stderr?.on('data', (chunk) => (log += chunk));
	const url = `http://127.0.0.1:${port}`;
	await waitForHealth(url, child).catch((err: Error) => {
		throw new Error(`${err.message}\n${log}`);
	});
	return {
		url,
		stop: async () => {
			const exited = new Promise((done) => child.once('exit', done));
			child.kill('SIGTERM');
			await exited;
			await rm(data, { recursive: true, force: true });
		}
	};
}

/** Asks the OS for a free port. Another process could grab it before the broker does, but
 * that is unlikely enough for tests. */
function freePort(): Promise<number> {
	return new Promise((done, fail) => {
		const server = createServer();
		server.once('error', fail);
		server.listen(0, '127.0.0.1', () => {
			const address = server.address();
			const port = typeof address === 'object' && address ? address.port : 0;
			server.close(() => done(port));
		});
	});
}

async function waitForHealth(url: string, child: ChildProcess) {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		if (child.exitCode !== null) throw new Error(`broker exited with ${child.exitCode}`);
		const healthy = await fetch(`${url}/healthz`).then(
			(response) => response.ok,
			() => false
		);
		if (healthy) return;
		await new Promise((done) => setTimeout(done, 50));
	}
	child.kill();
	throw new Error(`broker at ${url} did not become healthy`);
}
