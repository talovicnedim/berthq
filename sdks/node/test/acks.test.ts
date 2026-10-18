import { describe, expect, it } from 'vitest';
import { delivery, fakeBroker } from './fake.js';

describe('process acks', () => {
	it('acks with the next consume and sends the rest on its own when it stops', async () => {
		const broker = fakeBroker({ consumes: [[delivery('L1')], [delivery('L2')]] });
		const controller = new AbortController();
		const seen: unknown[] = [];

		await broker.client.process(
			'jobs',
			(message) => {
				seen.push(message.body);
				if (seen.length === 2) controller.abort();
			},
			{ signal: controller.signal }
		);

		expect(seen).toEqual(['L1', 'L2']);
		const consumes = broker.requests.filter((request) => request.path.endsWith('/consume'));
		expect(consumes.map((request) => request.body.ack)).toEqual([undefined, ['L1']]);
		const acks = broker.requests.filter((request) => request.path.endsWith('/ack'));
		expect(acks.map((request) => request.body.leases)).toEqual([['L2']]);
	});

	it('keeps extending a finished message until the broker has its ack', async () => {
		// The second consume, which carries the ack, takes a while to answer.
		const broker = fakeBroker({ consumes: [[delivery('L1', 40)]], delayMs: 60 });
		const controller = new AbortController();

		await broker.client.process(
			'jobs',
			() => {
				setTimeout(() => controller.abort(), 100);
			},
			{ signal: controller.signal }
		);

		const extends_ = broker.requests.filter((request) => request.path.endsWith('/extend'));
		expect(extends_.length).toBeGreaterThan(0);
		const consumes = broker.requests.filter((request) => request.path.endsWith('/consume'));
		expect(consumes[1].body.ack).toEqual(['L1']);
	});

	it('nacks failures right away instead of waiting for the next consume', async () => {
		const broker = fakeBroker({ consumes: [[delivery('L1')]] });
		const controller = new AbortController();

		await broker.client.process(
			'jobs',
			() => {
				controller.abort();
				throw new Error('boom');
			},
			{ signal: controller.signal }
		);

		const nacks = broker.requests.filter((request) => request.path.endsWith('/nack'));
		expect(nacks.map((request) => request.body.leases)).toEqual([['L1']]);
		expect(broker.requests.some((request) => request.path.endsWith('/ack'))).toBe(false);
	});
});
