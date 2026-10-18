import type { ConsumerInfo } from '#lib/api/index.js';
import { duration, number } from './format.js';

/** Messages a consumer still has to finish. */
export function held(consumer: ConsumerInfo): number {
	const { waiting, processing, retrying } = consumer.stats;
	return waiting + processing + retrying;
}

/** A warning for a consumer the broker marks as idle: the messages it holds can't be deleted. */
export function idleHint(consumer: ConsumerInfo, now = Date.now()): string | undefined {
	if (!consumer.idle) return undefined;
	const quiet = duration(Math.max(now - Date.parse(consumer.lastSeen ?? consumer.createdAt), 0));
	const who = consumer.lastSeen
		? `Not seen for ${quiet}`
		: `No worker since it was created ${quiet} ago`;
	return `${who}. ${number(held(consumer))} messages wait for it; they are kept until it catches up or is removed.`;
}
