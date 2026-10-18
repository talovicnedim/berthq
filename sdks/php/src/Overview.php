<?php

declare(strict_types=1);

namespace Berthq;

/**
 * Broker-wide totals, rates and the last minute of throughput, from {@see Client::stats()}.
 */
final class Overview
{
    /**
     * @param int $failed Messages in dead-letter queues.
     * @param array{published: int, delivered: int, acked: int, retried: int, deadLettered: int, dropped: int} $counters
     * @param list<array{at: string, published: int, delivered: int, acked: int, failed: int}> $history
     * @param array{free: int, minFree: int, full: bool} $disk Free bytes on the broker's data disk.
     *        `minFree` is the broker's `--min-free-disk` (0 when the check is off); `full` is true
     *        while publishes are refused with `diskFull`.
     */
    public function __construct(
        public readonly string $version,
        public readonly string $startedAt,
        public readonly int $queues,
        public readonly int $messages,
        public readonly int $backlog,
        public readonly int $processing,
        public readonly int $failed,
        public readonly int $consumers,
        public readonly int $workers,
        public readonly int $publishRate,
        public readonly int $deliverRate,
        public readonly int $ackRate,
        public readonly array $counters,
        public readonly array $history,
        public readonly array $disk,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            version: $data['version'],
            startedAt: $data['startedAt'],
            queues: $data['queues'] ?? 0,
            messages: $data['messages'] ?? 0,
            backlog: $data['backlog'] ?? 0,
            processing: $data['processing'] ?? 0,
            failed: $data['failed'] ?? 0,
            consumers: $data['consumers'] ?? 0,
            workers: $data['workers'] ?? 0,
            publishRate: $data['publishRate'] ?? 0,
            deliverRate: $data['deliverRate'] ?? 0,
            ackRate: $data['ackRate'] ?? 0,
            counters: $data['counters'] ?? [],
            history: $data['history'] ?? [],
            disk: $data['disk'] ?? ['free' => 0, 'minFree' => 0, 'full' => false],
        );
    }
}
