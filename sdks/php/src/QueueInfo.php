<?php

declare(strict_types=1);

namespace Berthq;

/**
 * A queue with its settings and stats. {@see Client::queuesGet()} returns a
 * {@see QueueDetails}, which adds its consumers and throughput.
 */
class QueueInfo
{
    /**
     * @param array{retentionMs: int, visibilityTimeoutMs: int, maxAttempts: int, retryDelayMs: int, maxMessages: int|null, paused: bool} $settings
     * @param array{messages: int, backlog: int, delayed: int, processing: int, bytes: int, consumers: int, workers: int, counters: array<string, int>, publishRate: int, deliverRate: int, ackRate: int} $stats
     *        `messages` includes messages kept only for replay; `backlog` counts messages at
     *        least one consumer still has to process.
     */
    public function __construct(
        public readonly string $name,
        public readonly string $createdAt,
        public readonly array $settings,
        public readonly array $stats,
        public readonly ?string $deadLetterQueue,
        public readonly ?string $deadLetterOf,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            createdAt: $data['createdAt'],
            settings: $data['settings'] ?? [],
            stats: $data['stats'] ?? [],
            deadLetterQueue: $data['deadLetterQueue'] ?? null,
            deadLetterOf: $data['deadLetterOf'] ?? null,
        );
    }
}
