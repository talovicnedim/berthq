<?php

declare(strict_types=1);

namespace Berthq;

/**
 * A name with a saved position in a queue. Every consumer gets every message; its workers
 * share them.
 */
final class ConsumerInfo
{
    /**
     * @param array{waiting: int, processing: int, retrying: int} $stats `waiting`: not received
     *        yet; `retrying`: received before and waiting for the next attempt.
     * @param bool $idle No worker for an hour or more while it still holds messages back from
     *        deletion. Remove it if it is not coming back.
     * @param string|null $lastSeen The last time a worker consumed for it; null if never.
     * @param list<array{name: string, lastSeen: string, waiting: bool}> $workers Seen in the last minute.
     */
    public function __construct(
        public readonly string $queue,
        public readonly string $name,
        public readonly string $createdAt,
        public readonly bool $paused,
        public readonly array $stats,
        public readonly bool $idle,
        public readonly ?string $lastSeen,
        public readonly array $workers,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            queue: $data['queue'],
            name: $data['name'],
            createdAt: $data['createdAt'],
            paused: (bool) ($data['paused'] ?? false),
            stats: $data['stats'] ?? ['waiting' => 0, 'processing' => 0, 'retrying' => 0],
            idle: (bool) ($data['idle'] ?? false),
            lastSeen: $data['lastSeen'] ?? null,
            workers: $data['workers'] ?? [],
        );
    }
}
