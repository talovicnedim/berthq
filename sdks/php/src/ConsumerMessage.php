<?php

declare(strict_types=1);

namespace Berthq;

/**
 * A message as one consumer sees it, from {@see Client::consumersMessages()}. No body.
 */
final class ConsumerMessage
{
    /**
     * @param string $state `waiting` (not received yet), `processing` or `retrying`.
     * @param string|null $worker Set while processing.
     * @param string|null $leaseExpiresAt Set while processing.
     * @param string|null $retryAt Set while retrying.
     */
    public function __construct(
        public readonly string $id,
        public readonly string $queue,
        public readonly string $consumer,
        public readonly string $state,
        public readonly int $priority,
        public readonly int $attempts,
        public readonly string $publishedAt,
        public readonly ?string $lastError,
        public readonly ?string $worker,
        public readonly ?string $leaseExpiresAt,
        public readonly ?string $retryAt,
        public readonly int $size,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            id: $data['id'],
            queue: $data['queue'],
            consumer: $data['consumer'],
            state: $data['state'],
            priority: $data['priority'] ?? 0,
            attempts: $data['attempts'] ?? 0,
            publishedAt: $data['publishedAt'],
            lastError: $data['lastError'] ?? null,
            worker: $data['worker'] ?? null,
            leaseExpiresAt: $data['leaseExpiresAt'] ?? null,
            retryAt: $data['retryAt'] ?? null,
            size: $data['size'] ?? 0,
        );
    }
}
