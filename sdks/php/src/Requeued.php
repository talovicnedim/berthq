<?php

declare(strict_types=1);

namespace Berthq;

/**
 * The result of moving dead letters back to the queue and consumer that failed them.
 */
final class Requeued
{
    /**
     * @param int $skipped The queue or consumer no longer exists, or the consumer already has
     *                     the message pending.
     */
    public function __construct(
        public readonly int $requeued,
        public readonly int $skipped,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self((int) $data['requeued'], (int) $data['skipped']);
    }
}
