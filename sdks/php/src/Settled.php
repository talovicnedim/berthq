<?php

declare(strict_types=1);

namespace Berthq;

/**
 * The result of ack, nack and extend.
 */
final class Settled
{
    /**
     * @param list<string> $unknown Leases that no longer exist: already settled, or expired and
     *                              redelivered. Not an error.
     */
    public function __construct(
        public readonly int $settled,
        public readonly array $unknown,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self((int) $data['settled'], $data['unknown'] ?? []);
    }
}
