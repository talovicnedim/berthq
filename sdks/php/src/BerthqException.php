<?php

declare(strict_types=1);

namespace Berthq;

use RuntimeException;
use Throwable;

/**
 * A failed request. `kind` is the broker's error kind (`invalid`, `notFound`, `conflict`,
 * `queueFull`, `diskFull`, `unavailable`, `storage`, `unauthorized`, `forbidden`), or `http` when
 * the broker could not be reached or answered something that isn't JSON. `status` is 0 for
 * transport errors.
 */
final class BerthqException extends RuntimeException
{
    public function __construct(
        string $message,
        public readonly string $kind = 'http',
        public readonly int $status = 0,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $status, $previous);
    }

    /** True when the broker could not be reached at all (refused, DNS, timeout, TLS). */
    public function isTransport(): bool
    {
        return $this->status === 0;
    }
}
