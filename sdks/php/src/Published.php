<?php

declare(strict_types=1);

namespace Berthq;

/**
 * The result of a publish.
 */
final class Published
{
    /**
     * @param bool $duplicate True when the `dedupId` was already seen and nothing was stored;
     *                        `id` is then the original message's id.
     */
    public function __construct(
        public readonly string $id,
        public readonly bool $duplicate,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self($data['id'], (bool) ($data['duplicate'] ?? false));
    }
}
