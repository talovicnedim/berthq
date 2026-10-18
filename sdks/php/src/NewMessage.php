<?php

declare(strict_types=1);

namespace Berthq;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;

/**
 * A message to publish. Only `body` is required.
 */
final class NewMessage
{
    /**
     * @param mixed $body Any JSON-encodable value. Plain text becomes a JSON string.
     * @param int|null $priority 0-9, higher is delivered first.
     * @param int|null $delayMs Hide the message for this long.
     * @param DateTimeInterface|string|null $deliverAt Hide it until then (overrides `delayMs`).
     * @param string|null $dedupId Repeats of this id on the same queue within an hour are ignored.
     */
    public function __construct(
        public readonly mixed $body,
        public readonly ?int $priority = null,
        public readonly ?int $delayMs = null,
        public readonly DateTimeInterface|string|null $deliverAt = null,
        public readonly ?string $dedupId = null,
    ) {
    }

    /**
     * A message from a body and options, or `$body` itself when it already is a {@see NewMessage}
     * (then the options are ignored).
     */
    public static function from(
        mixed $body,
        ?int $priority = null,
        ?int $delayMs = null,
        DateTimeInterface|string|null $deliverAt = null,
        ?string $dedupId = null,
    ): self {
        return $body instanceof self ? $body : new self($body, $priority, $delayMs, $deliverAt, $dedupId);
    }

    /** @internal The request body, without unset fields. */
    public function toArray(): array
    {
        $fields = [
            'body' => $this->body,
            'priority' => $this->priority,
            'delayMs' => $this->delayMs,
            'deliverAt' => self::time($this->deliverAt),
            'dedupId' => $this->dedupId,
        ];

        return array_filter($fields, fn (mixed $value, string $key) => $key === 'body' || $value !== null, ARRAY_FILTER_USE_BOTH);
    }

    private static function time(DateTimeInterface|string|null $time): ?string
    {
        if (!$time instanceof DateTimeInterface) {
            return $time;
        }

        return DateTimeImmutable::createFromInterface($time)
            ->setTimezone(new DateTimeZone('UTC'))
            ->format('Y-m-d\TH:i:s.v\Z');
    }
}
