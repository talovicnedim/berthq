<?php

declare(strict_types=1);

namespace Berthq;

/**
 * Finished deliveries, waiting for their ack to travel with the next consume call instead of a
 * request of its own. An ack must reach the broker before its lease runs out, so {@see due()}
 * says when one has waited long enough to be sent on its own.
 *
 * @internal Used by {@see Client::process()}.
 */
final class PendingAcks
{
    /** The broker takes up to 1000 acks per request. */
    private const MAX_ACKS = 1000;

    /** @var list<Delivery> */
    private array $deliveries = [];
    private ?float $sendBy = null;

    /** Adds a finished delivery. Its ack is due by `$sendBy` (a microtime(true) timestamp). */
    public function push(Delivery $delivery, float $sendBy): void
    {
        $this->deliveries[] = $delivery;
        $this->sendBy = $this->sendBy === null ? $sendBy : min($this->sendBy, $sendBy);
    }

    /**
     * Up to 1000 deliveries for the next request; they leave the buffer.
     *
     * @return list<Delivery>
     */
    public function take(): array
    {
        $taken = array_splice($this->deliveries, 0, self::MAX_ACKS);
        if ($this->deliveries === []) {
            $this->sendBy = null;
        }

        return $taken;
    }

    /**
     * Puts back deliveries whose request failed. Acking a lease twice is harmless: the second
     * time it is just unknown.
     *
     * @param list<Delivery> $deliveries
     */
    public function restore(array $deliveries): void
    {
        if ($deliveries === []) {
            return;
        }
        array_push($this->deliveries, ...$deliveries);
        $this->sendBy = microtime(true);
    }

    public function due(): bool
    {
        return $this->sendBy !== null && microtime(true) >= $this->sendBy;
    }
}
