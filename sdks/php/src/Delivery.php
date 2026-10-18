<?php

declare(strict_types=1);

namespace Berthq;

/**
 * A message from `consume`. Settle it with its `lease` (ack, nack or extend) before
 * `leaseExpiresAt`, or the broker retries it. With `autoAck` there is no lease.
 */
final class Delivery
{
    /**
     * @param string $id The same on every redelivery and replay, so it works as an idempotency key.
     * @param string|null $lease Null for auto-acked deliveries.
     * @param mixed $body The JSON body, decoded with objects as associative arrays.
     * @param int $attempt 1 on the first delivery.
     * @param int $visibilityTimeoutMs The lease length, handy for timing `extend` calls. 0 with
     *                                 `autoAck`.
     */
    public function __construct(
        public readonly string $id,
        public readonly ?string $lease,
        public readonly string $queue,
        public readonly string $consumer,
        public readonly mixed $body,
        public readonly int $priority,
        public readonly int $attempt,
        public readonly string $publishedAt,
        public readonly ?string $leaseExpiresAt,
        public readonly ?string $dedupId,
        public readonly ?string $lastError,
        public readonly int $visibilityTimeoutMs = 0,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            id: $data['id'],
            lease: $data['lease'] ?? null,
            queue: $data['queue'],
            consumer: $data['consumer'] ?? 'default',
            body: $data['body'] ?? null,
            priority: $data['priority'] ?? 0,
            attempt: $data['attempt'] ?? 1,
            publishedAt: $data['publishedAt'],
            leaseExpiresAt: $data['leaseExpiresAt'] ?? null,
            dedupId: $data['dedupId'] ?? null,
            lastError: $data['lastError'] ?? null,
            visibilityTimeoutMs: $data['visibilityTimeoutMs'] ?? 0,
        );
    }

    /**
     * The lease ids of deliveries or lease strings, for ack, nack, extend and consume.
     *
     * @internal
     * @param Delivery|string|list<Delivery|string> $leases
     * @return list<string>
     */
    public static function leasesOf(Delivery|string|array $leases): array
    {
        $leases = is_array($leases) ? array_values($leases) : [$leases];

        return array_map(fn (Delivery|string $lease) => $lease instanceof self ? $lease->leaseId() : $lease, $leases);
    }

    private function leaseId(): string
    {
        if ($this->lease === null) {
            throw new BerthqException("message {$this->id} was auto-acked and has no lease to settle", 'invalid');
        }

        return $this->lease;
    }
}
