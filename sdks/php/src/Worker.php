<?php

declare(strict_types=1);

namespace Berthq;

use Closure;
use RuntimeException;
use Throwable;

/**
 * The loop behind {@see Client::process()}. Acks travel with the next consume call and are sent
 * on their own before half the lease has passed, and when the loop ends for any reason.
 *
 * @internal Use {@see Client::process()}.
 */
final class Worker
{
    /** Most seconds to wait between tries while the broker is shutting down or unreachable. */
    private const MAX_BACKOFF = 30;

    private readonly PendingAcks $acks;
    private int $backoff = 1;

    public function __construct(
        private readonly Client $client,
        private readonly string $queue,
        private readonly Closure $handler,
        private readonly Closure $onError,
        private readonly int $batch,
        private readonly int $waitMs,
        private readonly ?string $consumer,
        private readonly ?int $visibilityTimeoutMs,
        private readonly ?string $worker,
        private readonly ?string $from,
        private readonly bool $autoAck,
    ) {
        $this->acks = new PendingAcks();
    }

    /**
     * @param (callable(): bool)|null $shouldStop
     * @return int How many messages were handled.
     */
    public function run(?callable $shouldStop): int
    {
        $handled = 0;
        try {
            while ($shouldStop === null || !$shouldStop()) {
                $handled += $this->handleBatch($this->next());
            }
        } finally {
            $this->sendAcks();
        }

        return $handled;
    }

    /**
     * The next batch, with the waiting acks sent along. Empty after a failure that passes by
     * itself (the broker restarting or out of reach), once it has waited.
     *
     * @return list<Delivery>
     */
    private function next(): array
    {
        $sending = $this->acks->take();
        try {
            $deliveries = $this->client->consume($this->queue, $this->batch, $this->waitMs, $this->consumer, $this->visibilityTimeoutMs, $this->autoAck, $this->from, $this->worker, $sending);
        } catch (BerthqException $e) {
            $this->acks->restore($sending);
            if (!self::isTemporary($e)) {
                throw $e;
            }
            sleep($this->backoff);
            $this->backoff = min($this->backoff * 2, self::MAX_BACKOFF);

            return [];
        }
        $this->backoff = 1;

        return $deliveries;
    }

    /**
     * Handles a batch one delivery after another. The whole batch is leased already, so it is
     * finished even if a stop was requested.
     *
     * @param list<Delivery> $deliveries
     */
    private function handleBatch(array $deliveries): int
    {
        $receivedAt = microtime(true);
        $handled = 0;
        foreach ($deliveries as $delivery) {
            if ($this->acks->due()) {
                $this->sendAcks();
            }
            $leaseEnds = $receivedAt + $delivery->visibilityTimeoutMs / 1000;
            if ($delivery->lease !== null && microtime(true) >= $leaseEnds) {
                // The broker gives it to another worker now; running it here too would do the job twice.
                ($this->onError)(new RuntimeException('its lease ran out before the handler started; the broker delivers it again'), $delivery);

                continue;
            }
            $this->handle($delivery, $receivedAt + $delivery->visibilityTimeoutMs / 2000);
            $handled++;
        }

        return $handled;
    }

    /** Runs the handler for one delivery and settles it: a nack right away, an ack with the next consume. */
    private function handle(Delivery $delivery, float $sendBy): void
    {
        try {
            ($this->handler)($delivery);
        } catch (Throwable $e) {
            if ($delivery->lease === null) {
                // Auto-acked: the message is already done for this consumer, so it can only be reported.
                ($this->onError)($e, $delivery);

                return;
            }
            $this->nack($delivery, $e);

            return;
        }
        if ($delivery->lease !== null) {
            $this->acks->push($delivery, $sendBy);
        }
    }

    private function nack(Delivery $delivery, Throwable $error): void
    {
        try {
            $this->client->nack($this->queue, $delivery, error: $error->getMessage() !== '' ? $error->getMessage() : $error::class);
        } catch (BerthqException $e) {
            if (!self::isTemporary($e)) {
                throw $e;
            }
            // The lease runs out and the message comes again, so this is only reported.
            ($this->onError)($e, $delivery);
        }
    }

    /** Sends the waiting acks on their own. A lease that can't be acked runs out and its message comes again. */
    private function sendAcks(): void
    {
        while (($deliveries = $this->acks->take()) !== []) {
            try {
                $this->client->ack($this->queue, $deliveries);
            } catch (BerthqException $e) {
                foreach ($deliveries as $delivery) {
                    ($this->onError)($e, $delivery);
                }
            }
        }
    }

    /** The broker restarting or shutting down, out of reach, or a proxy answering for it. */
    private static function isTemporary(BerthqException $e): bool
    {
        return $e->kind === 'unavailable' || $e->kind === 'http';
    }
}
