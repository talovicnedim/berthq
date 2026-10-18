<?php

declare(strict_types=1);

namespace Berthq\Laravel;

use Berthq\Client;
use Berthq\Delivery;
use Berthq\Transport;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Throwable;

/**
 * A Laravel job backed by a leased berthq delivery. Settling the job settles the lease:
 * delete acks, release nacks with the delay, and a failed job is dead-lettered (or acked when
 * the connection's `dead_letter` is off) once Laravel has recorded it as failed.
 */
final class BerthqJob extends Job implements JobContract
{
    private ?string $failure = null;

    public function __construct(
        Container $container,
        private readonly Client $client,
        private readonly Delivery $delivery,
        string $connectionName,
        string $queue,
        private readonly bool $deadLetter = true,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    public function getJobId(): string
    {
        return $this->delivery->id;
    }

    public function getRawBody(): string
    {
        // Laravel publishes the payload as a JSON object so the dashboard can show it. A string
        // body is accepted too, for producers that send the payload as an encoded string.
        if (is_string($this->delivery->body)) {
            return $this->delivery->body;
        }

        return Transport::encode($this->delivery->body);
    }

    /** The broker counts deliveries, so the first run of a job is attempt 1, like Laravel's. */
    public function attempts(): int
    {
        return $this->delivery->attempt;
    }

    public function delete(): void
    {
        parent::delete();

        if ($this->hasFailed() && $this->deadLetter) {
            $this->client->nack($this->queue, $this->delivery, error: $this->failure ?? 'job failed', deadLetter: true);

            return;
        }
        $this->client->ack($this->queue, $this->delivery);
    }

    /**
     * @param int $delay Seconds before the job is retried.
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        $this->client->nack($this->queue, $this->delivery, error: $this->failure, delayMs: max(0, (int) $delay) * 1000);
    }

    /**
     * Keeps the reason, so the dead letter shows why the job failed.
     *
     * @param Throwable|null $e
     */
    public function fail($e = null): void
    {
        if ($e !== null) {
            $this->failure = $e::class . ': ' . $e->getMessage();
        }

        parent::fail($e);
    }

    public function getDelivery(): Delivery
    {
        return $this->delivery;
    }
}
