<?php

declare(strict_types=1);

namespace Berthq\Laravel;

use Berthq\BerthqException;
use Berthq\Client;
use Berthq\NewMessage;
use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Illuminate\Contracts\Queue\ClearableQueue;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;

/**
 * Laravel queue connection on top of a Berthq queue. Job payloads are published as JSON
 * objects, so the dashboard shows them; jobs are leased with a short long-poll so
 * `queue:work` reacts quickly to new jobs and to signals. All workers of a connection share
 * one consumer, so another app can read the same queue with a consumer of its own.
 */
final class BerthqQueue extends Queue implements QueueContract, ClearableQueue
{
    private const BATCH_LIMIT = 1000;

    /**
     * @param int $waitSeconds How long `pop` long-polls for a job (0-60).
     * @param int|null $retryAfter Lease length in seconds; null uses the broker queue's
     *                             visibility timeout. Keep it above your jobs' `--timeout`.
     * @param string $consumer The consumer whose position the workers share.
     * @param string|null $worker Worker name shown in the dashboard; null is `host:pid`.
     * @param bool $createQueues Create a missing berthq queue on first use (false: fail).
     * @param bool $deadLetter Keep failed jobs in `<queue>.dlq` (true) or delete them (false).
     */
    public function __construct(
        private readonly Client $client,
        private readonly string $default = 'default',
        private readonly int $waitSeconds = 3,
        private readonly ?int $retryAfter = null,
        private readonly string $consumer = 'default',
        private readonly ?string $worker = null,
        private readonly bool $createQueues = true,
        private readonly bool $deadLetter = true,
        ?bool $dispatchAfterCommit = false,
    ) {
        $this->dispatchAfterCommit = $dispatchAfterCommit;
    }

    /** Every job that isn't done yet: pending, delayed (including released) and reserved. */
    public function size($queue = null): int
    {
        return array_sum($this->counts($queue));
    }

    public function pendingSize($queue = null): int
    {
        return $this->counts($queue)['pending'];
    }

    /** Delayed jobs plus released ones waiting for their next attempt. */
    public function delayedSize($queue = null): int
    {
        return $this->counts($queue)['delayed'];
    }

    public function reservedSize($queue = null): int
    {
        return $this->counts($queue)['reserved'];
    }

    /**
     * Unix timestamp of the next job waiting to run, or null when none is (or no worker has
     * created the connection's consumer yet).
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        try {
            $next = $this->client->consumersMessages($this->getQueue($queue), $this->consumer, 'waiting', 1)[0] ?? null;
        } catch (BerthqException $e) {
            if ($e->kind !== 'notFound') {
                throw $e;
            }

            return null;
        }

        return $next === null ? null : (new DateTimeImmutable($next->publishedAt))->getTimestamp();
    }

    public function push($job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            fn (string $payload, ?string $queue) => $this->pushRaw($payload, $queue),
        );
    }

    /**
     * @param array{delay?: DateTimeInterface|DateInterval|int} $options
     * @return string The broker's message id.
     */
    public function pushRaw($payload, $queue = null, array $options = []): string
    {
        $message = $this->message($payload, $options['delay'] ?? null);

        return $this->publish($this->getQueue($queue), fn (string $name) => $this->client->publish($name, $message)->id);
    }

    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data, $delay),
            $queue,
            $delay,
            fn (string $payload, ?string $queue, $delay) => $this->pushRaw($payload, $queue, ['delay' => $delay]),
        );
    }

    /**
     * Publishes the jobs in atomic batches of up to 1000. Like the database driver, this skips
     * the per-job queueing events.
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        $name = $this->getQueue($queue);
        $messages = [];
        foreach ((array) $jobs as $job) {
            $delay = is_object($job) && isset($job->delay) ? $job->delay : null;
            $messages[] = $this->message($this->createPayload($job, $name, $data, $delay), $delay);
        }

        foreach (array_chunk($messages, self::BATCH_LIMIT) as $chunk) {
            $this->publish($name, fn (string $name) => $this->client->publishBatch($name, $chunk));
        }
    }

    public function pop($queue = null): ?JobContract
    {
        $name = $this->getQueue($queue);
        try {
            $deliveries = $this->client->consume(
                $name,
                max: 1,
                waitMs: max(0, min($this->waitSeconds, 60)) * 1000,
                consumer: $this->consumer,
                visibilityTimeoutMs: $this->retryAfter === null ? null : $this->retryAfter * 1000,
                worker: $this->workerName(),
            );
        } catch (BerthqException $e) {
            if ($e->kind !== 'notFound' || !$this->createQueues) {
                throw $e;
            }
            $this->create($name);

            return null;
        }

        if ($deliveries === []) {
            return null;
        }

        return new BerthqJob($this->container, $this->client, $deliveries[0], $this->connectionName, $name, $this->deadLetter);
    }

    /** Deletes every job, including reserved ones; used by `php artisan queue:clear`. */
    public function clear($queue): int
    {
        return $this->client->queuesPurge($this->getQueue($queue));
    }

    public function getQueue(?string $queue): string
    {
        return $queue ?: $this->default;
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * The connection consumer's jobs. Acked jobs stay in the queue for replay, so the queue's
     * own message count isn't the job count. Before a worker has created the consumer, every
     * visible message counts as pending.
     *
     * @return array{pending: int, delayed: int, reserved: int}
     */
    private function counts(?string $queue): array
    {
        $details = $this->client->queuesGet($this->getQueue($queue));
        foreach ($details->consumers as $consumer) {
            if ($consumer->name === $this->consumer) {
                return [
                    'pending' => $consumer->stats['waiting'],
                    'delayed' => $details->stats['delayed'] + $consumer->stats['retrying'],
                    'reserved' => $consumer->stats['processing'],
                ];
            }
        }

        return ['pending' => $details->stats['backlog'], 'delayed' => $details->stats['delayed'], 'reserved' => 0];
    }

    private function message(string $payload, DateTimeInterface|DateInterval|int|null $delay): NewMessage
    {
        // Published as an object so the dashboard can show the job; BerthqJob encodes it back.
        $body = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        $delayMs = $delay === null ? null : self::delayMs($delay);

        return new NewMessage($body, delayMs: $delayMs ?: null);
    }

    /**
     * Runs a publish, creating the queue and trying once more when it doesn't exist and
     * `create_queues` is on.
     *
     * @template T
     * @param callable(string): T $publish
     * @return T
     */
    private function publish(string $queue, callable $publish): mixed
    {
        try {
            return $publish($queue);
        } catch (BerthqException $e) {
            if ($e->kind !== 'notFound' || !$this->createQueues) {
                throw $e;
            }
        }
        $this->create($queue);

        return $publish($queue);
    }

    /**
     * Laravel decides about retries (tries, backoff, retryUntil), so the broker should retry
     * forever and only dead-letter what Laravel marks as failed.
     */
    private function create(string $queue): void
    {
        try {
            $this->client->queuesCreate($queue, ['maxAttempts' => 0]);
        } catch (BerthqException $e) {
            // Another worker or process created it first.
            if ($e->kind !== 'conflict') {
                throw $e;
            }
        }
    }

    private function workerName(): string
    {
        $name = $this->worker ?? (gethostname() ?: 'laravel') . ':' . getmypid();

        return substr($name, 0, 128);
    }

    /** Milliseconds rather than Laravel's seconds, so a DateTime delay keeps its precision. */
    private static function delayMs(DateTimeInterface|DateInterval|int $delay): int
    {
        if (is_int($delay)) {
            return max(0, $delay * 1000);
        }
        $now = new DateTimeImmutable();
        $until = $delay instanceof DateInterval ? $now->add($delay) : $delay;
        $ms = (int) round(((float) $until->format('U.u') - (float) $now->format('U.u')) * 1000);

        return max(0, $ms);
    }
}
