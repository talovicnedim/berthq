<?php

declare(strict_types=1);

namespace Berthq;

use DateTimeInterface;
use stdClass;
use Throwable;

/**
 * PHP client for the Berthq HTTP API (see docs/api.md). Method names match the API and the
 * other SDKs: `queuesCreate`, `publish`, `consume`, `ack`, `nack`, `consumersReplay`, ...
 *
 * ```php
 * $client = new Client('http://127.0.0.1:7331');
 * $client->publish('emails', ['user' => 123]);
 *
 * foreach ($client->consume('emails', waitMs: 20_000) as $delivery) {
 *     // ... send the email ...
 *     $client->ack('emails', $delivery);
 * }
 * ```
 */
final class Client
{
    private readonly string $url;
    private readonly Transport $transport;

    /**
     * @param string $url The broker, e.g. `http://127.0.0.1:7331`.
     * @param string|null $apiKey Sent as `Authorization: Bearer <key>` when the broker requires keys.
     * @param float $timeout Seconds a request may take. Long-polling consumes get their wait on top.
     */
    public function __construct(
        string $url = 'http://127.0.0.1:7331',
        ?string $apiKey = null,
        float $timeout = 30,
    ) {
        $this->url = rtrim($url, '/');
        $this->transport = new Transport($this->url, $apiKey, $timeout);
    }

    public function url(): string
    {
        return $this->url;
    }

    // Queues

    /** @return list<QueueInfo> */
    public function queuesList(): array
    {
        return array_map(QueueInfo::fromArray(...), $this->transport->request('GET', '/v1/queues')['queues']);
    }

    /** @param array<string, mixed> $settings API settings, e.g. `['maxAttempts' => 3]`. */
    public function queuesCreate(string $name, array $settings = []): QueueInfo
    {
        return QueueInfo::fromArray($this->transport->request('POST', '/v1/queues', ['name' => $name] + $settings));
    }

    /** The queue with its consumers and the last minute of throughput. */
    public function queuesGet(string $queue): QueueDetails
    {
        return QueueDetails::fromArray($this->transport->request('GET', Path::queue($queue)));
    }

    /** @param array<string, mixed> $settings Settings to change. `maxMessages` of 0 removes the limit. */
    public function queuesUpdate(string $queue, array $settings): QueueInfo
    {
        return QueueInfo::fromArray($this->transport->request('PATCH', Path::queue($queue), (object) $settings));
    }

    /** Deletes the queue, its consumers and its messages. Its dead-letter queue stays. */
    public function queuesRemove(string $queue): void
    {
        $this->transport->request('DELETE', Path::queue($queue));
    }

    /** Deletes every message, including ones being processed; returns how many. */
    public function queuesPurge(string $queue): int
    {
        return (int) $this->transport->request('POST', Path::queue($queue, '/purge'))['purged'];
    }

    /** A paused queue accepts publishes but delivers nothing to any consumer. */
    public function queuesPause(string $queue): QueueInfo
    {
        return QueueInfo::fromArray($this->transport->request('POST', Path::queue($queue, '/pause')));
    }

    public function queuesResume(string $queue): QueueInfo
    {
        return QueueInfo::fromArray($this->transport->request('POST', Path::queue($queue, '/resume')));
    }

    // Publishing

    /**
     * Publishes one message. Returns once it is on disk.
     *
     * @param mixed $body Any JSON-encodable value, or a {@see NewMessage} (then the options are ignored).
     */
    public function publish(
        string $queue,
        mixed $body,
        ?int $priority = null,
        ?int $delayMs = null,
        DateTimeInterface|string|null $deliverAt = null,
        ?string $dedupId = null,
    ): Published {
        $message = NewMessage::from($body, $priority, $delayMs, $deliverAt, $dedupId);

        return Published::fromArray($this->transport->request('POST', Path::queue($queue, '/messages'), $message->toArray()));
    }

    /**
     * Publishes up to 1000 messages atomically: all are stored or none.
     *
     * @param list<NewMessage|mixed> $messages Messages, or plain bodies.
     * @return list<Published> In the same order.
     */
    public function publishBatch(string $queue, array $messages): array
    {
        $body = ['messages' => array_map(fn (mixed $message) => NewMessage::from($message)->toArray(), array_values($messages))];

        return array_map(Published::fromArray(...), $this->transport->request('POST', Path::queue($queue, '/batch'), $body)['messages']);
    }

    /**
     * Publishes messages that are already JSON as one atomic batch.
     *
     * @internal Used by {@see Publisher}.
     * @param list<string> $messages
     * @return list<Published>
     */
    public function publishEncoded(string $queue, array $messages): array
    {
        $json = '{"messages":[' . implode(',', $messages) . ']}';

        return array_map(Published::fromArray(...), $this->transport->requestJson('POST', Path::queue($queue, '/batch'), $json)['messages']);
    }

    /**
     * A publisher for `queue`: collects messages and publishes them in batches.
     *
     * @param int $maxBatch Most messages per batch (1-1000).
     * @param int $maxBytes A batch is sent before its JSON passes this; a bigger message goes alone.
     */
    public function publisher(string $queue, int $maxBatch = Publisher::MAX_BATCH, int $maxBytes = Publisher::MAX_BYTES): Publisher
    {
        return new Publisher($this, $queue, $maxBatch, $maxBytes);
    }

    // Consuming

    /**
     * Gets up to `max` messages (1-100) for a consumer, which is created if it doesn't exist. With
     * `waitMs` (at most 60000), waits up to that long for one to arrive.
     *
     * @param string|null $consumer Whose position to use; null is `default`.
     * @param int|null $visibilityTimeoutMs Lease length; defaults to the queue's setting.
     * @param bool $autoAck No lease: the position moves right away and nothing is retried.
     * @param string|null $from Where a new consumer starts: `start` (default), `end`, an RFC 3339
     *                          time or a message id. Ignored if the consumer exists.
     * @param string|null $worker Name shown in the dashboard.
     * @param Delivery|string|list<Delivery|string> $ack Deliveries or leases to ack first (up to
     *                                                   1000), instead of a separate `ack` call.
     * @return list<Delivery> Possibly empty.
     */
    public function consume(
        string $queue,
        int $max = 1,
        int $waitMs = 0,
        ?string $consumer = null,
        ?int $visibilityTimeoutMs = null,
        bool $autoAck = false,
        ?string $from = null,
        ?string $worker = null,
        Delivery|string|array $ack = [],
    ): array {
        $body = array_filter(
            [
                'consumer' => $consumer,
                'from' => $from,
                'max' => $max,
                'waitMs' => $waitMs,
                'visibilityTimeoutMs' => $visibilityTimeoutMs,
                'autoAck' => $autoAck ?: null,
                'worker' => $worker,
                'ack' => Delivery::leasesOf($ack) ?: null,
            ],
            fn (mixed $value) => $value !== null,
        );
        // The request legitimately takes as long as the wait, so that comes on top of the timeout.
        $response = $this->transport->request('POST', Path::queue($queue, '/consume'), $body, $waitMs / 1000);

        return array_map(Delivery::fromArray(...), $response['messages']);
    }

    /**
     * Marks deliveries as done for their consumer.
     *
     * @param Delivery|string|list<Delivery|string> $leases Deliveries or their lease ids.
     */
    public function ack(string $queue, Delivery|string|array $leases): Settled
    {
        return $this->settle($queue, '/ack', ['leases' => Delivery::leasesOf($leases)]);
    }

    /**
     * Marks deliveries as failed. They are retried after `delayMs` (or the queue's backoff), or
     * dead-lettered when out of attempts or when `deadLetter` is true.
     *
     * @param Delivery|string|list<Delivery|string> $leases
     * @param string|null $error Kept on the message and shown in the dashboard.
     */
    public function nack(
        string $queue,
        Delivery|string|array $leases,
        ?string $error = null,
        ?int $delayMs = null,
        bool $deadLetter = false,
    ): Settled {
        $body = array_filter(
            [
                'leases' => Delivery::leasesOf($leases),
                'error' => $error,
                'delayMs' => $delayMs === null ? null : max(0, $delayMs),
                'deadLetter' => $deadLetter ?: null,
            ],
            fn (mixed $value) => $value !== null,
        );

        return $this->settle($queue, '/nack', $body);
    }

    /**
     * Restarts the lease clock of deliveries that take long to process.
     *
     * @param Delivery|string|list<Delivery|string> $leases
     * @param int|null $visibilityTimeoutMs Defaults to the queue's visibility timeout.
     */
    public function extend(string $queue, Delivery|string|array $leases, ?int $visibilityTimeoutMs = null): Settled
    {
        $body = ['leases' => Delivery::leasesOf($leases)];
        if ($visibilityTimeoutMs !== null) {
            $body['visibilityTimeoutMs'] = $visibilityTimeoutMs;
        }

        return $this->settle($queue, '/extend', $body);
    }

    /**
     * Runs a worker loop: leases up to `batch` messages at a time and calls `handler` for each.
     * A handler that returns acks the message; one that throws nacks it with the exception's
     * message, so the broker retries it with backoff or dead-letters it. Acks travel with the
     * next consume call, and the rest are sent when the loop ends, also when it ends with an
     * exception. A delivery whose lease ran out while it waited for its turn is skipped: the
     * broker gives it to another worker. While the broker is shutting down or unreachable, the
     * loop waits and tries again, from 1 up to 30 seconds; other errors (a missing queue, a bad
     * key) are thrown.
     *
     * @param callable(Delivery): mixed $handler
     * @param (callable(): bool)|null $shouldStop Checked before every consume; return true to stop,
     *                                            e.g. from a SIGTERM handler. Null runs forever.
     * @param string|null $from Where the consumer starts if it doesn't exist yet (default `start`).
     * @param bool $autoAck Nothing is leased, acked or retried: faster, but a crash or a throwing
     *                      handler loses the message for this consumer.
     * @param (callable(Throwable, Delivery): mixed)|null $onError Called with what the loop
     *                                                     recovers from: an ack or nack the broker
     *                                                     could not take, a skipped delivery, and
     *                                                     a handler error in `autoAck` mode.
     *                                                     Defaults to error_log().
     * @return int How many messages were handled (acked, nacked or auto-acked).
     */
    public function process(
        string $queue,
        callable $handler,
        int $batch = 1,
        int $waitMs = 20_000,
        ?callable $shouldStop = null,
        ?string $consumer = null,
        ?int $visibilityTimeoutMs = null,
        ?string $worker = null,
        ?string $from = null,
        bool $autoAck = false,
        ?callable $onError = null,
    ): int {
        $onError ??= static fn (Throwable $e, Delivery $delivery) => error_log("berthq: message {$delivery->id}: {$e->getMessage()}");
        $loop = new Worker($this, $queue, $handler(...), $onError(...), $batch, $waitMs, $consumer, $visibilityTimeoutMs, $worker, $from, $autoAck);

        return $loop->run($shouldStop);
    }

    // Inspecting

    /**
     * Browses stored messages, newest first, without consuming them and without bodies.
     *
     * @param string|null $state `visible` (default) or `delayed`.
     * @param int|null $limit Up to 500 (default 50).
     * @param string|null $before A message id: list messages older than it.
     * @return list<MessageInfo>
     */
    public function messagesList(string $queue, ?string $state = null, ?int $limit = null, ?string $before = null): array
    {
        $path = Path::queue($queue, '/messages') . Path::query(['state' => $state, 'limit' => $limit, 'before' => $before]);

        return array_map(MessageInfo::fromArray(...), $this->transport->request('GET', $path)['messages']);
    }

    /** One message including its body. */
    public function messagesGet(string $queue, string $id): MessageInfo
    {
        return MessageInfo::fromArray($this->transport->request('GET', Path::queue($queue, '/messages/' . rawurlencode($id))));
    }

    /** Deletes the message for every consumer. */
    public function messagesRemove(string $queue, string $id): void
    {
        $this->transport->request('DELETE', Path::queue($queue, '/messages/' . rawurlencode($id)));
    }

    /**
     * Moves dead letters back to the queue and consumer that failed them, with attempts reset.
     * Call it on a dead-letter queue (`emails.dlq`).
     *
     * @param list<string>|null $ids Message ids, or null for all of them.
     */
    public function requeue(string $queue, ?array $ids = null): Requeued
    {
        $body = $ids === null ? new stdClass() : ['ids' => array_values($ids)];

        return Requeued::fromArray($this->transport->request('POST', Path::queue($queue, '/requeue'), $body));
    }

    // Consumers

    /** @return list<ConsumerInfo> Sorted by name. */
    public function consumersList(string $queue): array
    {
        return array_map(ConsumerInfo::fromArray(...), $this->transport->request('GET', Path::queue($queue, '/consumers'))['consumers']);
    }

    public function consumersGet(string $queue, string $consumer): ConsumerInfo
    {
        return ConsumerInfo::fromArray($this->transport->request('GET', Path::consumer($queue, $consumer)));
    }

    /**
     * Creates a consumer. Throws `conflict` if it exists; `consume` also creates consumers on
     * first use.
     *
     * @param string|null $from `start` (default), `end`, an RFC 3339 time or a message id.
     */
    public function consumersCreate(string $queue, string $name, ?string $from = null): ConsumerInfo
    {
        $body = array_filter(['name' => $name, 'from' => $from], fn (mixed $value) => $value !== null);

        return ConsumerInfo::fromArray($this->transport->request('POST', Path::queue($queue, '/consumers'), $body));
    }

    /** Removes the consumer with its position, leases and retries. */
    public function consumersRemove(string $queue, string $consumer): void
    {
        $this->transport->request('DELETE', Path::consumer($queue, $consumer));
    }

    /**
     * Moves the consumer's position. Drops what it was processing and retrying.
     *
     * @param string $from `start`, `end`, an RFC 3339 time or a message id.
     */
    public function consumersReplay(string $queue, string $consumer, string $from): ConsumerInfo
    {
        return ConsumerInfo::fromArray($this->transport->request('POST', Path::consumer($queue, $consumer, '/replay'), ['from' => $from]));
    }

    /** A paused consumer gets nothing; other consumers continue. */
    public function consumersPause(string $queue, string $consumer): ConsumerInfo
    {
        return ConsumerInfo::fromArray($this->transport->request('POST', Path::consumer($queue, $consumer, '/pause')));
    }

    public function consumersResume(string $queue, string $consumer): ConsumerInfo
    {
        return ConsumerInfo::fromArray($this->transport->request('POST', Path::consumer($queue, $consumer, '/resume')));
    }

    /**
     * What the consumer is doing, without bodies.
     *
     * @param string|null $state `processing` (default), `retrying` (every retry) or `waiting`
     *                           (not received yet).
     * @param int|null $limit Up to 500 (default 50).
     * @return list<ConsumerMessage>
     */
    public function consumersMessages(string $queue, string $consumer, ?string $state = null, ?int $limit = null): array
    {
        $path = Path::consumer($queue, $consumer, '/messages') . Path::query(['state' => $state, 'limit' => $limit]);

        return array_map(ConsumerMessage::fromArray(...), $this->transport->request('GET', $path)['messages']);
    }

    // Monitoring

    /** Broker-wide totals, rates, free disk space and the last minute of throughput. */
    public function stats(): Overview
    {
        return Overview::fromArray($this->transport->request('GET', '/v1/stats'));
    }

    /** @param array<string, mixed> $body */
    private function settle(string $queue, string $action, array $body): Settled
    {
        return Settled::fromArray($this->transport->request('POST', Path::queue($queue, $action), $body));
    }
}
