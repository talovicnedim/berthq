<?php

declare(strict_types=1);

namespace Berthq;

use DateTimeInterface;
use Throwable;

/**
 * Collects messages and publishes them in batches, many times faster than one request per
 * message. A batch is sent when it is full, before it would pass `maxBytes`, on {@see flush()},
 * and when the publisher is destroyed. Messages keep their order.
 *
 * ```php
 * $publisher = $client->publisher('emails');
 * foreach ($users as $user) {
 *     $publisher->publish(['user' => $user->id]);
 * }
 * $results = $publisher->flush();
 * ```
 */
final class Publisher
{
    /** The broker takes up to 1000 messages per batch. */
    public const MAX_BATCH = 1000;
    public const MAX_BYTES = 4 * 1024 * 1024;

    /** @var list<string> Waiting messages, each encoded once. */
    private array $waiting = [];
    private int $bytes = 0;
    /** @var list<Published|BerthqException> */
    private array $results = [];
    private readonly int $maxBatch;

    public function __construct(
        private readonly Client $client,
        private readonly string $queue,
        int $maxBatch = self::MAX_BATCH,
        private readonly int $maxBytes = self::MAX_BYTES,
    ) {
        $this->maxBatch = max(1, min($maxBatch, self::MAX_BATCH));
    }

    /**
     * Adds a message to the batch, and sends the batch when it is full.
     *
     * @param mixed $body Any JSON-encodable value, or a {@see NewMessage} (then the options are ignored).
     */
    public function publish(
        mixed $body,
        ?int $priority = null,
        ?int $delayMs = null,
        DateTimeInterface|string|null $deliverAt = null,
        ?string $dedupId = null,
    ): void {
        $message = NewMessage::from($body, $priority, $delayMs, $deliverAt, $dedupId);
        $json = Transport::encode($message->toArray());
        if ($this->waiting !== [] && $this->bytes + strlen($json) > $this->maxBytes) {
            $this->send();
        }
        $this->waiting[] = $json;
        $this->bytes += strlen($json);
        if (count($this->waiting) >= $this->maxBatch) {
            $this->send();
        }
    }

    /**
     * Sends what is waiting and returns the result of every message since the last flush, in
     * order. A message the broker rejected on its own (`invalid`) appears as its exception, and
     * so does every message of a batch the broker answered with too few results.
     *
     * @return list<Published|BerthqException>
     * @throws BerthqException When the broker can't be reached or refuses a batch for another
     *                         reason; the messages not sent yet stay waiting for the next flush.
     */
    public function flush(): array
    {
        $this->send();
        $results = $this->results;
        $this->results = [];

        return $results;
    }

    public function __destruct()
    {
        try {
            $this->send();
        } catch (Throwable $e) {
            error_log('berthq: ' . count($this->waiting) . " messages for {$this->queue} were not published: {$e->getMessage()}");
        }
    }

    /**
     * Publishes the waiting messages. One bad message fails a whole batch, so a batch rejected as
     * `invalid` is split in half until the bad messages are alone: only they fail, and the rest
     * still go in order.
     */
    private function send(): void
    {
        $parts = [$this->waiting]; // still to send, the next one last
        while ($parts !== []) {
            $part = array_pop($parts);
            if ($part === []) {
                continue;
            }
            try {
                $published = $this->client->publishEncoded($this->queue, $part);
            } catch (BerthqException $e) {
                if ($e->kind !== 'invalid') {
                    $this->keep([$part, ...array_reverse($parts)]);
                    throw $e;
                }
                if (count($part) === 1) {
                    $this->results[] = $e;
                    continue;
                }
                $half = intdiv(count($part), 2);
                $parts[] = array_slice($part, $half);
                $parts[] = array_slice($part, 0, $half);
                continue;
            }
            if (count($published) !== count($part)) {
                // The broker did answer (201), so this is not a transport error.
                $error = new BerthqException(sprintf('the broker answered %d results for %d messages', count($published), count($part)), 'http', 201);
                array_push($this->results, ...array_fill(0, count($part), $error));
                continue;
            }
            array_push($this->results, ...$published);
        }
        $this->keep([]);
    }

    /** @param list<list<string>> $parts The messages still to send, in order. */
    private function keep(array $parts): void
    {
        $this->waiting = array_merge(...$parts);
        $this->bytes = array_sum(array_map(strlen(...), $this->waiting));
    }
}
