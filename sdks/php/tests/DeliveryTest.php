<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\Delivery;

final class DeliveryTest extends BrokerTestCase
{
    public function testPublishConsumeAndAckRoundTrip(): void
    {
        $queue = $this->queue();
        $published = $this->client->publish($queue, ['user' => 123, 'name' => 'Zoë/ä'], priority: 5, dedupId: 'user-123');
        $this->assertFalse($published->duplicate);

        $deliveries = $this->client->consume($queue, max: 10, waitMs: 1000, worker: 'php-test');
        $this->assertCount(1, $deliveries);
        $delivery = $deliveries[0];
        $this->assertSame($published->id, $delivery->id);
        // The broker sorts object keys, so compare without order.
        $this->assertEquals(['user' => 123, 'name' => 'Zoë/ä'], $delivery->body);
        $this->assertSame(5, $delivery->priority);
        $this->assertSame(1, $delivery->attempt);
        $this->assertSame('user-123', $delivery->dedupId);
        $this->assertSame($queue, $delivery->queue);
        $this->assertSame('default', $delivery->consumer);
        $this->assertNotNull($delivery->lease);

        $settled = $this->client->ack($queue, $delivery);
        $this->assertSame(1, $settled->settled);
        $this->assertSame([], $settled->unknown);

        // Settling twice is not an error: the lease is reported as unknown.
        $this->assertSame([$delivery->lease], $this->client->ack($queue, (string) $delivery->lease)->unknown);
        $stats = $this->client->queuesGet($queue)->stats;
        $this->assertSame(0, $stats['backlog']);
        // Acked messages stay for the queue's retention, so a replay can bring them back.
        $this->assertSame(1, $stats['messages']);
    }

    public function testAutoAckMovesThePositionWithoutALease(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch($queue, ['a', 'b']);

        $deliveries = $this->client->consume($queue, max: 10, autoAck: true);
        $this->assertSame([['a', null, 0], ['b', null, 0]], array_map(fn (Delivery $d) => [$d->body, $d->lease, $d->visibilityTimeoutMs], $deliveries));
        $this->assertSame([], $this->client->consume($queue));
        $this->assertSame(['waiting' => 0, 'processing' => 0, 'retrying' => 0], $this->client->consumersGet($queue, 'default')->stats);
        $this->expectBerthqError('invalid', 0, fn () => $this->client->ack($queue, $deliveries[0]));
    }

    public function testNackedMessagesAreRetriedThenDeadLetteredAndRequeued(): void
    {
        $queue = $this->queue(['maxAttempts' => 2]);
        $id = $this->client->publish($queue, ['job' => 1])->id;

        $first = $this->client->consume($queue)[0];
        $this->client->nack($queue, $first, error: 'smtp timeout', delayMs: 0);

        $second = $this->client->consume($queue, waitMs: 1000)[0];
        $this->assertSame(2, $second->attempt);
        $this->assertSame('smtp timeout', $second->lastError);
        $this->client->nack($queue, $second, error: 'smtp timeout again');

        // A dead letter is a copy, so it is looked up in the DLQ rather than by the original id.
        $dlq = $queue . '.dlq';
        $dead = $this->client->messagesList($dlq);
        $this->assertCount(1, $dead);
        $this->assertSame('maxAttempts', $dead[0]->deadLetter['reason']);
        $this->assertSame('default', $dead[0]->deadLetter['consumer']);
        $this->assertSame($id, $dead[0]->deadLetter['messageId']);
        $this->assertSame(2, $dead[0]->attempts);
        $this->assertSame('smtp timeout again', $dead[0]->lastError);
        $this->assertSame(['job' => 1], $this->client->messagesGet($dlq, $dead[0]->id)->body);

        $requeued = $this->client->requeue($dlq);
        $this->assertSame(1, $requeued->requeued);
        $again = $this->client->consume($queue, waitMs: 1000)[0];
        $this->assertSame($id, $again->id);
        $this->assertSame(1, $again->attempt);
        $this->assertSame(['job' => 1], $again->body);
    }

    public function testNackWithDeadLetterSkipsRemainingAttempts(): void
    {
        $queue = $this->queue();
        $this->client->publish($queue, 'poison');
        $this->client->nack($queue, $this->client->consume($queue), error: 'bad input', deadLetter: true);

        $dead = $this->client->messagesList($queue . '.dlq');
        $this->assertSame('rejected', $dead[0]->deadLetter['reason']);
        $this->assertSame(1, $this->client->requeue($queue . '.dlq', [$dead[0]->id])->requeued);
    }

    public function testExtendRestartsTheLease(): void
    {
        $queue = $this->queue();
        $this->client->publish($queue, 'slow');
        $delivery = $this->client->consume($queue, visibilityTimeoutMs: 1000, worker: 'w1')[0];
        $this->assertSame(1000, $delivery->visibilityTimeoutMs);

        $this->assertSame(1, $this->client->extend($queue, $delivery, 60_000)->settled);
        $processing = $this->client->consumersMessages($queue, 'default', 'processing');
        $this->assertSame('w1', $processing[0]->worker);
        $this->assertGreaterThan($delivery->leaseExpiresAt, $processing[0]->leaseExpiresAt);
    }

    public function testConsumeAcksTheLeasesItIsGiven(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch($queue, ['first', 'second']);
        $first = $this->client->consume($queue)[0];

        $next = $this->client->consume($queue, ack: $first);

        $this->assertSame(['second'], array_map(fn (Delivery $d) => $d->body, $next));
        $this->assertSame(1, $this->client->queuesGet($queue)->stats['counters']['acked']);
    }
}
