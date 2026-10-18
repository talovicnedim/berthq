<?php

declare(strict_types=1);

namespace Berthq\Tests;

final class ConsumersTest extends BrokerTestCase
{
    public function testEveryConsumerGetsEveryMessageAndCanReplay(): void
    {
        $queue = $this->queue();
        $first = $this->client->publish($queue, 'first');
        $this->client->publish($queue, 'second');

        $this->assertSame(['first', 'second'], $this->drain($queue, 'billing'));
        $this->assertSame(['first', 'second'], $this->drain($queue, 'audit'));
        $this->assertSame([], $this->drain($queue, 'billing'));
        $this->assertSame(['audit', 'billing'], array_map(fn ($c) => $c->name, $this->client->consumersList($queue)));
        $this->assertCount(2, $this->client->queuesGet($queue)->consumers);

        $this->assertSame([], $this->client->consume($queue, consumer: 'late', from: 'end'));
        $this->assertSame(2, $this->client->consumersReplay($queue, 'late', 'start')->stats['waiting']);
        $this->assertSame(['first', 'second'], $this->drain($queue, 'late'));
        $this->client->consumersReplay($queue, 'late', $first->id);
        $this->assertSame(['first', 'second'], $this->drain($queue, 'late'));
    }

    public function testConsumerAdministration(): void
    {
        $queue = $this->queue();
        $created = $this->client->consumersCreate($queue, 'billing', 'end');
        $this->assertSame([$queue, 'billing', false, false, null], [$created->queue, $created->name, $created->paused, $created->idle, $created->lastSeen]);
        $this->expectBerthqError('conflict', 409, fn () => $this->client->consumersCreate($queue, 'billing'));

        $this->assertTrue($this->client->consumersPause($queue, 'billing')->paused);
        $this->client->publish($queue, 'held');
        $this->assertSame([], $this->client->consume($queue, consumer: 'billing'));
        $this->assertFalse($this->client->consumersResume($queue, 'billing')->paused);
        $delivery = $this->client->consume($queue, consumer: 'billing', worker: 'b1')[0];
        $this->assertSame(['held', 'billing'], [$delivery->body, $delivery->consumer]);

        $seen = $this->client->consumersGet($queue, 'billing');
        $this->assertNotNull($seen->lastSeen);
        $this->assertContains('b1', array_column($seen->workers, 'name'));

        $this->client->nack($queue, $delivery, error: 'later', delayMs: 60_000);
        $retrying = $this->client->consumersMessages($queue, 'billing', 'retrying');
        $this->assertSame('later', $retrying[0]->lastError);
        $this->assertNotNull($retrying[0]->retryAt);
        $this->assertSame(['waiting' => 0, 'processing' => 0, 'retrying' => 1], $this->client->consumersGet($queue, 'billing')->stats);

        $this->client->consumersRemove($queue, 'billing');
        $this->expectBerthqError('notFound', 404, fn () => $this->client->consumersGet($queue, 'billing'));
    }
}
