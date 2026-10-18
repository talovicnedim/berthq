<?php

declare(strict_types=1);

namespace Berthq\Tests;

final class QueuesTest extends BrokerTestCase
{
    public function testQueueAdministration(): void
    {
        $queue = $this->queue(['maxAttempts' => 3, 'retentionMs' => 60_000, 'maxMessages' => 100]);
        $settings = $this->client->queuesGet($queue)->settings;
        $this->assertSame(3, $settings['maxAttempts']);
        $this->assertSame(60_000, $settings['retentionMs']);
        $this->assertSame(100, $settings['maxMessages']);
        $updated = $this->client->queuesUpdate($queue, ['maxAttempts' => 7, 'maxMessages' => 0])->settings;
        $this->assertSame(7, $updated['maxAttempts']);
        $this->assertNull($updated['maxMessages'], 'maxMessages 0 removes the limit');
        $this->assertContains($queue, array_map(fn ($q) => $q->name, $this->client->queuesList()));

        $this->assertTrue($this->client->queuesPause($queue)->settings['paused']);
        $this->client->publish($queue, 'held');
        $this->assertSame([], $this->client->consume($queue));
        $this->assertFalse($this->client->queuesResume($queue)->settings['paused']);

        $id = $this->client->publish($queue, 'remove me')->id;
        $this->client->messagesRemove($queue, $id);
        $this->assertSame(1, $this->client->queuesPurge($queue));

        $this->client->queuesRemove($queue);
        $this->expectBerthqError('notFound', 404, fn () => $this->client->queuesGet($queue));
    }

    public function testMessagesAreBrowsedNewestFirst(): void
    {
        $queue = $this->queue();
        $first = $this->client->publish($queue, 'first');
        $second = $this->client->publish($queue, 'second');
        $this->client->publish($queue, 'later', delayMs: 60_000);

        $newest = $this->client->messagesList($queue, limit: 1);
        $this->assertSame($second->id, $newest[0]->id);
        $this->assertSame('visible', $newest[0]->state);
        $this->assertSame([$first->id], array_map(fn ($m) => $m->id, $this->client->messagesList($queue, before: $second->id)));
        $this->assertSame('delayed', $this->client->messagesList($queue, 'delayed')[0]->state);
    }
}
