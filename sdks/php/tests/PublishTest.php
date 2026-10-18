<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\BerthqException;
use Berthq\Delivery;
use Berthq\NewMessage;

final class PublishTest extends BrokerTestCase
{
    public function testPlainTextAndDelayedMessages(): void
    {
        $queue = $this->queue();
        $this->client->publish($queue, 'hello');
        $this->client->publish($queue, 'later', delayMs: 60_000);

        $this->assertSame(1, $this->client->queuesGet($queue)->stats['delayed']);
        $deliveries = $this->client->consume($queue, max: 10);
        $this->assertCount(1, $deliveries);
        $this->assertSame('hello', $deliveries[0]->body);
    }

    public function testDuplicatePublishesReturnTheOriginalId(): void
    {
        $queue = $this->queue();
        $first = $this->client->publish($queue, 'a', dedupId: 'order-1');
        $second = $this->client->publish($queue, 'b', dedupId: 'order-1');

        $this->assertFalse($first->duplicate);
        $this->assertTrue($second->duplicate);
        $this->assertSame($first->id, $second->id);
        $this->assertSame(1, $this->client->queuesGet($queue)->stats['messages']);
    }

    public function testBatchPublishKeepsOrderAndAcceptsMessagesAndBodies(): void
    {
        $queue = $this->queue();
        $published = $this->client->publishBatch($queue, [
            'first',
            new NewMessage(['second' => true], priority: 9),
            new NewMessage('third', dedupId: 'x'),
            new NewMessage('third again', dedupId: 'x'),
        ]);

        $this->assertCount(4, $published);
        $this->assertTrue($published[3]->duplicate);
        $this->assertSame($published[2]->id, $published[3]->id);

        $bodies = array_map(fn (Delivery $d) => $d->body, $this->client->consume($queue, max: 10));
        $this->assertSame([['second' => true], 'first', 'third'], $bodies);
    }

    public function testPublisherSendsBatchesAndIsolatesABadMessage(): void
    {
        $queue = $this->queue();
        $publisher = $this->client->publisher($queue, maxBatch: 2);
        $publisher->publish('a');
        $publisher->publish('b');
        $this->assertSame(2, $this->client->queuesGet($queue)->stats['messages'], 'a full batch goes out right away');

        $publisher->publish('bad', priority: 42);
        $publisher->publish('c');
        $results = $publisher->flush();

        $this->assertCount(4, $results);
        $this->assertInstanceOf(BerthqException::class, $results[2]);
        $this->assertSame('invalid', $results[2]->kind);
        $this->assertSame([], $publisher->flush());
        $this->assertSame(['a', 'b', 'c'], $this->drain($queue, 'default'));
    }
}
