<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\BerthqException;
use Berthq\Client;

final class ClientTest extends BrokerTestCase
{
    public function testErrorsCarryKindAndStatus(): void
    {
        $queue = $this->queue();
        $this->expectBerthqError('conflict', 409, fn () => $this->client->queuesCreate($queue));
        $this->expectBerthqError('invalid', 400, fn () => $this->client->publish($queue, 'x', priority: 42));
        $this->expectBerthqError('notFound', 404, fn () => $this->client->publish('nope-' . bin2hex(random_bytes(4)), 'x'));
        $this->expectBerthqError('notFound', 404, fn () => $this->client->consumersReplay($queue, 'nobody', 'start'));
    }

    public function testUnreachableBrokerIsATransportError(): void
    {
        $client = new Client('http://127.0.0.1:1', timeout: 2);
        try {
            $client->stats();
            $this->fail('expected an exception');
        } catch (BerthqException $e) {
            $this->assertSame('http', $e->kind);
            $this->assertTrue($e->isTransport());
            $this->assertStringContainsString('could not connect', $e->getMessage());
        }
    }

    public function testStatsShowTheFreeDiskSpace(): void
    {
        $disk = $this->client->stats()->disk;

        $this->assertGreaterThan(0, $disk['free']);
        $this->assertFalse($disk['full']);
    }

    public function testApiKeysAndScopes(): void
    {
        $url = BerthqServer::secured()->url;
        $admin = new Client($url, apiKey: 'adminadminadmin1');
        $publisher = new Client($url, apiKey: 'publishpublish12');
        $anonymous = new Client($url);
        $wrong = new Client($url, apiKey: 'not-a-real-key-123');

        $queue = 'auth-' . bin2hex(random_bytes(4));
        $admin->queuesCreate($queue);
        $this->assertFalse($publisher->publish($queue, 'hi')->duplicate);

        $this->expectBerthqError('unauthorized', 401, fn () => $anonymous->queuesList());
        $this->expectBerthqError('unauthorized', 401, fn () => $wrong->queuesList());
        $this->expectBerthqError('forbidden', 403, fn () => $publisher->consume($queue));
        $this->expectBerthqError('forbidden', 403, fn () => $publisher->queuesList());
        $this->expectBerthqError('forbidden', 403, fn () => $publisher->consumersCreate($queue, 'billing'));
        $this->assertCount(1, $admin->consume($queue));
        $this->assertGreaterThanOrEqual(1, $admin->stats()->queues);
    }
}
