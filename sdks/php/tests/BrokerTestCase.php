<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\BerthqException;
use Berthq\Client;
use Berthq\Delivery;
use PHPUnit\Framework\TestCase;

/**
 * Tests against the shared open broker, each with queues of its own.
 */
abstract class BrokerTestCase extends TestCase
{
    protected Client $client;

    protected function setUp(): void
    {
        $this->client = new Client(BerthqServer::open()->url);
    }

    /** Creates a queue with a unique name, so tests don't see each other's messages. */
    protected function queue(array $settings = []): string
    {
        $name = 'php-' . bin2hex(random_bytes(6));
        $this->client->queuesCreate($name, $settings);

        return $name;
    }

    /**
     * Consumes and acks everything a consumer has right now.
     *
     * @return list<mixed> The bodies, in order.
     */
    protected function drain(string $queue, string $consumer): array
    {
        $deliveries = $this->client->consume($queue, max: 100, consumer: $consumer);
        if ($deliveries !== []) {
            $this->client->ack($queue, $deliveries);
        }

        return array_map(fn (Delivery $d) => $d->body, $deliveries);
    }

    protected function expectBerthqError(string $kind, int $status, callable $call): void
    {
        try {
            $call();
        } catch (BerthqException $e) {
            $this->assertSame($kind, $e->kind, $e->getMessage());
            $this->assertSame($status, $e->status);

            return;
        }
        $this->fail("expected a {$kind} error");
    }
}
