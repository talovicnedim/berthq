<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\BerthqException;
use Berthq\Client;
use Berthq\Delivery;
use RuntimeException;
use Throwable;

final class WorkerTest extends BrokerTestCase
{
    public function testProcessAcksSuccessesAndNacksFailures(): void
    {
        $queue = $this->queue(['maxAttempts' => 1]);
        $this->client->publishBatch($queue, ['ok-1', 'boom', 'ok-2']);

        $seen = [];
        $handled = $this->client->process(
            $queue,
            function (Delivery $delivery) use (&$seen): void {
                $seen[] = $delivery->body;
                if ($delivery->body === 'boom') {
                    throw new RuntimeException('it exploded');
                }
            },
            batch: 10,
            waitMs: 200,
            // Not an arrow function: those capture $seen by value, so it would never change.
            shouldStop: function () use (&$seen): bool {
                return count($seen) >= 3;
            },
        );

        $this->assertSame(3, $handled);
        $this->assertSame(['ok-1', 'boom', 'ok-2'], $seen);
        $this->assertSame(0, $this->client->queuesGet($queue)->stats['backlog']);
        $dead = $this->client->messagesList($queue . '.dlq');
        $this->assertSame('it exploded', $dead[0]->lastError);
    }

    public function testProcessWithAutoAckReportsHandlerErrors(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch($queue, ['ok', 'boom']);

        $errors = [];
        $handled = $this->client->process(
            $queue,
            function (Delivery $delivery): void {
                if ($delivery->body === 'boom') {
                    throw new RuntimeException('it exploded');
                }
            },
            batch: 10,
            waitMs: 200,
            shouldStop: function () use (&$errors): bool {
                return $errors !== [];
            },
            consumer: 'metrics',
            autoAck: true,
            onError: function (Throwable $e) use (&$errors): void {
                $errors[] = $e->getMessage();
            },
        );

        $this->assertSame(2, $handled);
        $this->assertSame(['it exploded'], $errors);
        $this->assertSame(['waiting' => 0, 'processing' => 0, 'retrying' => 0], $this->client->consumersGet($queue, 'metrics')->stats);
    }

    public function testProcessReportsANackTheBrokerCouldNotTakeAndGoesOn(): void
    {
        $server = BerthqServer::dedicated();
        $client = new Client($server->url, timeout: 2);
        $queue = 'php-' . bin2hex(random_bytes(6));
        $client->queuesCreate($queue);
        $client->publishBatch($queue, ['ok', 'boom', 'after']);

        $seen = [];
        $errors = [];
        $handled = $client->process(
            $queue,
            function (Delivery $delivery) use (&$seen, $server): void {
                $seen[] = $delivery->body;
                if ($delivery->body === 'boom') {
                    $server->stop();
                    throw new RuntimeException('it exploded');
                }
            },
            batch: 10,
            waitMs: 200,
            shouldStop: function () use (&$errors): bool {
                return $errors !== [];
            },
            onError: function (Throwable $e, Delivery $delivery) use (&$errors): void {
                $errors[] = [$delivery->body, $e instanceof BerthqException && $e->isTransport()];
            },
        );

        $this->assertSame(3, $handled);
        $this->assertSame(['ok', 'boom', 'after'], $seen);
        $this->assertSame([['boom', true], ['ok', true], ['after', true]], $errors);
    }

    public function testProcessSendsWaitingAcksWhenItEndsWithAnException(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch($queue, ['a', 'b']);

        $seen = [];
        try {
            $this->client->process(
                $queue,
                function (Delivery $delivery) use (&$seen): void {
                    $seen[] = $delivery->body;
                },
                batch: 10,
                waitMs: 200,
                shouldStop: function () use (&$seen): bool {
                    if (count($seen) >= 2) {
                        throw new RuntimeException('out of memory budget');
                    }

                    return false;
                },
            );
            $this->fail('expected the exception from shouldStop');
        } catch (RuntimeException $e) {
            $this->assertSame('out of memory budget', $e->getMessage());
        }

        $this->assertSame(['waiting' => 0, 'processing' => 0, 'retrying' => 0], $this->client->consumersGet($queue, 'default')->stats);
    }

    public function testProcessSkipsDeliveriesWhoseLeaseRanOutWhileTheyWaited(): void
    {
        $queue = $this->queue();
        $this->client->publishBatch($queue, ['slow', 'late-1', 'late-2']);

        $seen = [];
        $errors = [];
        $handled = $this->client->process(
            $queue,
            function (Delivery $delivery) use (&$seen): void {
                $seen[] = $delivery->body;
                if ($delivery->body === 'slow') {
                    usleep(1_200_000);
                }
            },
            batch: 10,
            waitMs: 200,
            shouldStop: function () use (&$seen): bool {
                return $seen !== [];
            },
            visibilityTimeoutMs: 1000,
            onError: function (Throwable $e, Delivery $delivery) use (&$errors): void {
                $errors[] = $delivery->body;
            },
        );

        $this->assertSame(['slow'], $seen);
        $this->assertSame(1, $handled);
        $this->assertSame(['late-1', 'late-2'], $errors);
    }

    public function testProcessThrowsWhenTheQueueDoesNotExist(): void
    {
        $this->expectBerthqError('notFound', 404, fn () => $this->client->process('missing-' . bin2hex(random_bytes(4)), fn () => null, waitMs: 0));
    }
}
