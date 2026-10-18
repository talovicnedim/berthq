<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\BerthqException;
use Berthq\Client;
use PHPUnit\Framework\TestCase;

/** What the client sends, and what it makes of odd answers, against a fake broker. */
final class RequestsTest extends TestCase
{
    public function testBodiesArePlainUtf8WithoutNeedlessEscapes(): void
    {
        $broker = FakeBroker::start();
        $client = new Client($broker->url);
        $body = ['text' => 'é</>&'];
        $client->publish('jobs', $body);
        $publisher = $client->publisher('jobs');
        $publisher->publish($body);
        $publisher->flush();

        $this->assertSame(
            ['{"body":{"text":"é</>&"}}', '{"messages":[{"body":{"text":"é</>&"}}]}'],
            array_column($broker->requests(), 'body'),
        );
    }

    public function testABatchAnswerWithTooFewResultsFailsEveryMessageOfIt(): void
    {
        $publisher = (new Client(FakeBroker::start()->url))->publisher('jobs');
        $publisher->publish('a');
        $publisher->publish('short');
        $results = $publisher->flush();

        $this->assertCount(2, $results);
        foreach ($results as $result) {
            $this->assertInstanceOf(BerthqException::class, $result);
            $this->assertSame('http', $result->kind);
            $this->assertSame('the broker answered 1 results for 2 messages', $result->getMessage());
        }
    }
}
