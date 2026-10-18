<?php

declare(strict_types=1);

namespace Berthq\Tests;

use Berthq\Delivery;
use Berthq\PendingAcks;
use PHPUnit\Framework\TestCase;

final class PendingAcksTest extends TestCase
{
    public function testTheEarliestDeadlineDecidesWhenAcksAreDue(): void
    {
        $acks = new PendingAcks();
        $this->assertFalse($acks->due());
        $acks->push(self::delivery('a'), microtime(true) + 60);
        $this->assertFalse($acks->due());
        $acks->push(self::delivery('b'), microtime(true) - 1);
        $this->assertTrue($acks->due());
    }

    public function testTakeEmptiesTheBufferInChunksOf1000(): void
    {
        $acks = new PendingAcks();
        for ($n = 0; $n < 1500; $n++) {
            $acks->push(self::delivery((string) $n), microtime(true) + 60);
        }
        $this->assertCount(1000, $acks->take());
        $this->assertCount(500, $acks->take());
        $this->assertFalse($acks->due());
    }

    public function testRestoredAcksAreDueRightAway(): void
    {
        $acks = new PendingAcks();
        $delivery = self::delivery('a');
        $acks->restore([$delivery]);
        $this->assertTrue($acks->due());
        $this->assertSame([$delivery], $acks->take());
    }

    private static function delivery(string $lease): Delivery
    {
        return new Delivery('01J', $lease, 'jobs', 'default', null, 0, 1, '2026-10-17T00:00:00Z', null, null, null, 30_000);
    }
}
