<?php

declare(strict_types=1);

namespace Berthq\Tests\Laravel;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use RuntimeException;

/** A queued job like an application's, recording each run. */
final class SendEmail implements ShouldQueue
{
    use InteractsWithQueue;
    use Queueable;

    /** @var list<array{to: string, attempt: int}> */
    public static array $runs = [];

    public int $tries = 2;

    public function __construct(
        public string $to,
        public bool $fail = false,
        public ?int $releaseAfter = null,
    ) {
    }

    public function handle(): void
    {
        self::$runs[] = ['to' => $this->to, 'attempt' => $this->attempts()];
        if ($this->releaseAfter !== null) {
            $this->release($this->releaseAfter);

            return;
        }
        if ($this->fail) {
            throw new RuntimeException("could not reach {$this->to}");
        }
    }
}
