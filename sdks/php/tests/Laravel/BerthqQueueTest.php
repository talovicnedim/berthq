<?php

declare(strict_types=1);

namespace Berthq\Tests\Laravel;

use Berthq\Client;
use Berthq\Laravel\BerthqJob;
use Berthq\Laravel\BerthqQueue;
use Berthq\Laravel\BerthqServiceProvider;
use Berthq\Tests\BerthqServer;
use Illuminate\Bus\Dispatcher as BusDispatcher;
use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Contracts\Bus\Dispatcher as BusDispatcherContract;
use Illuminate\Contracts\Container\Container as ContainerContract;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher as EventsContract;
use Illuminate\Events\Dispatcher;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use PHPUnit\Framework\TestCase;
use Throwable;

/**
 * The driver inside a minimal Laravel container, run by Laravel's own queue worker.
 */
final class BerthqQueueTest extends TestCase
{
    private Container $app;
    private Client $client;
    private string $queueName;
    /** @var list<Throwable> */
    private array $reported = [];

    protected function setUp(): void
    {
        SendEmail::$runs = [];
        $url = BerthqServer::open()->url;
        $this->client = new Client($url);
        $this->queueName = 'laravel-' . bin2hex(random_bytes(6));
        $this->client->queuesCreate($this->queueName, ['maxAttempts' => 0]);

        $this->app = new Container();
        Container::setInstance($this->app);
        $this->app->instance(ContainerContract::class, $this->app);
        $events = new Dispatcher($this->app);
        $this->app->instance('events', $events);
        $this->app->instance(EventsContract::class, $events);
        $this->app->instance(BusDispatcherContract::class, new BusDispatcher($this->app));
        $this->app->instance('config', new Repository(['queue' => [
            'default' => 'berthq',
            'connections' => [
                'berthq' => ['driver' => 'berthq', 'url' => $url, 'queue' => $this->queueName, 'wait' => 0],
                // A released job is due right away; waiting covers the moment until it is.
                'berthq-retry' => ['driver' => 'berthq', 'url' => $url, 'queue' => $this->queueName, 'wait' => 2],
                'berthq-no-dlq' => ['driver' => 'berthq', 'url' => $url, 'queue' => $this->queueName, 'wait' => 2, 'dead_letter' => false],
                'berthq-audit' => ['driver' => 'berthq', 'url' => $url, 'queue' => $this->queueName, 'wait' => 0, 'consumer' => 'audit', 'worker' => 'audit-1'],
                'berthq-auto' => ['driver' => 'berthq', 'url' => $url, 'queue' => 'unused', 'wait' => 0, 'create_queues' => true],
            ],
        ]]));
        $this->app->instance('queue', new QueueManager($this->app));
        (new BerthqServiceProvider($this->app))->register();
    }

    protected function tearDown(): void
    {
        Container::setInstance(null);
    }

    public function testTheServiceProviderRegistersTheDriverAndClient(): void
    {
        $this->assertInstanceOf(BerthqQueue::class, $this->queue());
        $this->assertSame(BerthqServer::open()->url, $this->app->make(Client::class)->url());
    }

    public function testPushedJobsRunAndAreAcked(): void
    {
        $id = $this->queue()->push(new SendEmail('ada@example.com'));
        $this->assertIsString($id);
        $this->assertSame(1, $this->queue()->size());
        $this->assertSame(1, $this->queue()->pendingSize());
        // Unknown until a worker has created the connection's consumer.
        $this->assertNull($this->queue()->creationTimeOfOldestPendingJob());
        $this->client->consumersCreate($this->queueName, 'default');
        $this->assertNotNull($this->queue()->creationTimeOfOldestPendingJob());

        $this->work();

        $this->assertSame([['to' => 'ada@example.com', 'attempt' => 1]], SendEmail::$runs);
        $this->assertSame(0, $this->queue()->size());
        $this->assertSame([], $this->reported);
    }

    public function testPopReturnsAJobWithTheBrokerMessage(): void
    {
        $id = $this->queue()->push(new SendEmail('ada@example.com'));

        $job = $this->queue()->pop();
        $this->assertInstanceOf(BerthqJob::class, $job);
        $this->assertSame($id, $job->getJobId());
        $this->assertSame(1, $job->attempts());
        $this->assertSame($this->queueName, $job->getQueue());
        $this->assertSame(SendEmail::class, $job->resolveName());
        $this->assertSame(2, $job->maxTries());
        $this->assertSame(1, $this->queue()->reservedSize());
        $this->assertStringContainsString(':', $this->client->consumersGet($this->queueName, 'default')->workers[0]['name']);

        $job->delete();
        $this->assertSame(0, $this->queue()->size());
        $this->assertNull($this->queue()->pop());
    }

    public function testReleasedJobsComeBackAfterTheirDelay(): void
    {
        $this->queue()->push(new SendEmail('ada@example.com', releaseAfter: 30));
        $this->work();

        $this->assertSame(1, $this->queue()->delayedSize());
        $this->assertNull($this->queue()->pop());
        $retrying = $this->client->consumersMessages($this->queueName, 'default', 'retrying')[0];
        $this->assertGreaterThan(time() + 20, strtotime((string) $retrying->retryAt));
    }

    public function testFailingJobsAreRetriedThenDeadLettered(): void
    {
        $failed = [];
        $this->app['events']->listen(JobFailed::class, function (JobFailed $event) use (&$failed): void {
            $failed[] = $event->exception->getMessage();
        });
        $this->queue()->push(new SendEmail('ada@example.com', fail: true));

        $this->work();
        $this->assertSame(1, $this->queue()->size(), 'released for a second try');
        $this->work('berthq-retry');

        $this->assertSame([1, 2], array_column(SendEmail::$runs, 'attempt'));
        $this->assertSame(['could not reach ada@example.com'], $failed);
        $this->assertSame(0, $this->queue()->size());
        $dead = $this->client->messagesList($this->queueName . '.dlq');
        $this->assertCount(1, $dead);
        $this->assertSame('rejected', $dead[0]->deadLetter['reason']);
        $this->assertStringContainsString('could not reach ada@example.com', (string) $dead[0]->lastError);
    }

    public function testFailedJobsAreDeletedWhenDeadLetteringIsOff(): void
    {
        $this->queue('berthq-no-dlq')->push(new SendEmail('ada@example.com', fail: true));

        $this->work('berthq-no-dlq');
        $this->work('berthq-no-dlq');

        $this->assertCount(2, SendEmail::$runs);
        $this->assertSame(0, $this->queue()->size());
        // A dead-letter queue is created with its first dead letter, so it must not exist.
        $this->assertNotContains($this->queueName . '.dlq', array_map(fn ($q) => $q->name, $this->client->queuesList()));
    }

    public function testEachConsumerGetsItsOwnCopyOfAJob(): void
    {
        $id = $this->queue()->push(new SendEmail('ada@example.com'));

        $job = $this->queue()->pop();
        $audit = $this->queue('berthq-audit')->pop();
        $this->assertSame($id, $job?->getJobId());
        $this->assertSame($id, $audit?->getJobId());

        $audit->delete();
        $this->assertSame(0, $this->queue('berthq-audit')->size());
        $this->assertSame(1, $this->queue()->reservedSize());
        $this->assertSame(['audit-1'], array_column($this->client->consumersGet($this->queueName, 'audit')->workers, 'name'));
    }

    public function testDelayedJobsWaitUntilDue(): void
    {
        $this->queue()->later(60, new SendEmail('ada@example.com'));
        $this->queue()->later(new \DateTimeImmutable('+2 minutes'), new SendEmail('bob@example.com'));

        $this->assertSame(2, $this->queue()->delayedSize());
        $this->assertSame(0, $this->queue()->pendingSize());
        $this->assertNull($this->queue()->pop());
    }

    public function testBulkPublishesEveryJob(): void
    {
        $this->queue()->bulk([
            new SendEmail('a@example.com'),
            new SendEmail('b@example.com'),
            (new SendEmail('c@example.com'))->delay(60),
        ]);

        $this->assertSame(2, $this->queue()->pendingSize());
        $this->assertSame(1, $this->queue()->delayedSize());
        $this->work();
        $this->work();
        $this->assertEqualsCanonicalizing(['a@example.com', 'b@example.com'], array_column(SendEmail::$runs, 'to'));
    }

    public function testClearPurgesWaitingJobs(): void
    {
        $this->queue()->push(new SendEmail('a@example.com'));
        $this->queue()->push(new SendEmail('b@example.com'));

        $this->assertSame(2, $this->queue()->clear($this->queueName));
        $this->assertSame(0, $this->queue()->size());
    }

    public function testCreateQueuesCreatesAMissingQueueOnFirstPush(): void
    {
        $name = 'laravel-auto-' . bin2hex(random_bytes(6));
        $queue = $this->queue('berthq-auto');

        $this->assertNull($queue->pop($name), 'a worker may start before the first push');
        $queue->push(new SendEmail('ada@example.com'), '', $name);

        $this->assertSame(0, $this->client->queuesGet($name)->settings['maxAttempts']);
        $this->assertSame(1, $queue->size($name));
    }

    public function testAMissingQueueFailsLoudlyByDefault(): void
    {
        $this->expectException(\Berthq\BerthqException::class);
        $this->queue()->push(new SendEmail('ada@example.com'), '', 'missing-' . bin2hex(random_bytes(4)));
    }

    private function queue(string $connection = 'berthq'): BerthqQueue
    {
        $queue = $this->app['queue']->connection($connection);
        $this->assertInstanceOf(BerthqQueue::class, $queue);

        return $queue;
    }

    /** Runs one job like `php artisan queue:work berthq --once` would. */
    private function work(string $connection = 'berthq'): void
    {
        $test = $this;
        $handler = new class ($test) implements ExceptionHandler {
            public function __construct(private BerthqQueueTest $test)
            {
            }

            public function report(Throwable $e): void
            {
                $this->test->recordReport($e);
            }

            public function shouldReport(Throwable $e): bool
            {
                return true;
            }

            public function render($request, Throwable $e): never
            {
                throw $e;
            }

            public function renderForConsole($output, Throwable $e): void
            {
            }
        };
        $worker = new Worker($this->app['queue'], $this->app['events'], $handler, fn () => false);
        $worker->runNextJob($connection, $this->queueName, new WorkerOptions(sleep: 0, maxTries: 0));
    }

    public function recordReport(Throwable $e): void
    {
        $this->reported[] = $e;
    }
}
