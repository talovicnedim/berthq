<?php

declare(strict_types=1);

namespace Berthq\Laravel;

use Berthq\Client;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;

/**
 * Registers the `berthq` queue driver. Auto-discovered through composer's `extra.laravel`.
 */
final class BerthqServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        // callAfterResolving also covers a queue manager that was resolved before this provider.
        $this->callAfterResolving('queue', function (QueueManager $manager): void {
            $manager->addConnector('berthq', fn () => new BerthqConnector());
        });

        // The client for consumers, inspection and admin calls, configured like the queue connection.
        $this->app->singleton(Client::class, function ($app): Client {
            return BerthqConnector::client($app['config']->get('queue.connections.berthq', []));
        });
    }
}
