<?php

declare(strict_types=1);

namespace Berthq\Laravel;

use Berthq\Client;
use Illuminate\Contracts\Foundation\CachesConfiguration;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;

/**
 * Registers the `berthq` queue driver and a `berthq` connection. Auto-discovered through
 * composer's `extra.laravel`.
 */
final class BerthqServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->addDefaultConnection();

        // callAfterResolving also covers a queue manager that was resolved before this provider.
        $this->callAfterResolving('queue', function (QueueManager $manager): void {
            $manager->addConnector('berthq', fn () => new BerthqConnector());
        });

        // The client for consumers, inspection and admin calls, configured like the queue connection.
        $this->app->singleton(Client::class, function ($app): Client {
            return BerthqConnector::client($app['config']->get('queue.connections.berthq', []));
        });
    }

    /**
     * Adds a `berthq` connection read from the `BERTHQ_*` env vars, so `QUEUE_CONNECTION=berthq`
     * is all an app needs. A `berthq` connection in `config/queue.php` replaces it.
     */
    private function addDefaultConnection(): void
    {
        // Like mergeConfigFrom: a cached config already holds what this added before caching.
        if ($this->app instanceof CachesConfiguration && $this->app->configurationIsCached()) {
            return;
        }
        $config = $this->app['config'];
        if ($config->has('queue.connections.berthq')) {
            return;
        }
        $config->set('queue.connections.berthq', [
            'driver' => 'berthq',
            'url' => env('BERTHQ_URL', 'http://127.0.0.1:7331'),
            'key' => env('BERTHQ_API_KEY'),
            'queue' => env('BERTHQ_QUEUE', 'default'),
            'wait' => 3,
        ]);
    }
}
