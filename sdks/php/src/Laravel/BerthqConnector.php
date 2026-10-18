<?php

declare(strict_types=1);

namespace Berthq\Laravel;

use Berthq\Client;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Connectors\ConnectorInterface;

/**
 * Builds a {@see BerthqQueue} from a `config/queue.php` connection:
 *
 * ```php
 * 'berthq' => [
 *     'driver' => 'berthq',
 *     'url' => env('BERTHQ_URL', 'http://127.0.0.1:7331'),
 *     'key' => env('BERTHQ_API_KEY'),
 *     'queue' => env('BERTHQ_QUEUE', 'default'),
 *     'wait' => 3,
 * ],
 * ```
 *
 * Optional: `retry_after` (lease length in seconds, default: the broker queue's setting),
 * `timeout` (HTTP timeout in seconds), `consumer` (the consumer the workers share, default
 * `default`), `worker` (name in the dashboard), `create_queues`, `dead_letter` and
 * `after_commit`. See the README for what they do.
 */
final class BerthqConnector implements ConnectorInterface
{
    public function connect(array $config): QueueContract
    {
        return new BerthqQueue(
            client: self::client($config),
            default: (string) ($config['queue'] ?? 'default'),
            waitSeconds: (int) ($config['wait'] ?? 3),
            retryAfter: isset($config['retry_after']) ? (int) $config['retry_after'] : null,
            consumer: (string) ($config['consumer'] ?? 'default'),
            worker: $config['worker'] ?? null,
            createQueues: (bool) ($config['create_queues'] ?? false),
            deadLetter: (bool) ($config['dead_letter'] ?? true),
            dispatchAfterCommit: $config['after_commit'] ?? null,
        );
    }

    /** A client for a connection config; also used to bind {@see Client} in the container. */
    public static function client(array $config): Client
    {
        return new Client(
            url: (string) ($config['url'] ?? 'http://127.0.0.1:7331'),
            apiKey: $config['key'] ?? null,
            timeout: (float) ($config['timeout'] ?? 30),
        );
    }
}
