<?php

declare(strict_types=1);

namespace Berthq;

/**
 * Paths of the HTTP API. Names only use safe characters, but user input shouldn't be able to
 * change a path, so every part is encoded.
 *
 * @internal Used by {@see Client}.
 */
final class Path
{
    public static function queue(string $queue, string $rest = ''): string
    {
        return '/v1/queues/' . rawurlencode($queue) . $rest;
    }

    public static function consumer(string $queue, string $consumer, string $rest = ''): string
    {
        return self::queue($queue, '/consumers/' . rawurlencode($consumer) . $rest);
    }

    /**
     * A query string of the parameters that are set: `?state=retrying&limit=10`, or '' for none.
     *
     * @param array<string, string|int|null> $params
     */
    public static function query(array $params): string
    {
        $query = http_build_query(array_filter($params, fn (mixed $value) => $value !== null));

        return $query === '' ? '' : '?' . $query;
    }
}
