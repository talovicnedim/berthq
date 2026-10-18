<?php

declare(strict_types=1);

namespace Berthq\Tests;

use RuntimeException;

/**
 * Runs a real berthq binary on a free port with a temporary data directory. Instances are
 * shared by all tests and stopped when PHPUnit exits.
 */
final class BerthqServer
{
    /** @var array<string, self> */
    private static array $running = [];

    /** @var resource */
    private $process;

    private function __construct(public readonly string $url, $process, private readonly string $dataDir)
    {
        $this->process = $process;
    }

    /** The open broker (no API keys). */
    public static function open(): self
    {
        return self::$running['open'] ??= self::start([]);
    }

    /** A broker that requires keys: `adminadminadmin1` (admin) and `publishpublish12` (publish). */
    public static function secured(): self
    {
        return self::$running['secured'] ??= self::start(['--api-key', 'adminadminadmin1', '--api-key', 'publish:publishpublish12']);
    }

    /** A broker of its own (no API keys), for a test that stops it. */
    public static function dedicated(): self
    {
        return self::$running[bin2hex(random_bytes(6))] = self::start([]);
    }

    public static function stopAll(): void
    {
        foreach (self::$running as $server) {
            $server->stop();
        }
        self::$running = [];
    }

    /** @param list<string> $args */
    private static function start(array $args): self
    {
        $binary = getenv('BERTHQ_BINARY') ?: dirname(__DIR__, 3) . '/target/debug/berthq';
        if (!is_executable($binary)) {
            throw new RuntimeException("berthq binary not found at {$binary}; run `cargo build -p berthq-cli` in the repo root");
        }

        $port = self::freePort();
        $dataDir = sys_get_temp_dir() . '/berthq-php-' . bin2hex(random_bytes(6));
        mkdir($dataDir);
        $command = [$binary, 'start', '--host', '127.0.0.1', '--port', (string) $port, '--data', $dataDir, '--shutdown-timeout', '1s', ...$args];
        // A clean environment, so BERTHQ_* variables from the developer's shell (e.g.
        // BERTHQ_API_KEYS) can't change the instance under test.
        $env = ['PATH' => getenv('PATH') ?: '/usr/bin:/bin', 'RUST_LOG' => 'warn'];
        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['file', $dataDir . '/berthq.log', 'a'], 2 => ['file', $dataDir . '/berthq.log', 'a']], $pipes, null, $env);
        if (!is_resource($process)) {
            throw new RuntimeException('could not start the broker');
        }

        $server = new self("http://127.0.0.1:{$port}", $process, $dataDir);
        $server->waitUntilHealthy();

        return $server;
    }

    private function waitUntilHealthy(): void
    {
        $deadline = microtime(true) + 15;
        while (microtime(true) < $deadline) {
            $answer = @file_get_contents($this->url . '/healthz');
            if ($answer !== false && trim($answer) === 'ok') {
                return;
            }
            if (!proc_get_status($this->process)['running']) {
                break;
            }
            usleep(50_000);
        }
        $log = @file_get_contents($this->dataDir . '/berthq.log');
        $this->stop();
        throw new RuntimeException("the broker did not become healthy:\n{$log}");
    }

    public function stop(): void
    {
        if (is_resource($this->process)) {
            proc_terminate($this->process, SIGTERM);
            $deadline = microtime(true) + 5;
            while (proc_get_status($this->process)['running'] && microtime(true) < $deadline) {
                usleep(20_000);
            }
            if (proc_get_status($this->process)['running']) {
                proc_terminate($this->process, SIGKILL);
            }
            proc_close($this->process);
        }
        self::removeDir($this->dataDir);
    }

    private static function freePort(): int
    {
        $socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        if ($socket === false) {
            throw new RuntimeException("no free port: {$error}");
        }
        $name = stream_socket_get_name($socket, false);
        fclose($socket);

        return (int) substr((string) $name, strrpos((string) $name, ':') + 1);
    }

    private static function removeDir(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        foreach (array_diff(scandir($dir) ?: [], ['.', '..']) as $entry) {
            $path = $dir . '/' . $entry;
            is_dir($path) ? self::removeDir($path) : unlink($path);
        }
        rmdir($dir);
    }
}
