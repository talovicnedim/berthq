<?php

declare(strict_types=1);

namespace Berthq\Tests;

use RuntimeException;

/**
 * A fake broker on `php -S`, for tests that check what the client sends. Its answers are in
 * `fake/router.php`. Stopped when PHPUnit exits.
 */
final class FakeBroker
{
    /** @var resource */
    private $process;

    private function __construct(public readonly string $url, $process, private readonly string $log)
    {
        $this->process = $process;
    }

    public static function start(): self
    {
        $socket = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
        if ($socket === false) {
            throw new RuntimeException("no free port: {$error}");
        }
        $address = (string) stream_socket_get_name($socket, false);
        fclose($socket);

        $log = tempnam(sys_get_temp_dir(), 'berthq-php-fake-');
        $env = ['PATH' => getenv('PATH') ?: '/usr/bin:/bin', 'FAKE_BROKER_LOG' => $log];
        $command = [PHP_BINARY, '-S', $address, __DIR__ . '/fake/router.php'];
        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes, null, $env);
        if (!is_resource($process)) {
            throw new RuntimeException('could not start the fake broker');
        }
        $broker = new self("http://{$address}", $process, $log);
        register_shutdown_function($broker->stop(...));
        $broker->waitUntilListening($address);

        return $broker;
    }

    /** @return list<array{path: string, body: string}> Every request so far, in order. */
    public function requests(): array
    {
        $lines = file($this->log, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];

        return array_map(fn (string $line) => json_decode($line, true), $lines);
    }

    public function stop(): void
    {
        if (is_resource($this->process)) {
            proc_terminate($this->process);
            proc_close($this->process);
        }
        @unlink($this->log);
    }

    private function waitUntilListening(string $address): void
    {
        $deadline = microtime(true) + 5;
        while (microtime(true) < $deadline) {
            $connection = @stream_socket_client("tcp://{$address}");
            if ($connection !== false) {
                fclose($connection);

                return;
            }
            usleep(20_000);
        }
        $this->stop();
        throw new RuntimeException("the fake broker did not start on {$address}");
    }
}
