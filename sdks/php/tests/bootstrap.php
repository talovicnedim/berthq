<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

// Brokers are started lazily by the tests that need them and stopped when PHPUnit exits.
register_shutdown_function([\Berthq\Tests\BerthqServer::class, 'stopAll']);
