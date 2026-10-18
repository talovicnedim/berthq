<?php

/*
 * The fake broker's answers, served by `php -S`. Every request is logged to FAKE_BROKER_LOG as
 * one JSON line. A batch with a `short` body gets one result too few.
 */

declare(strict_types=1);

$body = (string) file_get_contents('php://input');
$path = (string) parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
file_put_contents((string) getenv('FAKE_BROKER_LOG'), json_encode(['path' => $path, 'body' => $body]) . "\n", FILE_APPEND | LOCK_EX);

header('Content-Type: application/json');
http_response_code(201);
if (!str_ends_with($path, '/batch')) {
    echo json_encode(['id' => 'id-1', 'duplicate' => false]);

    return;
}
$messages = json_decode($body, true)['messages'];
$results = array_map(fn (int $i) => ['id' => "id-{$i}", 'duplicate' => false], array_keys($messages));
if (in_array('short', array_column($messages, 'body'), true)) {
    array_pop($results);
}
echo json_encode(['messages' => $results]);
