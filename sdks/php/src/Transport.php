<?php

declare(strict_types=1);

namespace Berthq;

use CurlHandle;
use JsonException;

/**
 * Sends JSON requests with ext-curl. One handle is reused so keep-alive connections survive
 * between calls, which matters for workers that ack every message.
 *
 * @internal Use {@see Client}.
 */
final class Transport
{
    private ?CurlHandle $curl = null;

    public function __construct(
        private readonly string $baseUrl,
        private readonly ?string $apiKey,
        private readonly float $timeout,
        private readonly float $connectTimeout = 5.0,
    ) {
    }

    /**
     * Sends a request and returns the decoded JSON answer (null for an empty answer).
     *
     * @param float $extraTimeout Seconds added to the timeout, for long-polling requests.
     *
     * @throws BerthqException
     */
    public function request(string $method, string $path, mixed $body = null, float $extraTimeout = 0.0): mixed
    {
        return $this->send($method, $path, $body === null ? null : self::encode($body), $extraTimeout);
    }

    /**
     * Sends a body that is already JSON, as it is.
     *
     * @throws BerthqException
     */
    public function requestJson(string $method, string $path, string $json): mixed
    {
        return $this->send($method, $path, $json, 0.0);
    }

    private function send(string $method, string $path, ?string $json, float $extraTimeout): mixed
    {
        $curl = $this->handle();
        // An empty Expect header stops curl from waiting for "100 Continue" on large bodies.
        $headers = ['Accept: application/json', 'Expect:'];
        if ($this->apiKey !== null && $this->apiKey !== '') {
            $headers[] = 'Authorization: Bearer ' . $this->apiKey;
        }

        $options = [
            CURLOPT_URL => $this->baseUrl . $path,
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT_MS => (int) ($this->connectTimeout * 1000),
            CURLOPT_TIMEOUT_MS => (int) (($this->timeout + $extraTimeout) * 1000),
            CURLOPT_NOSIGNAL => true,
            // HTTP/2 over TLS when the broker offers it; plain HTTP stays on HTTP/1.1.
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_2TLS,
        ];
        if ($json !== null) {
            $headers[] = 'Content-Type: application/json';
            $options[CURLOPT_POSTFIELDS] = $json;
        }
        $options[CURLOPT_HTTPHEADER] = $headers;
        curl_setopt_array($curl, $options);

        $raw = curl_exec($curl);
        if ($raw === false) {
            throw $this->transportError($curl);
        }
        $status = (int) curl_getinfo($curl, CURLINFO_RESPONSE_CODE);

        if ($status < 200 || $status >= 300) {
            throw self::apiError($status, (string) $raw);
        }
        if ($raw === '' || $status === 204) {
            return null;
        }
        try {
            return json_decode((string) $raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new BerthqException('invalid response from the broker: ' . $e->getMessage(), 'http', $status, $e);
        }
    }

    public static function encode(mixed $value): string
    {
        try {
            return json_encode(
                $value,
                JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
            );
        } catch (JsonException $e) {
            throw new BerthqException('could not encode the request as JSON: ' . $e->getMessage(), 'invalid', 0, $e);
        }
    }

    private function handle(): CurlHandle
    {
        if ($this->curl === null) {
            $this->curl = curl_init();
        } else {
            // Reset clears the previous request's options but keeps the open connection.
            curl_reset($this->curl);
        }

        return $this->curl;
    }

    private function transportError(CurlHandle $curl): BerthqException
    {
        $error = curl_error($curl);

        return match (curl_errno($curl)) {
            CURLE_OPERATION_TIMEDOUT => new BerthqException("the broker at {$this->baseUrl} did not answer in time"),
            default => new BerthqException("could not connect to the broker at {$this->baseUrl} ({$error})"),
        };
    }

    private static function apiError(int $status, string $raw): BerthqException
    {
        $decoded = json_decode($raw, true);
        $error = is_array($decoded) && is_array($decoded['error'] ?? null) ? $decoded['error'] : [];
        $kind = is_string($error['kind'] ?? null) ? $error['kind'] : 'http';
        $message = is_string($error['message'] ?? null) ? $error['message'] : "the broker answered {$status}";

        return new BerthqException($message, $kind, $status);
    }
}
