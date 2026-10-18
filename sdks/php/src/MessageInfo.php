<?php

declare(strict_types=1);

namespace Berthq;

/**
 * A stored message as seen by `messagesList` (without body) and `messagesGet` (with body).
 */
final class MessageInfo
{
    /**
     * @param string $state `visible` or `delayed`.
     * @param int $attempts In a dead-letter queue: the failed consumer's attempts. 0 otherwise.
     * @param array{queue: string, consumer: string, messageId: string, reason: string, at: string}|null $deadLetter
     *        Set for dead letters. `messageId` is the original message's id, which a requeued
     *        message is delivered with again; `reason` is `maxAttempts` or `rejected`.
     */
    public function __construct(
        public readonly string $id,
        public readonly string $queue,
        public readonly string $state,
        public readonly int $priority,
        public readonly string $publishedAt,
        public readonly ?string $deliverAt,
        public readonly ?string $dedupId,
        public readonly int $size,
        public readonly int $attempts,
        public readonly ?string $lastError,
        public readonly ?array $deadLetter,
        public readonly mixed $body,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            id: $data['id'],
            queue: $data['queue'],
            state: $data['state'],
            priority: $data['priority'] ?? 0,
            publishedAt: $data['publishedAt'],
            deliverAt: $data['deliverAt'] ?? null,
            dedupId: $data['dedupId'] ?? null,
            size: $data['size'] ?? 0,
            attempts: $data['attempts'] ?? 0,
            lastError: $data['lastError'] ?? null,
            deadLetter: $data['deadLetter'] ?? null,
            body: $data['body'] ?? null,
        );
    }
}
