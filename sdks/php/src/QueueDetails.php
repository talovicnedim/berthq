<?php

declare(strict_types=1);

namespace Berthq;

/**
 * A queue with its consumers and the last minute of throughput, from {@see Client::queuesGet()}.
 */
final class QueueDetails extends QueueInfo
{
    /**
     * @param list<ConsumerInfo> $consumers
     * @param list<array{at: string, published: int, delivered: int, acked: int, failed: int}> $history
     */
    public function __construct(
        QueueInfo $info,
        public readonly array $consumers,
        public readonly array $history,
    ) {
        parent::__construct($info->name, $info->createdAt, $info->settings, $info->stats, $info->deadLetterQueue, $info->deadLetterOf);
    }

    public static function fromArray(array $data): self
    {
        return new self(
            info: QueueInfo::fromArray($data),
            consumers: array_map(ConsumerInfo::fromArray(...), $data['consumers'] ?? []),
            history: $data['history'] ?? [],
        );
    }
}
