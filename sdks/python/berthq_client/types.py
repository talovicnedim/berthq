"""Request and response bodies of the HTTP API (see docs/api.md), as dataclasses with
snake_case fields. ``_json`` turns them into the API's camelCase JSON and back."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from . import _json


@dataclass
class NewMessage:
    """A message to publish. Pass it to ``publish_batch`` when a message needs more than a body;
    plain values are published as bodies."""

    body: Any
    priority: Optional[int] = None
    """0-9, higher is delivered first."""
    delay_ms: Optional[int] = None
    deliver_at: Optional[str] = None
    """RFC 3339 time to deliver at; overrides ``delay_ms``."""
    dedup_id: Optional[str] = None
    """Repeats of this id within an hour are ignored."""

    def to_json(self) -> Dict[str, Any]:
        # The body is always sent, even when it is None (JSON null is a valid body).
        options = {name: value for name, value in vars(self).items() if name != "body"}
        return {"body": self.body, **_json.to_json(options)}


@dataclass
class Published:
    id: str
    duplicate: bool
    """True when the ``dedup_id`` was already seen and nothing was stored."""


@dataclass
class QueueSettings:
    retention_ms: int
    """How long messages stay after they become visible, for replay."""
    visibility_timeout_ms: int
    max_attempts: int
    """Deliveries before a message moves to the dead-letter queue; 0 retries forever."""
    retry_delay_ms: int
    """First retry delay, doubled per attempt up to 5 minutes."""
    paused: bool = False
    max_messages: Optional[int] = None
    """Backlog limit: messages a consumer still has to process, plus delayed ones."""


@dataclass
class Counters:
    published: int = 0
    delivered: int = 0
    acked: int = 0
    retried: int = 0
    dead_lettered: int = 0
    dropped: int = 0
    """Ran out of attempts in a dead-letter queue, which has no dead-letter queue itself."""


@dataclass
class QueueStats:
    messages: int
    """Everything stored, including messages kept only for replay and delayed ones."""
    backlog: int
    """Messages at least one consumer still has to process."""
    delayed: int
    processing: int
    """Messages leased right now, across all consumers."""
    bytes: int
    consumers: int
    workers: int
    """Workers seen in the last minute, across all consumers."""
    counters: Counters
    publish_rate: int
    deliver_rate: int
    ack_rate: int


@dataclass
class QueueInfo:
    name: str
    created_at: str
    settings: QueueSettings
    stats: QueueStats
    dead_letter_queue: Optional[str] = None
    dead_letter_of: Optional[str] = None


@dataclass
class ConsumerStats:
    waiting: int
    """Messages after its position it hasn't received yet."""
    processing: int
    retrying: int
    """Messages it received before that wait for their next attempt, backoff over or not."""


@dataclass
class WorkerInfo:
    """A process consuming for a consumer, seen in the last minute."""

    name: str
    last_seen: str
    waiting: bool
    """True while it is long-polling."""


@dataclass
class ConsumerInfo:
    """A name with a saved position in a queue. Every consumer gets every message."""

    queue: str
    name: str
    created_at: str
    paused: bool
    stats: ConsumerStats
    idle: bool = False
    """No worker for an hour or more while it still holds messages back from deletion."""
    last_seen: Optional[str] = None
    """The last time a worker consumed for it; ``None`` if never."""
    workers: List[WorkerInfo] = field(default_factory=list)


@dataclass
class StatsPoint:
    at: str
    published: int
    delivered: int
    acked: int
    failed: int


@dataclass
class QueueDetails(QueueInfo):
    """A queue plus its consumers and its throughput over the last minute."""

    consumers: List[ConsumerInfo] = field(default_factory=list)
    history: List[StatsPoint] = field(default_factory=list)


@dataclass
class DiskStats:
    """Free space on the broker's data disk, in bytes."""

    free: int
    min_free: int
    """The broker's ``--min-free-disk``; 0 when the check is off."""
    full: bool
    """True while ``free`` is below ``min_free``: publishes fail with ``diskFull``."""


@dataclass
class Overview:
    version: str
    started_at: str
    queues: int
    messages: int
    backlog: int
    processing: int
    failed: int
    """Messages in dead-letter queues."""
    consumers: int
    workers: int
    publish_rate: int
    deliver_rate: int
    ack_rate: int
    counters: Counters
    disk: DiskStats
    history: List[StatsPoint] = field(default_factory=list)


@dataclass
class Delivery:
    id: str
    """The same on every redelivery and replay, so it works as an idempotency key."""
    queue: str
    consumer: str
    body: Any
    priority: int
    attempt: int
    """1 on the first delivery."""
    published_at: str
    visibility_timeout_ms: int
    """The lease length; ``extend`` before it runs out to keep the message. 0 with
    ``auto_ack``."""
    lease: Optional[str] = None
    """Pass to ``ack``, ``nack`` or ``extend``. ``None`` with ``auto_ack``."""
    lease_expires_at: Optional[str] = None
    dedup_id: Optional[str] = None
    last_error: Optional[str] = None


@dataclass
class Consumed:
    """What ``consume`` returns: the messages, and how the ``ack`` leases were settled."""

    messages: List[Delivery] = field(default_factory=list)
    settled: int = 0
    unknown: List[str] = field(default_factory=list)
    """``ack`` leases that no longer exist (already settled, expired and redelivered, or
    dropped by a replay)."""


@dataclass
class Settled:
    settled: int
    unknown: List[str] = field(default_factory=list)
    """Leases that no longer exist (already settled, expired and redelivered, or dropped by a
    replay)."""


@dataclass
class MessageDeadLetter:
    queue: str
    """The queue the message failed in."""
    consumer: str
    """The consumer that failed it; ``requeue`` moves it back to this consumer."""
    message_id: str
    """The original message's id (the dead letter has its own ``id``). A requeued message is
    delivered with this id again."""
    reason: str
    """``maxAttempts`` or ``rejected``."""
    at: str


@dataclass
class MessageInfo:
    id: str
    queue: str
    state: str
    """``visible`` or ``delayed``."""
    priority: int
    published_at: str
    size: int
    attempts: int = 0
    """In a dead-letter queue: the failed consumer's attempts."""
    deliver_at: Optional[str] = None
    dedup_id: Optional[str] = None
    last_error: Optional[str] = None
    dead_letter: Optional[MessageDeadLetter] = None
    body: Any = None
    """Only set by ``messages_get``."""


@dataclass
class ConsumerMessage:
    """A message as one consumer sees it: not received yet, being processed, or waiting for its
    next attempt."""

    id: str
    queue: str
    consumer: str
    state: str
    """``waiting``, ``processing`` or ``retrying``."""
    priority: int
    attempts: int
    published_at: str
    size: int
    last_error: Optional[str] = None
    worker: Optional[str] = None
    """Set while processing."""
    lease_expires_at: Optional[str] = None
    """Set while processing."""
    retry_at: Optional[str] = None
    """Set while retrying."""


@dataclass
class Requeued:
    requeued: int
    skipped: int
    """The queue or consumer no longer exists, or the consumer already has the message
    pending."""

