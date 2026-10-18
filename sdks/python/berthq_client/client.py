"""The client. Method names match the HTTP API and the other SDKs: ``queues_create``,
``publish``, ``consume``, ``ack``, ``nack``, ``consumers_replay``, ``requeue``, ..."""

from typing import Any, Callable, Iterable, List, Optional, Sequence, Union

from ._http import Transport, query, segment
from ._json import from_json, to_json
from .publisher import Publisher
from .types import (
    Consumed,
    ConsumerInfo,
    ConsumerMessage,
    Delivery,
    MessageInfo,
    NewMessage,
    Overview,
    Published,
    QueueDetails,
    QueueInfo,
    Requeued,
    Settled,
)
from .worker import process as run_worker

Leases = Union[str, Sequence[str]]


class Client:
    """A client for the broker at ``url``. Safe to share between threads; each thread keeps
    its own connection open. ``close()`` (or a ``with`` block) closes them."""

    def __init__(
        self,
        url: str = "http://127.0.0.1:7331",
        api_key: Optional[str] = None,
        timeout: float = 30,
    ) -> None:
        self._http = Transport(url, api_key, timeout)

    @property
    def url(self) -> str:
        return self._http.base

    def close(self) -> None:
        """Closes the open connections. The client still works; it reconnects when needed."""
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # Queues

    def queues_list(self) -> List[QueueInfo]:
        response = self._http.request("GET", "/v1/queues")
        return [from_json(QueueInfo, queue) for queue in response["queues"]]

    def queues_create(self, name: str, **settings: Any) -> QueueInfo:
        """Creates a queue. Settings are snake_case keywords: ``retention_ms``,
        ``visibility_timeout_ms``, ``max_attempts``, ``retry_delay_ms``, ``max_messages``."""
        body = {"name": name, **to_json(settings)}
        return from_json(QueueInfo, self._http.request("POST", "/v1/queues", body))

    def queues_get(self, queue: str) -> QueueDetails:
        """Settings, stats, consumers and the last minute of throughput."""
        return from_json(QueueDetails, self._http.request("GET", _queue(queue)))

    def queues_update(self, queue: str, **settings: Any) -> QueueInfo:
        """Changes the given settings; ``max_messages=0`` removes the limit."""
        return from_json(QueueInfo, self._http.request("PATCH", _queue(queue), to_json(settings)))

    def queues_remove(self, queue: str) -> None:
        """Deletes the queue, its consumers and its messages. Its dead-letter queue stays."""
        self._http.request("DELETE", _queue(queue))

    def queues_purge(self, queue: str) -> int:
        """Deletes every message, including ones being processed; returns how many."""
        return self._http.request("POST", _queue(queue, "/purge"))["purged"]

    def queues_pause(self, queue: str) -> QueueInfo:
        """A paused queue accepts publishes but delivers nothing to any consumer."""
        return from_json(QueueInfo, self._http.request("POST", _queue(queue, "/pause")))

    def queues_resume(self, queue: str) -> QueueInfo:
        return from_json(QueueInfo, self._http.request("POST", _queue(queue, "/resume")))

    # Publishing

    def publish(
        self,
        queue: str,
        body: Any,
        *,
        priority: Optional[int] = None,
        delay_ms: Optional[int] = None,
        deliver_at: Optional[str] = None,
        dedup_id: Optional[str] = None,
    ) -> Published:
        """Publishes one message; returns once it is on disk. A ``dedup_id`` seen in the last
        hour stores nothing: the answer has the first message's id and ``duplicate=True``."""
        message = NewMessage(body, priority, delay_ms, deliver_at, dedup_id)
        response = self._http.request("POST", _queue(queue, "/messages"), message.to_json())
        return from_json(Published, response)

    def publisher(self, queue: str, **options: Any) -> Publisher:
        """A publisher for ``queue``: it sends messages in batches, much faster than one
        ``publish`` each. ``Publisher`` describes its options."""
        return Publisher(self, queue, **options)

    def publish_batch(self, queue: str, messages: Iterable[Any]) -> List[Published]:
        """Publishes up to 1000 messages (``NewMessage``s or plain bodies) atomically."""
        body = {"messages": [_message(entry).to_json() for entry in messages]}
        response = self._http.request("POST", _queue(queue, "/batch"), body)
        return [from_json(Published, published) for published in response["messages"]]

    def _publish_encoded(self, queue: str, messages: List[bytes]) -> List[Published]:
        """Publishes messages that are already encoded as JSON, as one atomic batch."""
        body = b'{"messages":[' + b",".join(messages) + b"]}"
        response = self._http.request("POST", _queue(queue, "/batch"), body)
        return [from_json(Published, published) for published in response["messages"]]

    # Consuming

    def consume(
        self,
        queue: str,
        *,
        consumer: Optional[str] = None,
        from_: Optional[str] = None,
        max: Optional[int] = None,
        wait_ms: Optional[int] = None,
        visibility_timeout_ms: Optional[int] = None,
        auto_ack: bool = False,
        worker: Optional[str] = None,
        ack: Optional[Sequence[str]] = None,
    ) -> Consumed:
        """Gets up to ``max`` messages for ``consumer`` (default ``default``), which is created
        if it doesn't exist, starting at ``from_`` (``start``, ``end``, an RFC 3339 time or a
        message id). ``ack`` leases (up to 1000) are acked first, saving a separate ``ack``
        call. With ``wait_ms``, waits for a message to arrive. With ``auto_ack`` there is no
        lease: the position moves right away and nothing is retried. ``worker`` is the name
        shown in the dashboard."""
        body = to_json(
            {
                "consumer": consumer,
                "from_": from_,
                "max": max,
                "wait_ms": wait_ms,
                "visibility_timeout_ms": visibility_timeout_ms,
                "auto_ack": auto_ack or None,
                "worker": worker,
                "ack": list(ack) if ack else None,
            }
        )
        # The request may legitimately take as long as the wait, plus time to answer.
        timeout = (wait_ms or 0) / 1000 + self._http.timeout
        response = self._http.request("POST", _queue(queue, "/consume"), body, timeout)
        return from_json(Consumed, response)

    def ack(self, queue: str, leases: Leases) -> Settled:
        """Done for the lease's consumer. Leases that no longer exist are listed in ``unknown``."""
        body = {"leases": _leases(leases)}
        return from_json(Settled, self._http.request("POST", _queue(queue, "/ack"), body))

    def nack(
        self,
        queue: str,
        leases: Leases,
        *,
        error: Optional[str] = None,
        delay_ms: Optional[int] = None,
        dead_letter: bool = False,
    ) -> Settled:
        """Fails messages: retried after ``delay_ms`` or the queue's backoff, or dead-lettered
        when out of attempts or ``dead_letter`` is true."""
        body = to_json(
            {
                "leases": _leases(leases),
                "error": error,
                "delay_ms": delay_ms,
                "dead_letter": dead_letter or None,
            }
        )
        return from_json(Settled, self._http.request("POST", _queue(queue, "/nack"), body))

    def extend(
        self, queue: str, leases: Leases, visibility_timeout_ms: Optional[int] = None
    ) -> Settled:
        """Restarts the lease clock of messages that take long to process."""
        body = to_json({"leases": _leases(leases), "visibility_timeout_ms": visibility_timeout_ms})
        return from_json(Settled, self._http.request("POST", _queue(queue, "/extend"), body))

    def process(self, queue: str, handler: Callable[[Delivery], Any], **options: Any) -> None:
        """Runs ``handler`` for every message of ``queue`` until ``stop_event`` is set. The
        options (``batch``, ``consumer``, ``from_``, ``worker``, ``visibility_timeout_ms``,
        ``auto_ack``, ``stop_event``, ``wait_ms``) are described on
        ``berthq_client.worker.process``."""
        run_worker(self, queue, handler, **options)

    # Consumers

    def consumers_list(self, queue: str) -> List[ConsumerInfo]:
        response = self._http.request("GET", _queue(queue, "/consumers"))
        return [from_json(ConsumerInfo, consumer) for consumer in response["consumers"]]

    def consumers_get(self, queue: str, consumer: str) -> ConsumerInfo:
        return from_json(ConsumerInfo, self._http.request("GET", _consumer(queue, consumer)))

    def consumers_create(self, queue: str, name: str, from_: Optional[str] = None) -> ConsumerInfo:
        """Creates a consumer starting at ``from_`` (default ``start``). Raises ``conflict`` if
        it exists; ``consume`` also creates consumers on first use."""
        body = to_json({"name": name, "from_": from_})
        return from_json(ConsumerInfo, self._http.request("POST", _queue(queue, "/consumers"), body))

    def consumers_remove(self, queue: str, consumer: str) -> None:
        """Removes the consumer with its position, leases and retries."""
        self._http.request("DELETE", _consumer(queue, consumer))

    def consumers_replay(self, queue: str, consumer: str, from_: str) -> ConsumerInfo:
        """Moves the position to ``start``, ``end``, an RFC 3339 time or a message id. Drops
        what the consumer was processing and retrying."""
        path = _consumer(queue, consumer, "/replay")
        return from_json(ConsumerInfo, self._http.request("POST", path, {"from": from_}))

    def consumers_pause(self, queue: str, consumer: str) -> ConsumerInfo:
        """A paused consumer gets nothing; other consumers continue."""
        path = _consumer(queue, consumer, "/pause")
        return from_json(ConsumerInfo, self._http.request("POST", path))

    def consumers_resume(self, queue: str, consumer: str) -> ConsumerInfo:
        path = _consumer(queue, consumer, "/resume")
        return from_json(ConsumerInfo, self._http.request("POST", path))

    def consumers_messages(
        self,
        queue: str,
        consumer: str,
        state: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ConsumerMessage]:
        """What the consumer is doing, without bodies. ``state`` is ``processing`` (default),
        ``retrying`` (every retry) or ``waiting`` (not received yet)."""
        path = _consumer(queue, consumer, "/messages") + query({"state": state, "limit": limit})
        response = self._http.request("GET", path)
        return [from_json(ConsumerMessage, message) for message in response["messages"]]

    # Inspecting

    def messages_list(
        self,
        queue: str,
        state: Optional[str] = None,
        limit: Optional[int] = None,
        before: Optional[str] = None,
    ) -> List[MessageInfo]:
        """Browses stored messages, newest first, without consuming them. ``state`` is
        ``visible`` (default) or ``delayed``; ``before`` (a message id) pages to older ones."""
        params = {"state": state, "limit": limit, "before": before}
        response = self._http.request("GET", _queue(queue, "/messages") + query(params))
        return [from_json(MessageInfo, message) for message in response["messages"]]

    def messages_get(self, queue: str, id: str) -> MessageInfo:
        """One message including its body."""
        path = _queue(queue, "/messages/" + segment(id))
        return from_json(MessageInfo, self._http.request("GET", path))

    def messages_remove(self, queue: str, id: str) -> None:
        """Deletes the message for every consumer."""
        self._http.request("DELETE", _queue(queue, "/messages/" + segment(id)))

    def requeue(self, queue: str, ids: Optional[Sequence[str]] = None) -> Requeued:
        """Moves dead letters back to the queue and consumer that failed them: the given ids,
        or all. Call it on a dead-letter queue (``emails.dlq``)."""
        body = {} if ids is None else {"ids": list(ids)}
        return from_json(Requeued, self._http.request("POST", _queue(queue, "/requeue"), body))

    # Monitoring

    def stats(self) -> Overview:
        """Berthq-wide totals, rates, free disk space and the last minute of throughput."""
        return from_json(Overview, self._http.request("GET", "/v1/stats"))


def _queue(queue: str, rest: str = "") -> str:
    return f"/v1/queues/{segment(queue)}{rest}"


def _consumer(queue: str, consumer: str, rest: str = "") -> str:
    return _queue(queue, f"/consumers/{segment(consumer)}{rest}")


def _leases(leases: Leases) -> List[str]:
    # A lone string would otherwise be split into characters.
    return [leases] if isinstance(leases, str) else list(leases)


def _message(entry: Any) -> NewMessage:
    return entry if isinstance(entry, NewMessage) else NewMessage(entry)
