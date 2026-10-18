"""``Publisher``: collects messages and publishes them in batches.

A background thread sends one batch at a time, of up to ``max_batch`` messages (and
``max_bytes``), so messages keep their order; what arrives meanwhile forms the next batch. With
the default ``linger_ms=0`` a message goes out right away; a higher value waits for more.
"""

import atexit
import functools
import threading
import time
import weakref
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Deque, List, Optional

from ._json import dumps
from .errors import BerthqError
from .types import NewMessage, Published

if TYPE_CHECKING:
    from .client import Client

# The broker takes up to 1000 messages per batch.
BATCH_LIMIT = 1000


@dataclass
class _Entry:
    seq: int
    json: bytes
    queued_at: float
    future: "Future[Published]"


class Publisher:
    """Make one with ``client.publisher(queue)``. ``publish`` returns a
    ``concurrent.futures.Future`` with the message's result once its batch is on disk. Close it
    (or use it in a ``with`` block) so the last messages are sent; at interpreter exit an open
    publisher is closed for you."""

    def __init__(
        self,
        client: "Client",
        queue: str,
        max_batch: int = BATCH_LIMIT,
        linger_ms: float = 0,
        max_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        self.queue = queue
        self._client = client
        self._max_batch = min(max(1, max_batch), BATCH_LIMIT)
        self._linger = max(0.0, linger_ms) / 1000
        self._max_bytes = max(1, max_bytes)
        self._cond = threading.Condition()
        self._waiting: Deque[_Entry] = deque()
        self._waiting_bytes = 0
        self._seq = 0
        self._done_seq = 0
        self._force_through = 0
        self._closed = False
        self._thread = threading.Thread(
            target=self._run, name=f"berthq-publisher-{queue}", daemon=True
        )
        self._thread.start()
        # The sending thread is a daemon, so without this the last batch could be lost at exit.
        self._at_exit = functools.partial(_close_at_exit, weakref.ref(self))
        atexit.register(self._at_exit)

    def publish(
        self,
        body: Any,
        *,
        priority: Optional[int] = None,
        delay_ms: Optional[int] = None,
        deliver_at: Optional[str] = None,
        dedup_id: Optional[str] = None,
    ) -> "Future[Published]":
        """Queues a message (a body, or a ``NewMessage`` with its options). The future fails with
        the error its batch got, or after an ``invalid`` batch, the error of this message alone.
        Cancelling the future before its batch is sent leaves the message out."""
        message = (
            body
            if isinstance(body, NewMessage)
            else NewMessage(body, priority, delay_ms, deliver_at, dedup_id)
        )
        encoded = dumps(message.to_json())
        future: "Future[Published]" = Future()
        with self._cond:
            if self._closed:
                raise BerthqError("invalid", None, f"the publisher for {self.queue} is closed")
            self._seq += 1
            self._waiting.append(_Entry(self._seq, encoded, time.monotonic(), future))
            self._waiting_bytes += len(encoded)
            self._cond.notify_all()
        return future

    def flush(self) -> None:
        """Sends every message queued so far without waiting for the linger, and returns once
        they are all settled. Failures go to their own futures."""
        with self._cond:
            target = self._seq
            self._force_through = max(self._force_through, target)
            self._cond.notify_all()
            while self._done_seq < target:
                self._cond.wait()

    def close(self) -> None:
        """Flushes, stops the sending thread and refuses further messages."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        self.flush()
        self._thread.join()
        atexit.unregister(self._at_exit)

    def __enter__(self) -> "Publisher":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _run(self) -> None:
        while True:
            with self._cond:
                batch = self._next_batch()
            if batch is None:
                return
            self._send(batch)
            with self._cond:
                self._done_seq = batch[-1].seq
                self._cond.notify_all()

    def _next_batch(self) -> Optional[List[_Entry]]:
        """Waits until a batch is due and takes it; ``None`` once closed with nothing left."""
        while True:
            if not self._waiting:
                if self._closed:
                    return None
                self._cond.wait()
                continue
            wait = self._linger_left()
            if wait <= 0:
                return self._take_batch()
            self._cond.wait(wait)

    def _linger_left(self) -> float:
        """0 when the next batch should go now: it is full, a flush or close asked for it, or
        its first message has waited long enough."""
        first = self._waiting[0]
        full = len(self._waiting) >= self._max_batch or self._waiting_bytes >= self._max_bytes
        if full or self._closed or first.seq <= self._force_through:
            return 0
        return first.queued_at + self._linger - time.monotonic()

    def _take_batch(self) -> List[_Entry]:
        batch: List[_Entry] = []
        size = 0
        while self._waiting and len(batch) < self._max_batch:
            entry = self._waiting[0]
            if batch and size + len(entry.json) > self._max_bytes:
                break
            batch.append(self._waiting.popleft())
            size += len(entry.json)
        self._waiting_bytes -= size
        return batch

    def _send(self, batch: List[_Entry]) -> None:
        entries = [entry for entry in batch if entry.future.set_running_or_notify_cancel()]
        if entries:
            self._publish(entries)

    def _publish(self, entries: List[_Entry]) -> None:
        """Publishes a batch. One bad message fails the whole batch, so a batch rejected as
        ``invalid`` is split in half until the bad messages are alone: only they fail, and the
        rest still go in order."""
        parts = [entries]  # still to send, the next one last
        while parts:
            part = parts.pop()
            try:
                results = self._client._publish_encoded(self.queue, [e.json for e in part])
            except BerthqError as err:
                if err.kind == "invalid" and len(part) > 1:
                    half = len(part) // 2
                    parts += [part[half:], part[:half]]
                else:
                    _fail(part, err)
                continue
            except Exception as err:  # noqa: BLE001 - the thread must live on; each future gets it
                _fail(part, err)
                continue
            if len(results) != len(part):
                message = f"the broker answered {len(results)} results for {len(part)} messages"
                _fail(part, BerthqError("http", None, message))
                continue
            for entry, result in zip(part, results):
                entry.future.set_result(result)


def _fail(entries: List[_Entry], err: BaseException) -> None:
    for entry in entries:
        entry.future.set_exception(err)


def _close_at_exit(ref: "weakref.ReferenceType[Publisher]") -> None:
    publisher = ref()
    if publisher is not None:
        publisher.close()
