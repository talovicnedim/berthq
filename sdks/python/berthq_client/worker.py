"""``process()``: a worker loop around consume, ack and nack.

It is synchronous on purpose: handlers run one at a time on the calling thread, and the only
other thread keeps leases alive. Run several workers (threads or processes) for parallelism.
Finished messages are acked with the next consume call, so a busy worker needs one request per
batch.
"""

import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from .errors import BerthqError
from .types import Delivery

if TYPE_CHECKING:
    from .client import Client

log = logging.getLogger("berthq_client")

# Worth retrying after a pause: the broker is restarting (`unavailable`), unreachable, or a proxy
# in front of it answered with something that isn't the API (`http`).
RETRYABLE_KINDS = ("unavailable", "http")
BACKOFF_SECONDS = 1.0
# How often the lease keeper looks for leases to extend. Checking is free (no request), so this
# is short to cope with short visibility timeouts.
KEEPER_TICK_SECONDS = 0.2
# The broker takes up to 1000 leases per ack or extend request.
MAX_LEASES = 1000


def process(
    client: "Client",
    queue: str,
    handler: Callable[[Delivery], Any],
    *,
    batch: int = 10,
    consumer: Optional[str] = None,
    from_: Optional[str] = None,
    worker: Optional[str] = None,
    visibility_timeout_ms: Optional[int] = None,
    auto_ack: bool = False,
    stop_event: Optional[threading.Event] = None,
    wait_ms: int = 20_000,
) -> None:
    """Long-polls ``queue`` for ``consumer`` (default ``default``, created at ``from_`` if it
    doesn't exist) and calls ``handler(delivery)`` for each message: acks it when the handler
    returns, nacks it with ``str(exc)`` when the handler raises (the broker then retries it with
    backoff or dead-letters it). Acks go out with the next consume call, and the last ones on
    their own when it stops. Leases are extended in the background until a message is settled,
    so it isn't redelivered while it is still being worked on. ``worker`` is the name shown in
    the dashboard. ``visibility_timeout_ms`` is the lease length (default: the queue's setting);
    as leases are extended, it only bounds how long a crashed worker holds a message.

    With ``auto_ack`` nothing is leased, acked or retried: the position moves when messages are
    delivered, and a handler that raises only logs a warning.

    Returns when ``stop_event`` is set, after finishing the messages it already received; a
    long-poll in progress ends within ``wait_ms``. Without ``stop_event`` it runs forever.
    Errors other than ``unavailable`` and connection failures are raised.
    """
    stop = stop_event or threading.Event()
    keeper = LeaseKeeper(client, queue)
    keeper.start()
    acks: List[str] = []  # leases of finished messages, acked with the next consume
    try:
        while not stop.is_set():
            sending = acks[:MAX_LEASES]
            try:
                consumed = client.consume(
                    queue,
                    consumer=consumer,
                    from_=from_,
                    max=batch,
                    wait_ms=wait_ms,
                    visibility_timeout_ms=visibility_timeout_ms,
                    auto_ack=auto_ack,
                    worker=worker,
                    ack=sending,
                )
            except BerthqError as err:
                if err.kind not in RETRYABLE_KINDS:
                    raise
                log.warning("berthq_client: %s; retrying in %ss", err, BACKOFF_SECONDS)
                stop.wait(BACKOFF_SECONDS)
                continue
            _acked(keeper, acks, sending)
            for delivery in consumed.messages:
                keeper.watch(delivery)
            # The whole batch is handled even after a stop: these messages are leased to us,
            # and abandoning them would cost each one an attempt.
            for delivery in consumed.messages:
                _handle(client, keeper, handler, delivery, acks)
    finally:
        _flush_acks(client, queue, keeper, acks)
        keeper.stop()


def _handle(
    client: "Client",
    keeper: "LeaseKeeper",
    handler: Callable[[Delivery], Any],
    delivery: Delivery,
    acks: List[str],
) -> None:
    lease = delivery.lease
    try:
        handler(delivery)
    except Exception as exc:
        error = str(exc) or type(exc).__name__
        log.warning("berthq_client: message %s failed: %s", delivery.id, error)
        if lease is None:
            return  # auto-acked: done for this consumer, nothing to retry
        keeper.forget(lease)
        try:
            client.nack(delivery.queue, lease, error=error)
        except BerthqError as err:
            # The lease runs out and the message is redelivered (at-least-once), so a broker
            # that is briefly out of reach is logged; the rest of the batch still runs.
            if err.kind not in RETRYABLE_KINDS:
                raise
            log.warning("berthq_client: could not nack message %s: %s", delivery.id, err)
    else:
        # The lease stays watched until the ack has gone out with the next consume.
        if lease is not None:
            acks.append(lease)


def _acked(keeper: "LeaseKeeper", acks: List[str], sent: List[str]) -> None:
    """The broker has these acks: stop extending their leases."""
    for lease in sent:
        keeper.forget(lease)
    del acks[: len(sent)]


def _flush_acks(client: "Client", queue: str, keeper: "LeaseKeeper", acks: List[str]) -> None:
    """Sends the acks that didn't go out with a consume. A lease that can't be acked runs out
    and its message is redelivered (at-least-once), so a failure is logged, not raised."""
    while acks:
        sending = acks[:MAX_LEASES]
        try:
            client.ack(queue, sending)
        except BerthqError as err:
            log.warning("berthq_client: could not ack %d messages: %s", len(sending), err)
        _acked(keeper, acks, sending)


class LeaseKeeper:
    """Extends the leases of messages that are waiting for or running in the handler, once half
    of their lease has passed. The leases due at a tick go out together, in one request per lease
    length (usually one)."""

    def __init__(self, client: "Client", queue: str) -> None:
        self._client = client
        self._queue = queue
        self._lock = threading.Lock()
        # lease -> (lease length in seconds, monotonic time to extend at)
        self._leases: Dict[str, Tuple[float, float]] = {}
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"berthq-lease-keeper-{queue}", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()
        self._thread.join()

    def watch(self, delivery: Delivery) -> None:
        if delivery.lease is None:
            return  # auto-acked: nothing to keep alive
        # The lease length comes from the broker rather than from comparing leaseExpiresAt
        # with the local clock, which may be skewed.
        length = delivery.visibility_timeout_ms / 1000
        with self._lock:
            self._leases[delivery.lease] = (length, time.monotonic() + length / 2)

    def forget(self, lease: str) -> None:
        with self._lock:
            self._leases.pop(lease, None)

    def _run(self) -> None:
        while not self._stopped.wait(KEEPER_TICK_SECONDS):
            for length, leases in self._due().items():
                for start in range(0, len(leases), MAX_LEASES):
                    self._extend(leases[start : start + MAX_LEASES], length)

    def _due(self) -> Dict[float, List[str]]:
        """The leases to extend now, by lease length: one request asks for one length."""
        now = time.monotonic()
        due: Dict[float, List[str]] = {}
        with self._lock:
            for lease, (length, at) in self._leases.items():
                if at <= now:
                    due.setdefault(length, []).append(lease)
        return due

    def _extend(self, leases: List[str], length: float) -> None:
        try:
            # Ask for the same lease length again rather than the queue's current setting, so
            # the next extension is due at the time we expect.
            settled = self._client.extend(self._queue, leases, round(length * 1000))
            gone = set(settled.unknown)
            next_try = time.monotonic() + length / 2
        except BerthqError as err:
            log.warning("berthq_client: could not extend %d leases: %s", len(leases), err)
            gone = set()
            next_try = time.monotonic() + BACKOFF_SECONDS
        with self._lock:
            for lease in leases:
                if lease not in self._leases:
                    continue  # settled meanwhile
                if lease in gone:
                    del self._leases[lease]
                else:
                    self._leases[lease] = (length, next_try)
