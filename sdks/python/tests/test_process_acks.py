import threading
import time
import unittest
from collections import deque
from typing import Any, Deque, List, Optional, Sequence, Union

from berthq_client import Consumed, Delivery, Settled
from berthq_client.worker import LeaseKeeper, process


def delivery(lease: str, visibility_timeout_ms: int = 30_000) -> Delivery:
    return Delivery(
        id=f"id-{lease}",
        queue="jobs",
        consumer="default",
        body=lease,
        priority=0,
        attempt=1,
        published_at="2026-10-14T09:00:00.000Z",
        visibility_timeout_ms=visibility_timeout_ms,
        lease=lease,
    )


class FakeClient:
    """Hands out the given batches, one per consume call, and records what comes back."""

    def __init__(self, batches: List[List[Delivery]]) -> None:
        self.batches: Deque[List[Delivery]] = deque(batches)
        self.consume_acks: List[List[str]] = []
        self.acked: List[List[str]] = []
        self.nacked: List[str] = []
        self.extended: List[List[str]] = []
        self.extended_some = threading.Event()

    def consume(self, queue: str, ack: Optional[Sequence[str]] = None, **options: Any) -> Consumed:
        sent = list(ack or [])
        self.consume_acks.append(sent)
        messages = self.batches.popleft() if self.batches else []
        return Consumed(messages=messages, settled=len(sent))

    def ack(self, queue: str, leases: Sequence[str]) -> Settled:
        self.acked.append(list(leases))
        return Settled(len(leases))

    def nack(self, queue: str, lease: str, **options: Any) -> Settled:
        self.nacked.append(lease)
        return Settled(1)

    def extend(
        self, queue: str, leases: Union[str, Sequence[str]], visibility_timeout_ms: Optional[int] = None
    ) -> Settled:
        sent = [leases] if isinstance(leases, str) else list(leases)
        self.extended.append(sent)
        self.extended_some.set()
        return Settled(len(sent))

    def extended_leases(self) -> int:
        return sum(len(leases) for leases in self.extended)


class ProcessAckTests(unittest.TestCase):
    def test_acks_go_with_the_next_consume_and_the_rest_on_their_own(self) -> None:
        client = FakeClient([[delivery("L1")], [delivery("L2")]])
        stop = threading.Event()
        seen: List[Any] = []

        def handler(message: Delivery) -> None:
            seen.append(message.body)
            if len(seen) == 2:
                stop.set()

        process(client, "jobs", handler, batch=1, stop_event=stop, wait_ms=0)  # type: ignore[arg-type]

        self.assertEqual(seen, ["L1", "L2"])
        self.assertEqual(client.consume_acks, [[], ["L1"]])
        self.assertEqual(client.acked, [["L2"]])

    def test_failures_are_nacked_right_away(self) -> None:
        client = FakeClient([[delivery("L1")]])
        stop = threading.Event()

        def handler(message: Delivery) -> None:
            stop.set()
            raise ValueError("boom")

        with self.assertLogs("berthq_client", "WARNING"):
            process(client, "jobs", handler, stop_event=stop, wait_ms=0)  # type: ignore[arg-type]

        self.assertEqual(client.nacked, ["L1"])
        self.assertEqual(client.acked, [])


class LeaseKeeperTests(unittest.TestCase):
    def test_one_extend_request_carries_every_lease_that_is_due(self) -> None:
        batch = [delivery(lease, visibility_timeout_ms=200) for lease in ["L1", "L2", "L3"]]
        client = FakeClient([batch])
        stop = threading.Event()

        def handler(message: Delivery) -> None:
            stop.set()
            if message.lease == "L1":
                self.assertTrue(client.extended_some.wait(5), "no lease was extended")

        process(client, "jobs", handler, batch=3, stop_event=stop, wait_ms=0)  # type: ignore[arg-type]

        self.assertEqual(client.extended[0], ["L1", "L2", "L3"])

    def test_extend_requests_carry_at_most_1000_leases(self) -> None:
        client = FakeClient([])
        keeper = LeaseKeeper(client, "jobs")  # type: ignore[arg-type]
        for n in range(1001):
            keeper.watch(delivery(f"L{n}", visibility_timeout_ms=200))
        keeper.start()
        try:
            deadline = time.monotonic() + 5
            while client.extended_leases() < 1001 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            keeper.stop()
        self.assertEqual([len(leases) for leases in client.extended[:2]], [1000, 1])


if __name__ == "__main__":
    unittest.main()
