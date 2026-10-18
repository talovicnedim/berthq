import json
import threading
import time
import unittest
from concurrent.futures import Future
from typing import Any, List

from berthq_client import BerthqError, Client, NewMessage, Published
from berthq_client.publisher import Publisher

LONG = 60_000


class FakeClient:
    """``_publish_encoded`` like the broker: a ``bad`` body is invalid, a ``full`` body hits
    queueFull, a ``short`` body leaves the last result out. ``latency`` makes every batch take
    that long."""

    def __init__(self, latency: float = 0.0) -> None:
        self.batches: List[List[Any]] = []
        self.latency = latency
        self.in_flight = 0
        self.max_in_flight = 0
        self._lock = threading.Lock()

    def _publish_encoded(self, queue: str, messages: List[bytes]) -> List[Published]:
        bodies = [json.loads(message)["body"] for message in messages]
        with self._lock:
            self.batches.append(bodies)
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.latency:
                time.sleep(self.latency)
            if "bad" in bodies:
                raise BerthqError("invalid", 400, "body bad is not allowed")
            if "full" in bodies:
                raise BerthqError("queueFull", 507, "queue is full")
            results = [Published(f"id-{body}", False) for body in bodies]
            # A broker (or proxy) that answers with fewer results than messages.
            return results[:-1] if "short" in bodies else results
        finally:
            with self._lock:
                self.in_flight -= 1


def results(futures: List["Future[Published]"]) -> List[str]:
    return [future.result(timeout=5).id for future in futures]


class PublisherTests(unittest.TestCase):
    def test_a_batch_goes_out_when_max_batch_messages_wait(self) -> None:
        client = FakeClient()
        with Publisher(client, "jobs", max_batch=3, linger_ms=LONG) as publisher:  # type: ignore[arg-type]
            futures = [publisher.publish(n) for n in range(7)]
            self.assertEqual(results(futures[:6]), [f"id-{n}" for n in range(6)])
            self.assertEqual(client.batches, [[0, 1, 2], [3, 4, 5]])
            publisher.flush()
            self.assertEqual(client.batches, [[0, 1, 2], [3, 4, 5], [6]])

    def test_messages_wait_linger_ms_for_more(self) -> None:
        client = FakeClient()
        with Publisher(client, "jobs", linger_ms=50) as publisher:  # type: ignore[arg-type]
            started = time.monotonic()
            futures = [publisher.publish("a"), publisher.publish("b")]
            self.assertEqual(client.batches, [])
            self.assertEqual(results(futures), ["id-a", "id-b"])
            self.assertGreaterEqual(time.monotonic() - started, 0.04)
            self.assertEqual(client.batches, [["a", "b"]])

    def test_batches_are_cut_before_max_bytes_and_a_bigger_message_goes_alone(self) -> None:
        client = FakeClient()
        big = "x" * 100
        # {"body":"s1"} is 13 bytes, so two small messages fit in 30 and a third does not.
        with Publisher(client, "jobs", max_bytes=30, linger_ms=LONG) as publisher:  # type: ignore[arg-type]
            futures = [publisher.publish(body) for body in ["s1", "s2", "s3", big, "s4"]]
            publisher.flush()
            results(futures)
        self.assertEqual(client.batches, [["s1", "s2"], ["s3"], [big], ["s4"]])

    def test_one_batch_in_flight_keeps_the_order(self) -> None:
        client = FakeClient(latency=0.005)
        with Publisher(client, "jobs", max_batch=2, linger_ms=0) as publisher:  # type: ignore[arg-type]
            futures = [publisher.publish(n) for n in range(10)]
            self.assertEqual(results(futures), [f"id-{n}" for n in range(10)])
        self.assertEqual(client.max_in_flight, 1)
        self.assertEqual([body for batch in client.batches for body in batch], list(range(10)))

    def test_an_invalid_batch_is_split_until_the_bad_message_is_alone(self) -> None:
        client = FakeClient()
        with Publisher(client, "jobs", linger_ms=LONG) as publisher:  # type: ignore[arg-type]
            first, bad, last = [publisher.publish(body) for body in ["a", "bad", "c"]]
            publisher.flush()
        self.assertEqual(first.result().id, "id-a")
        error = bad.exception()
        assert isinstance(error, BerthqError)
        self.assertEqual((error.kind, error.status), ("invalid", 400))
        self.assertEqual(last.result().id, "id-c")
        self.assertEqual(client.batches, [["a", "bad", "c"], ["a"], ["bad", "c"], ["bad"], ["c"]])

    def test_other_errors_fail_the_whole_batch(self) -> None:
        client = FakeClient()
        with Publisher(client, "jobs", linger_ms=LONG) as publisher:  # type: ignore[arg-type]
            futures = [publisher.publish(body) for body in ["x", "full"]]
        for future in futures:
            error = future.exception()
            assert isinstance(error, BerthqError)
            self.assertEqual(error.kind, "queueFull")
        self.assertEqual(client.batches, [["x", "full"]])

    def test_a_batch_answer_with_too_few_results_fails_every_message_of_it(self) -> None:
        client = FakeClient()
        with Publisher(client, "jobs", linger_ms=LONG) as publisher:  # type: ignore[arg-type]
            futures = [publisher.publish(body) for body in ["a", "short"]]
        for future in futures:
            error = future.exception(timeout=2)
            assert isinstance(error, BerthqError)
            self.assertEqual(error.kind, "http")
            self.assertIn("1 results for 2 messages", error.message)

    def test_close_flushes_and_refuses_new_messages(self) -> None:
        client = FakeClient()
        publisher = Publisher(client, "jobs", linger_ms=LONG)  # type: ignore[arg-type]
        future = publisher.publish("last", priority=5)
        publisher.close()
        self.assertEqual(future.result().id, "id-last")
        with self.assertRaises(BerthqError) as caught:
            publisher.publish("late")
        self.assertEqual(caught.exception.kind, "invalid")
        self.assertEqual(client.batches, [["last"]])

    def test_a_cancelled_message_is_left_out(self) -> None:
        client = FakeClient()
        with Publisher(client, "jobs", linger_ms=LONG) as publisher:  # type: ignore[arg-type]
            kept = publisher.publish("kept")
            dropped = publisher.publish("dropped")
            self.assertTrue(dropped.cancel())
            publisher.flush()
        self.assertEqual(kept.result().id, "id-kept")
        self.assertEqual(client.batches, [["kept"]])

    def test_a_message_object_keeps_its_options(self) -> None:
        sent: List[Any] = []

        class Recording(FakeClient):
            def _publish_encoded(self, queue: str, messages: List[bytes]) -> List[Published]:
                sent.extend(json.loads(message) for message in messages)
                return super()._publish_encoded(queue, messages)

        with Publisher(Recording(), "jobs", linger_ms=0) as publisher:  # type: ignore[arg-type]
            publisher.publish(NewMessage("job", priority=9, dedup_id="d1")).result(timeout=5)
        self.assertEqual((sent[0]["priority"], sent[0]["dedupId"]), (9, "d1"))

    def test_messages_are_plain_utf8_without_needless_escapes(self) -> None:
        sent: List[bytes] = []

        class Recording(FakeClient):
            def _publish_encoded(self, queue: str, messages: List[bytes]) -> List[Published]:
                sent.extend(messages)
                return super()._publish_encoded(queue, messages)

        with Publisher(Recording(), "jobs", linger_ms=0) as publisher:  # type: ignore[arg-type]
            publisher.publish({"text": "é</>&"}).result(timeout=5)
        self.assertEqual(sent, ['{"body":{"text":"é</>&"}}'.encode("utf-8")])

    def test_client_publisher_uses_the_publishers_own_defaults(self) -> None:
        publisher = Client("http://127.0.0.1:1").publisher("jobs")
        try:
            self.assertEqual((publisher._max_batch, publisher._linger), (1000, 0))
        finally:
            publisher.close()


if __name__ == "__main__":
    unittest.main()
