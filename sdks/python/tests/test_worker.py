import threading
import time
import unittest
from typing import Any, Callable, List

from support import BerthqTestCase

from berthq_client import BerthqError, Client, Delivery


class Worker:
    """Runs ``client.process`` on a thread, keeping whatever it raises."""

    def __init__(self, client: Client, queue: str, handler: Callable[[Delivery], Any], **options: Any):
        self.stop = threading.Event()
        self.errors: List[BaseException] = []
        options = {"stop_event": self.stop, "wait_ms": 200, **options}
        self.thread = threading.Thread(target=self._run, args=(client, queue, handler, options))
        self.thread.start()

    def _run(self, client: Client, queue: str, handler: Callable[[Delivery], Any], options: Any) -> None:
        try:
            client.process(queue, handler, **options)
        except BaseException as err:  # noqa: BLE001 - reported by join()
            self.errors.append(err)

    def join(self) -> None:
        """Stops the worker and waits for it to return."""
        self.stop.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise AssertionError("process() did not return after stop_event was set")
        if self.errors:
            raise self.errors[0]


class ProcessTests(BerthqTestCase):
    def test_messages_are_acked_when_the_handler_returns(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, [1, 2, 3])
        seen: List[Any] = []
        done = threading.Event()

        def handler(delivery: Delivery) -> None:
            seen.append(delivery.body)
            if len(seen) == 3:
                done.set()

        worker = Worker(self.client, name, handler, worker="py-worker")
        self.assertTrue(done.wait(10))
        worker.join()
        self.assertEqual(seen, [1, 2, 3])
        stats = self.client.queues_get(name).stats
        self.assertEqual((stats.backlog, stats.counters.acked), (0, 3))

    def test_a_named_consumer_works_next_to_other_consumers(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, ["a", "b"])
        seen: List[Any] = []
        done = threading.Event()

        def handler(delivery: Delivery) -> None:
            seen.append((delivery.consumer, delivery.body))
            if len(seen) == 2:
                done.set()

        worker = Worker(self.client, name, handler, consumer="billing")
        self.assertTrue(done.wait(10))
        worker.join()
        self.assertEqual(seen, [("billing", "a"), ("billing", "b")])
        self.assertEqual(len(self.client.consume(name, consumer="audit", max=10).messages), 2)

    def test_auto_ack_settles_nothing_and_logs_handler_errors(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, ["ok", "boom"])
        done = threading.Event()

        def handler(delivery: Delivery) -> None:
            if delivery.body == "boom":
                done.set()
                raise ValueError("it exploded")

        with self.assertLogs("berthq_client", "WARNING") as logs:
            worker = Worker(self.client, name, handler, auto_ack=True)
            self.assertTrue(done.wait(10))
            worker.join()
        self.assertIn("it exploded", logs.output[0])
        stats = self.client.consumers_get(name, "default").stats
        self.assertEqual((stats.waiting, stats.processing, stats.retrying), (0, 0, 0))
        self.assertIsNone(self.client.queues_get(name).dead_letter_queue)

    def test_a_raising_handler_nacks_with_the_error_and_the_message_is_retried(self) -> None:
        name = self.name()
        self.client.queues_create(name, retry_delay_ms=100)
        self.client.publish(name, "flaky")
        retried: List[Delivery] = []
        done = threading.Event()

        def handler(delivery: Delivery) -> None:
            if delivery.attempt == 1:
                raise ValueError("smtp timeout")
            retried.append(delivery)
            done.set()

        with self.assertLogs("berthq_client", "WARNING") as logs:
            worker = Worker(self.client, name, handler)
            self.assertTrue(done.wait(10))
            worker.join()
        self.assertIn("smtp timeout", logs.output[0])
        self.assertEqual(retried[0].last_error, "smtp timeout")
        stats = self.client.queues_get(name).stats
        self.assertEqual((stats.backlog, stats.counters.retried, stats.counters.acked), (0, 1, 1))

    def test_leases_of_slow_handlers_are_extended(self) -> None:
        name = self.name()
        self.client.queues_create(name, visibility_timeout_ms=1000, retry_delay_ms=100)
        self.client.publish_batch(name, ["a", "b"])
        attempts: List[int] = []
        done = threading.Event()

        def handler(delivery: Delivery) -> None:
            # Each takes longer than the lease, and "b" waits for "a" first.
            time.sleep(1.5)
            attempts.append(delivery.attempt)
            if len(attempts) == 2:
                done.set()

        worker = Worker(self.client, name, handler, batch=10)
        self.assertTrue(done.wait(15))
        worker.join()
        self.assertEqual(attempts, [1, 1])
        stats = self.client.queues_get(name).stats
        self.assertEqual((stats.counters.retried, stats.counters.acked), (0, 2))

    def test_visibility_timeout_ms_sets_the_lease_length(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish(name, "job")
        lengths: List[int] = []
        done = threading.Event()

        def handler(delivery: Delivery) -> None:
            lengths.append(delivery.visibility_timeout_ms)
            done.set()

        worker = Worker(self.client, name, handler, visibility_timeout_ms=5000)
        self.assertTrue(done.wait(10))
        worker.join()
        self.assertEqual(lengths, [5000])

    def test_process_returns_promptly_when_stopped_while_idle(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        worker = Worker(self.client, name, lambda delivery: None)
        time.sleep(0.3)
        started = time.monotonic()
        worker.join()
        self.assertLess(time.monotonic() - started, 2)

    def test_errors_that_retrying_cannot_fix_are_raised(self) -> None:
        with self.assertRaises(BerthqError) as caught:
            self.client.process(self.name("missing"), lambda delivery: None, wait_ms=0)
        self.assertEqual(caught.exception.kind, "notFound")


class BackoffTests(unittest.TestCase):
    def test_an_unreachable_broker_is_retried_until_stopped(self) -> None:
        client = Client("http://127.0.0.1:9", timeout=2)
        with self.assertLogs("berthq_client", "WARNING") as logs:
            worker = Worker(client, "emails", lambda delivery: None)
            time.sleep(0.5)
            worker.join()
        self.assertIn("could not connect", logs.output[0])


if __name__ == "__main__":
    unittest.main()
