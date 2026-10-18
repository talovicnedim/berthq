import unittest

from support import BerthqTestCase

from berthq_client import BerthqError


class ConsumerTests(BerthqTestCase):
    def test_every_consumer_gets_every_message(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, [1, 2])
        self.assertEqual(self.drain(name, "billing"), [1, 2])
        self.assertEqual(self.drain(name, "audit"), [1, 2])
        self.assertEqual(self.drain(name, "billing"), [])
        consumers = self.client.consumers_list(name)
        self.assertEqual([consumer.name for consumer in consumers], ["audit", "billing"])
        self.assertEqual(len(self.client.queues_get(name).consumers), 2)

    def test_new_consumers_start_at_from_and_replay_moves_them(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        first = self.client.publish(name, "first")
        self.client.publish(name, "second")

        self.assertEqual(self.client.consume(name, consumer="late", from_="end").messages, [])
        self.client.publish(name, "third")
        self.assertEqual(self.drain(name, "late"), ["third"])

        replayed = self.client.consumers_replay(name, "late", "start")
        self.assertEqual(replayed.stats.waiting, 3)
        self.assertEqual(self.drain(name, "late"), ["first", "second", "third"])
        self.client.consumers_replay(name, "late", first.id)
        self.assertEqual(self.drain(name, "late"), ["first", "second", "third"])
        self.client.consumers_replay(name, "late", "end")
        self.assertEqual(self.drain(name, "late"), [])

    def test_create_get_pause_resume_and_remove_a_consumer(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        created = self.client.consumers_create(name, "billing", from_="end")
        self.assertEqual((created.queue, created.name, created.paused), (name, "billing", False))
        self.assertFalse(created.idle)
        self.assertIsNone(created.last_seen)
        with self.assertRaises(BerthqError) as caught:
            self.client.consumers_create(name, "billing")
        self.assertEqual(caught.exception.kind, "conflict")

        self.assertTrue(self.client.consumers_pause(name, "billing").paused)
        self.client.publish(name, "held")
        self.assertEqual(self.client.consume(name, consumer="billing").messages, [])
        self.assertFalse(self.client.consumers_resume(name, "billing").paused)
        [delivery] = self.client.consume(name, consumer="billing", worker="b1").messages
        self.assertEqual(delivery.body, "held")
        seen = self.client.consumers_get(name, "billing")
        self.assertIsNotNone(seen.last_seen)
        self.assertIn("b1", [worker.name for worker in seen.workers])

        self.client.consumers_remove(name, "billing")
        with self.assertRaises(BerthqError) as caught:
            self.client.consumers_get(name, "billing")
        self.assertEqual(caught.exception.kind, "notFound")
        self.assertEqual(self.client.ack(name, self.lease(delivery)).unknown, [delivery.lease])

    def test_consumer_messages_show_processing_retrying_and_waiting(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, [1, 2, 3])
        first, second = self.client.consume(name, max=2).messages
        self.client.nack(name, self.lease(first), error="later", delay_ms=60_000)

        processing = self.client.consumers_messages(name, "default", state="processing")
        self.assertEqual([m.id for m in processing], [second.id])
        [retrying] = self.client.consumers_messages(name, "default", state="retrying")
        self.assertEqual((retrying.id, retrying.last_error), (first.id, "later"))
        self.assertIsNotNone(retrying.retry_at)
        self.assertEqual(len(self.client.consumers_messages(name, "default", state="waiting")), 1)
        stats = self.client.consumers_get(name, "default").stats
        self.assertEqual((stats.waiting, stats.processing, stats.retrying), (1, 1, 1))


if __name__ == "__main__":
    unittest.main()
