import unittest

from support import BerthqTestCase

from berthq_client import NewMessage


class PublishConsumeTests(BerthqTestCase):
    def test_round_trip(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        published = self.client.publish(name, {"user": 1}, dedup_id="user-1")
        self.assertFalse(published.duplicate)

        [delivery] = self.client.consume(name, max=10, wait_ms=1000, worker="tests").messages
        self.assertEqual(delivery.id, published.id)
        self.assertEqual(delivery.body, {"user": 1})
        self.assertEqual(delivery.dedup_id, "user-1")
        self.assertEqual(delivery.attempt, 1)
        self.assertEqual((delivery.queue, delivery.consumer), (name, "default"))
        self.assertEqual(delivery.visibility_timeout_ms, 30_000)
        assert delivery.lease is not None

        self.assertEqual(self.client.extend(name, delivery.lease, 10_000).settled, 1)
        settled = self.client.ack(name, [delivery.lease])
        self.assertEqual((settled.settled, settled.unknown), (1, []))
        # A lease can only be settled once.
        self.assertEqual(self.client.ack(name, delivery.lease).unknown, [delivery.lease])
        self.assertEqual(self.client.queues_get(name).stats.backlog, 0)

    def test_consume_acks_the_given_leases_first(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, [1, 2])
        [first] = self.client.consume(name).messages
        lease = self.lease(first)

        following = self.client.consume(name, ack=[lease])
        self.assertEqual((following.settled, following.unknown), (1, []))
        self.assertEqual([delivery.body for delivery in following.messages], [2])
        again = self.client.consume(name, ack=[lease])
        self.assertEqual((again.messages, again.settled, again.unknown), ([], 0, [lease]))

    def test_a_publisher_publishes_every_message_once_in_order(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        with self.client.publisher(name) as publisher:
            futures = [publisher.publish(n) for n in range(250)]
        self.assertEqual(len({future.result().id for future in futures}), 250)

        bodies = []
        while True:
            messages = self.client.consume(name, max=100, auto_ack=True).messages
            if not messages:
                break
            bodies.extend(delivery.body for delivery in messages)
        self.assertEqual(bodies, list(range(250)))

    def test_auto_ack_moves_the_position_without_a_lease(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, ["a", "b"])
        deliveries = self.client.consume(name, max=10, auto_ack=True).messages
        self.assertEqual(
            [(d.body, d.lease, d.visibility_timeout_ms) for d in deliveries],
            [("a", None, 0), ("b", None, 0)],
        )
        self.assertEqual(self.client.consume(name).messages, [])
        stats = self.client.consumers_get(name, "default").stats
        self.assertEqual((stats.waiting, stats.processing, stats.retrying), (0, 0, 0))

    def test_higher_priority_is_delivered_first_and_delays_hide_messages(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish(name, "low")
        self.client.publish(name, "high", priority=9)
        self.client.publish(name, "later", delay_ms=60_000)
        bodies = [delivery.body for delivery in self.client.consume(name, max=10).messages]
        self.assertEqual(bodies, ["high", "low"])
        [delayed] = self.client.messages_list(name, state="delayed")
        self.assertIsNotNone(delayed.deliver_at)

    def test_a_repeated_dedup_id_is_not_stored_twice(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        first = self.client.publish(name, "a", dedup_id="order-1")
        second = self.client.publish(name, "b", dedup_id="order-1")
        self.assertTrue(second.duplicate)
        self.assertEqual(second.id, first.id)
        self.assertEqual(self.client.queues_get(name).stats.messages, 1)

    def test_batch_publishes_messages_and_plain_bodies_in_order(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        published = self.client.publish_batch(
            name, [{"n": 1}, NewMessage({"n": 2}, priority=5), None, NewMessage("dup", dedup_id="d")]
        )
        self.assertEqual(len(published), 4)
        bodies = [delivery.body for delivery in self.client.consume(name, max=10).messages]
        self.assertEqual(bodies, [{"n": 2}, {"n": 1}, None, "dup"])
        again = self.client.publish_batch(name, [NewMessage("dup", dedup_id="d")])
        self.assertTrue(again[0].duplicate)

    def test_messages_can_be_browsed_fetched_and_removed(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        first = self.client.publish(name, {"secret": True}, dedup_id="peek")
        second = self.client.publish(name, "newer")
        [newest] = self.client.messages_list(name, limit=1)
        self.assertEqual((newest.id, newest.state), (second.id, "visible"))
        [older] = self.client.messages_list(name, before=newest.id)
        self.assertEqual((older.id, older.dedup_id), (first.id, "peek"))
        self.assertIsNone(older.body)
        self.assertEqual(self.client.messages_get(name, first.id).body, {"secret": True})
        self.client.messages_remove(name, first.id)
        self.assertEqual([m.id for m in self.client.messages_list(name)], [second.id])


if __name__ == "__main__":
    unittest.main()
