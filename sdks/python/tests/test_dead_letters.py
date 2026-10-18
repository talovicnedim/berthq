import unittest

from support import BerthqTestCase


class DeadLetterTests(BerthqTestCase):
    def test_nack_retries_then_dead_letters_and_requeue_brings_it_back(self) -> None:
        name = self.name()
        self.client.queues_create(name, max_attempts=2, retry_delay_ms=100)
        published = self.client.publish(name, {"job": 1})

        [first] = self.client.consume(name, wait_ms=1000).messages
        self.client.nack(name, self.lease(first), error="smtp timeout")
        [second] = self.client.consume(name, wait_ms=5000).messages
        self.assertEqual((second.id, second.attempt), (published.id, 2))
        self.assertEqual(second.last_error, "smtp timeout")
        self.client.nack(name, self.lease(second), error="smtp timeout again")

        dlq = name + ".dlq"
        self.assertEqual(self.client.queues_get(name).dead_letter_queue, dlq)
        # A dead letter is a copy, so it is looked up in the DLQ rather than by the original id.
        [dead] = self.client.messages_list(dlq)
        assert dead.dead_letter is not None
        self.assertEqual(dead.dead_letter.reason, "maxAttempts")
        self.assertEqual((dead.dead_letter.queue, dead.dead_letter.consumer), (name, "default"))
        self.assertEqual(dead.dead_letter.message_id, published.id)
        self.assertEqual((dead.attempts, dead.last_error), (2, "smtp timeout again"))
        self.assertEqual(self.client.messages_get(dlq, dead.id).body, {"job": 1})

        requeued = self.client.requeue(dlq)
        self.assertEqual((requeued.requeued, requeued.skipped), (1, 0))
        [again] = self.client.consume(name, wait_ms=1000).messages
        self.assertEqual((again.id, again.body, again.attempt), (published.id, {"job": 1}, 1))

    def test_nack_with_dead_letter_skips_remaining_attempts(self) -> None:
        name = self.name()
        self.client.queues_create(name, max_attempts=5)
        self.client.publish(name, "poison")
        [delivery] = self.client.consume(name).messages
        self.client.nack(name, [self.lease(delivery)], error="bad input", dead_letter=True)
        [dead] = self.client.messages_list(name + ".dlq")
        assert dead.dead_letter is not None
        self.assertEqual(dead.dead_letter.reason, "rejected")
        self.assertEqual(self.client.requeue(name + ".dlq", ids=[dead.id]).requeued, 1)

    def test_a_dead_letter_is_requeued_to_the_consumer_that_failed_it(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish(name, "job")
        self.assertEqual(self.drain(name, "audit"), ["job"])
        [delivery] = self.client.consume(name, consumer="billing").messages
        self.client.nack(name, self.lease(delivery), dead_letter=True)

        self.client.requeue(name + ".dlq")
        self.assertEqual(self.drain(name, "billing"), ["job"])
        self.assertEqual(self.drain(name, "audit"), [])


if __name__ == "__main__":
    unittest.main()
