import unittest

from support import BerthqTestCase

from berthq_client import BerthqError, NewMessage, QueueSettings


class QueueTests(BerthqTestCase):
    def test_create_get_update_and_remove_a_queue(self) -> None:
        name = self.name()
        created = self.client.queues_create(
            name, max_attempts=3, visibility_timeout_ms=5000, retention_ms=60_000
        )
        self.assertEqual(created.name, name)
        self.assertEqual(
            created.settings,
            QueueSettings(
                retention_ms=60_000, visibility_timeout_ms=5000, max_attempts=3, retry_delay_ms=1000
            ),
        )

        updated = self.client.queues_update(name, max_messages=100, retry_delay_ms=500)
        self.assertEqual((updated.settings.max_messages, updated.settings.retry_delay_ms), (100, 500))
        # 0 removes the limit.
        self.assertIsNone(self.client.queues_update(name, max_messages=0).settings.max_messages)
        self.assertIn(name, [queue.name for queue in self.client.queues_list()])

        details = self.client.queues_get(name)
        self.assertEqual(details.stats.messages, 0)
        self.assertEqual(details.consumers, [])

        self.client.queues_remove(name)
        with self.assertRaises(BerthqError) as caught:
            self.client.queues_get(name)
        self.assertEqual((caught.exception.kind, caught.exception.status), ("notFound", 404))

    def test_creating_an_existing_queue_is_a_conflict(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        with self.assertRaises(BerthqError) as caught:
            self.client.queues_create(name)
        self.assertEqual((caught.exception.kind, caught.exception.status), ("conflict", 409))

    def test_invalid_input_raises_invalid(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        with self.assertRaises(BerthqError) as caught:
            self.client.publish(name, "x", priority=10)
        self.assertEqual((caught.exception.kind, caught.exception.status), ("invalid", 400))
        self.assertIn("priority", caught.exception.message)

    def test_pause_resume_and_purge(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, [1, 2, 3])
        self.assertTrue(self.client.queues_pause(name).settings.paused)
        self.assertEqual(self.client.consume(name, max=10).messages, [])
        self.assertFalse(self.client.queues_resume(name).settings.paused)
        self.assertEqual(self.client.queues_purge(name), 3)

    def test_acked_messages_stay_for_replay_but_leave_the_backlog(self) -> None:
        name = self.name()
        self.client.queues_create(name)
        self.client.publish_batch(name, [1, 2, NewMessage(3, delay_ms=60_000)])
        stats = self.client.queues_get(name).stats
        self.assertEqual((stats.messages, stats.backlog, stats.delayed), (3, 2, 1))

        self.assertEqual(self.drain(name, "default"), [1, 2])
        stats = self.client.queues_get(name).stats
        self.assertEqual((stats.messages, stats.backlog, stats.consumers), (3, 0, 1))

    def test_stats_count_queues_and_show_free_disk_space(self) -> None:
        self.client.queues_create(self.name())
        stats = self.client.stats()
        self.assertGreaterEqual(stats.queues, 1)
        self.assertTrue(stats.version)
        self.assertGreater(stats.disk.free, 0)
        self.assertGreater(stats.disk.min_free, 0)
        self.assertFalse(stats.disk.full)


if __name__ == "__main__":
    unittest.main()
