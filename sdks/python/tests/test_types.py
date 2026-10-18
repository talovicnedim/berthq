import unittest

from berthq_client import BerthqError, ConsumerInfo, Delivery, NewMessage
from berthq_client._json import camel, from_json, to_json


class TypesTests(unittest.TestCase):
    def test_snake_case_becomes_camel_case(self) -> None:
        self.assertEqual(camel("visibility_timeout_ms"), "visibilityTimeoutMs")
        self.assertEqual(camel("max"), "max")
        # `from` is a Python keyword, so the SDK takes `from_`.
        self.assertEqual(to_json({"from_": "end", "auto_ack": True}), {"from": "end", "autoAck": True})

    def test_messages_leave_out_unset_options_but_always_send_the_body(self) -> None:
        self.assertEqual(NewMessage(None).to_json(), {"body": None})
        self.assertEqual(
            NewMessage({"a": 1}, priority=0, dedup_id="x").to_json(),
            {"body": {"a": 1}, "priority": 0, "dedupId": "x"},
        )

    def test_unknown_response_fields_are_ignored(self) -> None:
        delivery = from_json(
            Delivery,
            {
                "id": "1",
                "lease": "1.1.default",
                "queue": "q",
                "consumer": "default",
                "body": {"nested": {"camelCase": "kept as is"}},
                "priority": 0,
                "attempt": 1,
                "publishedAt": "2026-10-12T09:00:00.000Z",
                "leaseExpiresAt": "2026-10-12T09:00:30.000Z",
                "visibilityTimeoutMs": 30000,
                "addedInALaterVersion": True,
            },
        )
        self.assertEqual(delivery.body, {"nested": {"camelCase": "kept as is"}})
        self.assertIsNone(delivery.last_error)
        self.assertIsNone(delivery.dedup_id)

    def test_an_auto_acked_delivery_has_no_lease(self) -> None:
        delivery = from_json(
            Delivery,
            {
                "id": "1",
                "lease": None,
                "queue": "q",
                "consumer": "metrics",
                "body": 1,
                "priority": 0,
                "attempt": 1,
                "publishedAt": "2026-10-12T09:00:00.000Z",
                "leaseExpiresAt": None,
                "visibilityTimeoutMs": 0,
            },
        )
        self.assertIsNone(delivery.lease)
        self.assertIsNone(delivery.lease_expires_at)

    def test_nested_dataclasses_and_lists_of_them_are_parsed(self) -> None:
        consumer = from_json(
            ConsumerInfo,
            {
                "queue": "orders",
                "name": "billing",
                "createdAt": "2026-10-14T08:00:00.000Z",
                "paused": False,
                "stats": {"waiting": 3, "processing": 1, "retrying": 0},
                "idle": True,
                "lastSeen": None,
                "workers": [{"name": "w1", "lastSeen": "2026-10-14T09:00:00.000Z", "waiting": True}],
            },
        )
        self.assertEqual(consumer.stats.waiting, 3)
        self.assertTrue(consumer.idle)
        self.assertEqual(consumer.workers[0].name, "w1")

    def test_a_response_without_a_required_field_raises_kind_http(self) -> None:
        with self.assertRaises(BerthqError) as caught:
            from_json(Delivery, {"id": "1"})
        self.assertEqual(caught.exception.kind, "http")
        self.assertIn("visibilityTimeoutMs", caught.exception.message)


if __name__ == "__main__":
    unittest.main()
