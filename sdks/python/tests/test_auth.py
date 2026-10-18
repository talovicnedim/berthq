import unittest

from support import Berthq

from berthq_client import BerthqError, Client

ADMIN = "adminadminadmin1"
PUBLISH = "publishpublish12"


class AuthTests(unittest.TestCase):
    broker: Berthq

    @classmethod
    def setUpClass(cls) -> None:
        cls.broker = Berthq(api_keys=[ADMIN, f"publish:{PUBLISH}"])

    @classmethod
    def tearDownClass(cls) -> None:
        cls.broker.stop()

    def test_an_admin_key_can_do_everything(self) -> None:
        admin = Client(self.broker.url, api_key=ADMIN)
        admin.queues_create("auth-admin")
        admin.publish("auth-admin", "hi")
        [delivery] = admin.consume("auth-admin").messages
        assert delivery.lease is not None
        self.assertEqual(admin.ack("auth-admin", delivery.lease).settled, 1)

    def test_a_missing_key_is_unauthorized(self) -> None:
        with self.assertRaises(BerthqError) as caught:
            Client(self.broker.url).queues_list()
        self.assertEqual((caught.exception.kind, caught.exception.status), ("unauthorized", 401))

    def test_an_unknown_key_is_unauthorized(self) -> None:
        with self.assertRaises(BerthqError) as caught:
            Client(self.broker.url, api_key="wrongwrongwrong1").queues_list()
        self.assertEqual((caught.exception.kind, caught.exception.status), ("unauthorized", 401))

    def test_a_key_without_the_scope_is_forbidden(self) -> None:
        Client(self.broker.url, api_key=ADMIN).queues_create("auth-scoped")
        publisher = Client(self.broker.url, api_key=PUBLISH)
        self.assertFalse(publisher.publish("auth-scoped", "allowed").duplicate)
        with self.assertRaises(BerthqError) as caught:
            publisher.consume("auth-scoped").messages
        self.assertEqual((caught.exception.kind, caught.exception.status), ("forbidden", 403))

    def test_managing_consumers_needs_an_admin_key(self) -> None:
        admin = Client(self.broker.url, api_key=ADMIN)
        admin.queues_create("auth-consumers")
        with self.assertRaises(BerthqError) as caught:
            Client(self.broker.url, api_key=PUBLISH).consumers_create("auth-consumers", "billing")
        self.assertEqual((caught.exception.kind, caught.exception.status), ("forbidden", 403))
        self.assertEqual(admin.consumers_create("auth-consumers", "billing").name, "billing")


if __name__ == "__main__":
    unittest.main()
