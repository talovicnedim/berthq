import http.client
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List, Tuple

from berthq_client import BerthqError, Client
from berthq_client._http import Transport


class Recorder(ThreadingHTTPServer):
    """Answers every request with ``{"ok": true}`` and notes which connection it came on."""

    def __init__(self, close_after_each: bool = False) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.close_after_each = close_after_each
        self.requests: List[Tuple[int, str]] = []
        self.bodies: List[bytes] = []
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"

    def ports(self) -> int:
        return len({port for port, _ in self.requests})

    def stop(self) -> None:
        self.shutdown()
        self.server_close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keeps connections open
    server: Recorder

    def do_GET(self) -> None:
        self._answer()

    def do_POST(self) -> None:
        self._answer()

    def _answer(self) -> None:
        self.server.bodies.append(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        self.server.requests.append((self.client_address[1], self.path))
        if self.path.endswith("/missing"):
            self._send(404, b'{"error": {"kind": "notFound", "message": "queue missing not found"}}')
        else:
            self._send(200, b'{"ok": true}')
        # Closing without saying so is what an idle-timeout on the other side looks like.
        self.close_connection = self.server.close_after_each

    def _send(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


class TransportTests(unittest.TestCase):
    def test_requests_share_one_kept_alive_connection(self) -> None:
        server = Recorder()
        self.addCleanup(server.stop)
        transport = Transport(server.url, None, 5)
        self.addCleanup(transport.close)
        for _ in range(3):
            self.assertEqual(transport.request("POST", "/v1/queues/jobs/consume", {"max": 1}), {"ok": True})
        self.assertEqual((len(server.requests), server.ports()), (3, 1))

    def test_a_connection_the_broker_closed_is_replaced(self) -> None:
        server = Recorder(close_after_each=True)
        self.addCleanup(server.stop)
        transport = Transport(server.url, None, 5)
        self.addCleanup(transport.close)
        for _ in range(3):
            self.assertEqual(transport.request("GET", "/v1/stats"), {"ok": True})
        self.assertEqual((len(server.requests), server.ports()), (3, 3))

    def test_each_thread_uses_its_own_connection(self) -> None:
        server = Recorder()
        self.addCleanup(server.stop)
        transport = Transport(server.url, None, 5)
        self.addCleanup(transport.close)
        threads = [threading.Thread(target=transport.request, args=("GET", "/v1/stats")) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(server.ports(), 2)

    def test_a_path_prefix_is_kept(self) -> None:
        server = Recorder()
        self.addCleanup(server.stop)
        transport = Transport(server.url + "/berthq/", None, 5)
        self.addCleanup(transport.close)
        transport.request("GET", "/v1/stats")
        self.assertEqual(server.requests[0][1], "/berthq/v1/stats")

    def test_bodies_are_plain_utf8_without_needless_escapes(self) -> None:
        server = Recorder()
        self.addCleanup(server.stop)
        transport = Transport(server.url, None, 5)
        self.addCleanup(transport.close)
        transport.request("POST", "/v1/queues/jobs/messages", {"body": {"text": "é</>&"}})
        self.assertEqual(server.bodies[0], '{"body":{"text":"é</>&"}}'.encode("utf-8"))

    def test_api_errors_keep_their_kind_and_status(self) -> None:
        server = Recorder()
        self.addCleanup(server.stop)
        transport = Transport(server.url, None, 5)
        self.addCleanup(transport.close)
        with self.assertRaises(BerthqError) as caught:
            transport.request("GET", "/v1/queues/missing")
        self.assertEqual((caught.exception.kind, caught.exception.status), ("notFound", 404))

    def test_https_urls_use_tls(self) -> None:
        connection = Transport("https://berthq.example.com", None, 5)._open(5)
        self.assertIsInstance(connection, http.client.HTTPSConnection)

    def test_a_url_that_is_not_http_is_invalid(self) -> None:
        with self.assertRaises(BerthqError) as caught:
            Transport("ftp://example.com", None, 5)
        self.assertEqual(caught.exception.kind, "invalid")


class ConnectionTests(unittest.TestCase):
    def test_an_unreachable_broker_raises_kind_http(self) -> None:
        client = Client("http://127.0.0.1:9", timeout=2)
        with self.assertRaises(BerthqError) as caught:
            client.queues_list()
        self.assertEqual(caught.exception.kind, "http")
        self.assertIsNone(caught.exception.status)
        self.assertIn("could not connect", caught.exception.message)


if __name__ == "__main__":
    unittest.main()
