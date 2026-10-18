"""Starts real brokers for the tests: the binary built by `cargo build -p berthq-cli`."""

import os
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.request
import uuid
from pathlib import Path
from typing import Any, List, Optional

from berthq_client import Client, Delivery

REPO = Path(__file__).resolve().parents[3]
BINARY = Path(os.environ.get("BERTHQ_BINARY", REPO / "target" / "debug" / "berthq"))


class Berthq:
    """A broker on a free port with its own data directory."""

    def __init__(self, api_keys: Optional[List[str]] = None) -> None:
        if not BINARY.exists():
            raise unittest.SkipTest(f"{BINARY} not found; run `cargo build -p berthq-cli`")
        self.data = tempfile.mkdtemp(prefix="berthq-python-")
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        args = [str(BINARY), "start", "--port", str(self.port), "--data", self.data]
        # Shutdown waits for leased messages; tests leave some behind on purpose.
        args += ["--shutdown-timeout", "1s"]
        for key in api_keys or []:
            args += ["--api-key", key]
        # BERTHQ_* variables from the developer's shell would change the broker's settings.
        env = {key: value for key, value in os.environ.items() if not key.startswith("BERTHQ_")}
        self.process = subprocess.Popen(
            args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self._wait_until_healthy()

    def _wait_until_healthy(self) -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"broker exited with {self.process.returncode}")
            try:
                with urllib.request.urlopen(self.url + "/healthz", timeout=1) as response:
                    if response.read() == b"ok":
                        return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError("broker did not become healthy")

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        shutil.rmtree(self.data, ignore_errors=True)


class BerthqTestCase(unittest.TestCase):
    """One broker per test class; every test uses its own queues (see ``name``)."""

    broker: Berthq
    client: Client

    @classmethod
    def setUpClass(cls) -> None:
        cls.broker = Berthq()
        cls.client = Client(cls.broker.url)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.broker.stop()

    def name(self, prefix: str = "q") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    def drain(self, queue: str, consumer: str) -> List[Any]:
        """Consumes and acks everything ``consumer`` has right now; returns the bodies."""
        deliveries = self.client.consume(queue, consumer=consumer, max=100).messages
        leases = [delivery.lease for delivery in deliveries if delivery.lease is not None]
        if leases:
            self.client.ack(queue, leases)
        return [delivery.body for delivery in deliveries]

    @staticmethod
    def lease(delivery: Delivery) -> str:
        assert delivery.lease is not None
        return delivery.lease


def _free_port() -> int:
    # The port could in theory be taken between closing and the broker binding it; good
    # enough for tests.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
