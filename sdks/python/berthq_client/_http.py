"""JSON over HTTP with the standard library, turning every failure into a BerthqError.

Connections stay open and are reused, one per thread, so a client can be shared between
threads without them waiting for each other.
"""

import http.client
import json
import os
import select
import socket
import ssl
import threading
import urllib.parse
import weakref
from typing import Any, Dict, Optional, Tuple, Union

from ._json import dumps
from .errors import BerthqError

# What a kept-alive connection raises when the broker closed it before our request arrived.
STALE_CONNECTION = (
    http.client.RemoteDisconnected,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
)


class Transport:
    def __init__(self, url: str, api_key: Optional[str], timeout: float) -> None:
        self.base = url.rstrip("/")
        parts = urllib.parse.urlsplit(self.base)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise BerthqError("invalid", None, f"not an http(s) url: {url}")
        self.api_key = api_key
        self.timeout = timeout
        self._host = parts.hostname
        self._port = parts.port
        # A broker behind a reverse proxy may live under a path (https://example.com/berthq).
        self._prefix = parts.path
        self._tls = ssl.create_default_context() if parts.scheme == "https" else None
        self._local = threading.local()
        # Weak, so the connection of a thread that has ended can go away with it.
        self._open_connections: "weakref.WeakSet[http.client.HTTPConnection]" = weakref.WeakSet()
        self._lock = threading.Lock()

    def request(
        self,
        method: str,
        path: str,
        body: Union[Dict[str, Any], bytes, None] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Sends a request and returns the decoded JSON answer, or ``None`` for an empty one
        (``204``). ``body`` is a dict to encode, or JSON bytes that are sent as they are."""
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            data = body if isinstance(body, bytes) else dumps(body)
            headers["Content-Type"] = "application/json"
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        status, raw = self._send(method, self._prefix + path, data, headers, timeout or self.timeout)
        if status >= 400:
            raise _api_error(status, raw)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            # Unknown paths are answered by the dashboard with HTML, so this usually means a
            # wrong url (or a proxy in between).
            raise BerthqError(
                "http", status, f"the answer from {self.base} isn't JSON; is the url right?"
            ) from None

    def _send(
        self, method: str, target: str, data: Optional[bytes], headers: Dict[str, str], timeout: float
    ) -> Tuple[int, bytes]:
        connection, reused = self._connection(timeout)
        try:
            try:
                return _exchange(connection, method, target, data, headers)
            except STALE_CONNECTION:
                if not reused:
                    raise
                # The broker closed the kept-alive connection just before our request reached
                # it: send it once more on a new connection.
                self._drop()
                connection, _ = self._connection(timeout)
                return _exchange(connection, method, target, data, headers)
        except (socket.timeout, TimeoutError):
            self._drop()
            raise BerthqError(
                "http", None, f"the broker at {self.base} did not answer in time"
            ) from None
        except (*STALE_CONNECTION, http.client.HTTPException) as err:
            self._drop()
            raise BerthqError("http", None, f"request to {self.base} failed ({err})") from None
        except OSError as err:  # refused, unknown host, TLS, ...
            self._drop()
            raise BerthqError(
                "http", None, f"could not connect to the broker at {self.base} ({err})"
            ) from None

    def _connection(self, timeout: float) -> Tuple[http.client.HTTPConnection, bool]:
        """This thread's connection, and whether it was already open."""
        slot: Optional[_Slot] = getattr(self._local, "slot", None)
        # A forked worker must not share its parent's socket.
        connection = slot.connection if slot is not None and slot.pid == os.getpid() else None
        if connection is not None and connection.sock is not None and _closed_by_peer(connection.sock):
            connection.close()
        if connection is None:
            connection = self._open(timeout)
            self._local.slot = _Slot(connection)
        reused = connection.sock is not None
        connection.timeout = timeout
        if connection.sock is not None:
            connection.sock.settimeout(timeout)
        return connection, reused

    def close(self) -> None:
        """Closes every open connection; the next request opens a new one."""
        with self._lock:
            connections = list(self._open_connections)
            self._open_connections.clear()
        for connection in connections:
            connection.close()

    def _open(self, timeout: float) -> http.client.HTTPConnection:
        connection: http.client.HTTPConnection
        if self._tls is not None:
            connection = http.client.HTTPSConnection(
                self._host, self._port, timeout=timeout, context=self._tls
            )
        else:
            connection = http.client.HTTPConnection(self._host, self._port, timeout=timeout)
        with self._lock:
            self._open_connections.add(connection)
        return connection

    def _drop(self) -> None:
        slot: Optional[_Slot] = getattr(self._local, "slot", None)
        if slot is not None:
            slot.connection.close()
        self._local.slot = None


class _Slot:
    """One thread's connection. When the thread ends, its slot goes away and closes it."""

    def __init__(self, connection: http.client.HTTPConnection) -> None:
        self.connection = connection
        self.pid = os.getpid()
        weakref.finalize(self, connection.close)


def _exchange(
    connection: http.client.HTTPConnection,
    method: str,
    target: str,
    data: Optional[bytes],
    headers: Dict[str, str],
) -> Tuple[int, bytes]:
    connection.request(method, target, body=data, headers=headers)
    response = connection.getresponse()
    # Reading the whole answer frees the connection for the next request.
    return response.status, response.read()


def _closed_by_peer(sock: socket.socket) -> bool:
    """An idle kept-alive socket is only readable when the broker closed it (or sent junk)."""
    try:
        readable, _, _ = select.select([sock], [], [], 0)
    except (OSError, ValueError):
        return True
    return bool(readable)


def _api_error(status: int, raw: bytes) -> BerthqError:
    """Reads ``{ "error": { "kind", "message" } }``. Anything else (a proxy's error page, or a
    plain-text rejection) becomes kind ``http`` with the text as the message, if there is any."""
    try:
        error = json.loads(raw)["error"]
        return BerthqError(error["kind"], status, error["message"])
    except (ValueError, KeyError, TypeError):
        text = raw.decode("utf-8", "replace").strip()[:500]
        return BerthqError("http", status, text or f"the broker answered {status}")


def segment(value: str) -> str:
    """Percent-encodes a path segment so a name can't change the path."""
    return urllib.parse.quote(value, safe="")


def query(params: Dict[str, Any]) -> str:
    present = {key: value for key, value in params.items() if value is not None}
    return "?" + urllib.parse.urlencode(present) if present else ""
