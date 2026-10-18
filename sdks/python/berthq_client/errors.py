"""The one exception type the client raises."""

from typing import Optional


class BerthqError(Exception):
    """A failed request.

    ``kind`` is the API's error kind (``invalid``, ``notFound``, ``conflict``, ``queueFull``,
    ``diskFull``, ``unavailable``, ``storage``, ``unauthorized``, ``forbidden``), or ``http``
    when the broker could not be reached or answered something that isn't JSON. ``status`` is
    the HTTP status, or ``None`` when there was no response at all.
    """

    def __init__(self, kind: str, status: Optional[int], message: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.status = status
        self.message = message

    def __repr__(self) -> str:
        return f"BerthqError(kind={self.kind!r}, status={self.status!r}, message={self.message!r})"
