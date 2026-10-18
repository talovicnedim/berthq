"""Python client for Berthq, a message broker in one binary.

    from berthq_client import Client

    client = Client("http://127.0.0.1:7331")
    client.publish("emails", {"user": 123})

    def send_email(delivery):
        ...  # raise to nack, return to ack

    client.process("emails", send_email)
"""

from .client import Client
from .errors import BerthqError
from .publisher import Publisher
from .types import (
    Consumed,
    ConsumerInfo,
    ConsumerMessage,
    ConsumerStats,
    Counters,
    Delivery,
    DiskStats,
    MessageDeadLetter,
    MessageInfo,
    NewMessage,
    Overview,
    Published,
    QueueDetails,
    QueueInfo,
    QueueSettings,
    QueueStats,
    Requeued,
    Settled,
    StatsPoint,
    WorkerInfo,
)

__version__ = "0.1.0"

__all__ = [
    "BerthqError",
    "Client",
    "Consumed",
    "ConsumerInfo",
    "ConsumerMessage",
    "ConsumerStats",
    "Counters",
    "Delivery",
    "DiskStats",
    "MessageDeadLetter",
    "MessageInfo",
    "NewMessage",
    "Overview",
    "Published",
    "Publisher",
    "QueueDetails",
    "QueueInfo",
    "QueueSettings",
    "QueueStats",
    "Requeued",
    "Settled",
    "StatsPoint",
    "WorkerInfo",
]
