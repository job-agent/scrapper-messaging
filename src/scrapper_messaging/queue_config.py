"""Provides queue configuration parameters for RabbitMQ consumers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueConfig:
    """Encapsulates queue declaration options for RabbitMQ consumers.

    Provide ``exchange`` and optionally ``routing_key`` when the consumer should bind to a
    non-default exchange. When ``routing_key`` is omitted, the queue name will be used as the
    routing key by default.
    """

    queue_name: str
    durable: bool = True
    prefetch_count: int = 1
    exchange: str = ""
    routing_key: str = ""
