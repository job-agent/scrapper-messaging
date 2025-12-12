"""Messaging package providing RabbitMQ connectivity and consumers."""

from .connection import RabbitMQConnection
from .consumer import (
    DEFAULT_QUEUE_NAME,
    QueueConfig,
    ScrapperConsumer,
    ScrapperConsumerDependencies,
)
from .contracts import IRabbitMQConnection

__all__ = [
    "RabbitMQConnection",
    "ScrapperConsumer",
    "IRabbitMQConnection",
    "QueueConfig",
    "ScrapperConsumerDependencies",
    "DEFAULT_QUEUE_NAME",
]
