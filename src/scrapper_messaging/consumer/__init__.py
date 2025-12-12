"""Scrapper consumer for RabbitMQ message processing."""

from .queue_config import QueueConfig
from .scrapper_consumer import ScrapperConsumer
from .scrapper_consumer_config import DEFAULT_QUEUE_NAME, ScrapperConsumerDependencies

__all__ = [
    "QueueConfig",
    "ScrapperConsumer",
    "ScrapperConsumerDependencies",
    "DEFAULT_QUEUE_NAME",
]
