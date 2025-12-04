"""Messaging package providing RabbitMQ connectivity and consumers."""

from .connection import RabbitMQConnection
from .consumer import ScrapperConsumer
from .queue_config import QueueConfig
from .rabbitmq_connection_interface import IRabbitMQConnection
from .scrapper_consumer_config import DEFAULT_QUEUE_NAME, ScrapperConsumerDependencies

__all__ = [
    "RabbitMQConnection",
    "ScrapperConsumer",
    "IRabbitMQConnection",
    "QueueConfig",
    "ScrapperConsumerDependencies",
    "DEFAULT_QUEUE_NAME",
]
