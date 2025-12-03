"""RabbitMQ connection management."""

import logging
import os
from typing import Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection


class RabbitMQConnection:
    """Manages RabbitMQ connection and channel lifecycle.

    Attributes:
        rabbitmq_url: AMQP connection URL
        connection: Pika blocking connection
        channel: Pika blocking channel
    """

    def __init__(self, rabbitmq_url: Optional[str] = None):
        """Initialize RabbitMQ connection.

        Args:
            rabbitmq_url: AMQP connection URL. If None, reads from RABBITMQ_URL env var
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv("RABBITMQ_URL")
        self.connection: Optional[BlockingConnection] = None
        self.channel: Optional[BlockingChannel] = None
        self.logger = logging.getLogger(__name__)

    def connect(self) -> BlockingChannel:
        """Establish connection and create channel.

        Returns:
            BlockingChannel: Pika channel for message operations

        Raises:
            pika.exceptions.AMQPConnectionError: If connection fails
        """
        if self.connection is None or self.connection.is_closed:
            self.logger.info(f"Connecting to RabbitMQ at {self.rabbitmq_url}")
            parameters = pika.URLParameters(self.rabbitmq_url)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.logger.info("Connected to RabbitMQ")

        return self.channel

    def close(self) -> None:
        """Close channel and connection gracefully."""
        if self.channel and not self.channel.is_closed:
            self.channel.close()
            self.logger.info("Closed RabbitMQ channel")

        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Closed RabbitMQ connection")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
