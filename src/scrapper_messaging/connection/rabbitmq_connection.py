"""RabbitMQ connection management."""

from __future__ import annotations

import logging
import os
from types import TracebackType
from typing import Optional, Type

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.connection import Parameters

from scrapper_messaging.contracts import IRabbitMQConnection


class RabbitMQConnection(IRabbitMQConnection):
    """Manages lifecycle of a blocking RabbitMQ connection."""

    def __init__(self, rabbitmq_url: Optional[str] = None) -> None:
        url = (rabbitmq_url or os.getenv("RABBITMQ_URL") or "").strip()
        if not url:
            raise ValueError(
                "RabbitMQ URL must be provided via argument or RABBITMQ_URL environment variable."
            )

        try:
            self._parameters: Parameters = pika.URLParameters(url)
        except ValueError as exc:
            raise ValueError(f"Invalid RabbitMQ URL provided: {url}") from exc

        self.rabbitmq_url = url
        self.connection: Optional[BlockingConnection] = None
        self.channel: Optional[BlockingChannel] = None
        self.logger = logging.getLogger(__name__)

    def connect(self) -> BlockingChannel:
        if self.connection is None or self.connection.is_closed:
            self.logger.info("Connecting to RabbitMQ at %s", self.rabbitmq_url)
            try:
                self.connection = pika.BlockingConnection(self._parameters)
            except pika.exceptions.AMQPConnectionError as exc:
                self.logger.error("Failed to establish RabbitMQ connection: %s", exc)
                raise

            self.channel = self.connection.channel()
            self.logger.info("Connected to RabbitMQ.")

        if self.channel is None or self.channel.is_closed:
            self.logger.debug("Re-opening channel for RabbitMQ connection.")
            self.channel = self.connection.channel()

        return self.channel

    def close(self) -> None:
        if self.channel and not self.channel.is_closed:
            self.channel.close()
            self.logger.info("Closed RabbitMQ channel.")

        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Closed RabbitMQ connection.")

    def __enter__(self) -> RabbitMQConnection:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()
