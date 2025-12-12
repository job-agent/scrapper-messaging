"""Defines the contract for RabbitMQ connections."""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Optional, Type

from pika.adapters.blocking_connection import BlockingChannel


class IRabbitMQConnection(ABC):
    """Represents a RabbitMQ connection capable of producing blocking channels."""

    @abstractmethod
    def connect(self) -> BlockingChannel:
        """Return an open blocking channel ready for message operations."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection and associated resources."""

    @abstractmethod
    def __enter__(self) -> IRabbitMQConnection:
        """Enter a managed connection context."""

    @abstractmethod
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit a managed connection context."""
