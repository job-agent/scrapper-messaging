"""Tests for RabbitMQ connection management."""

import os
from unittest.mock import Mock, patch

import pika
import pytest

from scrapper_messaging.connection import RabbitMQConnection


@pytest.fixture
def mock_pika():
    with patch("scrapper_messaging.connection.rabbitmq_connection.pika") as mock_pika_module:
        mock_parameters = Mock()
        mock_pika_module.URLParameters.return_value = mock_parameters
        mock_pika_module.exceptions = pika.exceptions
        yield mock_pika_module


def test_init_requires_url(monkeypatch):
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    with pytest.raises(ValueError):
        RabbitMQConnection()


def test_init_uses_env_var(mock_pika):
    url = "amqp://env:pass@localhost:5672/"
    with patch.dict(os.environ, {"RABBITMQ_URL": url}):
        connection = RabbitMQConnection()
        assert connection.rabbitmq_url == url
    mock_pika.URLParameters.assert_called_once_with(url)


def test_init_invalid_url():
    with patch(
        "scrapper_messaging.connection.rabbitmq_connection.pika.URLParameters",
        side_effect=ValueError,
    ):
        with pytest.raises(ValueError):
            RabbitMQConnection("invalid-url")


def test_connect_creates_channel(mock_pika):
    url = "amqp://localhost"
    mock_blocking_connection = Mock()
    mock_channel = Mock()
    mock_blocking_connection.channel.return_value = mock_channel
    mock_pika.BlockingConnection.return_value = mock_blocking_connection

    connection = RabbitMQConnection(url)
    channel = connection.connect()

    mock_pika.BlockingConnection.assert_called_once()
    assert connection.connection == mock_blocking_connection
    assert connection.channel == mock_channel
    assert channel == mock_channel


def test_connect_reuses_channel(mock_pika):
    url = "amqp://localhost"
    connection = RabbitMQConnection(url)
    existing_connection = Mock()
    existing_connection.is_closed = False
    existing_channel = Mock()
    existing_channel.is_closed = False
    connection.connection = existing_connection
    connection.channel = existing_channel

    channel = connection.connect()

    mock_pika.BlockingConnection.assert_not_called()
    assert channel == existing_channel


def test_connect_reopens_closed_channel(mock_pika):
    url = "amqp://localhost"
    connection = RabbitMQConnection(url)

    existing_connection = Mock()
    existing_connection.is_closed = False
    existing_connection.channel.return_value = Mock()
    connection.connection = existing_connection

    existing_channel = Mock()
    existing_channel.is_closed = True
    connection.channel = existing_channel

    channel = connection.connect()

    existing_connection.channel.assert_called_once()
    assert channel == existing_connection.channel.return_value


def test_connect_failure_logs_and_raises(mock_pika):
    url = "amqp://localhost"
    mock_pika.BlockingConnection.side_effect = pika.exceptions.AMQPConnectionError("failed")
    connection = RabbitMQConnection(url)

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        connection.connect()


def test_close_closes_resources():
    url = "amqp://localhost"
    connection = RabbitMQConnection(url)
    connection.connection = Mock()
    connection.channel = Mock()
    connection.connection.is_closed = False
    connection.channel.is_closed = False

    connection.close()

    connection.channel.close.assert_called_once()
    connection.connection.close.assert_called_once()


def test_context_manager(mock_pika):
    url = "amqp://localhost"
    mock_blocking_connection = Mock()
    mock_channel = Mock()
    mock_blocking_connection.channel.return_value = mock_channel
    mock_pika.BlockingConnection.return_value = mock_blocking_connection

    with RabbitMQConnection(url) as connection:
        assert connection.connection == mock_blocking_connection
        assert connection.channel == mock_channel
        connection.connection.is_closed = False
        connection.channel.is_closed = False

    connection.channel.close.assert_called_once()
    connection.connection.close.assert_called_once()


def test_close_is_safe_when_never_opened(mock_pika):
    url = "amqp://localhost"
    connection = RabbitMQConnection(url)

    connection.close()

    assert connection.connection is None
    assert connection.channel is None


def test_close_is_safe_when_already_closed(mock_pika):
    url = "amqp://localhost"
    connection = RabbitMQConnection(url)
    mock_connection = Mock()
    mock_channel = Mock()
    mock_connection.is_closed = True
    mock_channel.is_closed = True
    connection.connection = mock_connection
    connection.channel = mock_channel

    connection.close()

    mock_channel.close.assert_not_called()
    mock_connection.close.assert_not_called()
