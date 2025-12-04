"""Tests for RabbitMQ consumer in scrapper-messaging."""

import json
from unittest.mock import Mock

import pika
import pytest

from scrapper_messaging.consumer import ScrapperConsumer
from scrapper_messaging.jobs_service_invoker_interface import IJobsServiceInvoker
from scrapper_messaging.json_scrape_request_decoder import JSONScrapeRequestDecoder
from scrapper_messaging.queue_config import QueueConfig
from scrapper_messaging.rabbitmq_connection_interface import IRabbitMQConnection
from scrapper_messaging.response_publisher_interface import IResponsePublisher
from scrapper_messaging.scrape_request_decoder_interface import IScrapeRequestDecoder
from scrapper_messaging.scrapper_consumer_config import ScrapperConsumerDependencies
from scrapper_messaging.scrapper_service_invoker import ScrapperServiceInvoker


class DummyJob:
    def __init__(self, **data):
        self._data = data

    def to_dict(self):
        return self._data.copy()


@pytest.fixture
def mock_connection():
    connection = Mock(spec=IRabbitMQConnection)
    channel = Mock()
    connection.connect.return_value = channel
    return connection


@pytest.fixture
def mock_publisher():
    return Mock(spec=IResponsePublisher)


@pytest.fixture
def mock_service():
    service = Mock()

    def fake_scrape_jobs(*, filters, timeout, batch_size, on_jobs_batch):
        job_data = {
            "job_id": 12345,
            "title": "Python Developer",
            "url": "https://example.com/job/12345",
            "description": "Great Python job",
            "company": {"name": "Tech Corp", "website": "https://techcorp.com"},
            "category": "Software Development",
            "date_posted": "2024-01-01T00:00:00",
            "valid_through": "2024-02-01T00:00:00",
            "employment_type": "remote",
        }

        jobs = [DummyJob(**job_data)]

        if on_jobs_batch:
            on_jobs_batch(jobs, False)
            on_jobs_batch([], True)

        return jobs

    service.scrape_jobs.side_effect = fake_scrape_jobs
    return service


@pytest.fixture
def queue_config():
    return QueueConfig(queue_name=ScrapperConsumer.QUEUE_NAME)


@pytest.fixture
def consumer(mock_service, mock_connection, mock_publisher, queue_config):
    service_invoker = ScrapperServiceInvoker(mock_service)
    return ScrapperConsumer(
        connection=mock_connection,
        queue_config=queue_config,
        request_decoder=JSONScrapeRequestDecoder(),
        response_publisher=mock_publisher,
        service_invoker=service_invoker,
    )


def test_consumer_initialization(consumer, queue_config):
    assert consumer.queue_config == queue_config
    assert isinstance(consumer.request_decoder, JSONScrapeRequestDecoder)


def test_start_uses_queue_config(mock_connection):
    queue_config = QueueConfig(
        queue_name="jobs",
        durable=False,
        prefetch_count=5,
        exchange="jobs.exchange",
        routing_key="jobs.route",
    )
    channel = mock_connection.connect.return_value
    channel.start_consuming.side_effect = KeyboardInterrupt()

    consumer = ScrapperConsumer(
        connection=mock_connection,
        queue_config=queue_config,
        request_decoder=Mock(spec=IScrapeRequestDecoder),
        response_publisher=Mock(spec=IResponsePublisher),
        service_invoker=Mock(spec=IJobsServiceInvoker),
    )

    consumer.start()

    channel.queue_declare.assert_called_once_with(queue="jobs", durable=False)
    channel.queue_bind.assert_called_once_with(
        queue="jobs",
        exchange="jobs.exchange",
        routing_key="jobs.route",
    )
    channel.basic_qos.assert_called_once_with(prefetch_count=5)
    channel.basic_consume.assert_called_once()
    mock_connection.close.assert_called_once()


def test_process_request_basic(consumer, mock_publisher):
    channel = Mock()
    request = {"filters": {"min_salary": 5000}, "timeout": 30}

    response, final_emitted = consumer._process_request(request, channel, "reply.queue", "cid")

    assert final_emitted is True
    assert response["success"] is True
    assert response["total_jobs"] == 1
    assert mock_publisher.publish.call_count == 2


def test_on_message_success_flow(consumer, mock_connection, mock_publisher):
    channel = mock_connection.connect.return_value
    method = Mock()
    method.delivery_tag = 1
    properties = Mock()
    properties.correlation_id = "cid"
    properties.reply_to = "reply.queue"

    body = json.dumps({"filters": {"location": "remote"}}).encode("utf-8")

    consumer._on_message(channel, method, properties, body)

    channel.basic_ack.assert_called_once_with(delivery_tag=1)
    assert mock_publisher.publish.call_count >= 1


def test_on_message_handles_decoder_error(consumer, mock_connection, mock_publisher):
    consumer.request_decoder = Mock()
    consumer.request_decoder.decode.side_effect = ValueError("bad payload")

    channel = mock_connection.connect.return_value
    method = Mock()
    method.delivery_tag = 1
    properties = Mock()
    properties.correlation_id = "cid"
    properties.reply_to = "reply.queue"

    consumer._on_message(channel, method, properties, b"invalid")

    mock_publisher.publish.assert_called_once()
    error_response = mock_publisher.publish.call_args.kwargs["response"]
    assert error_response["success"] is False
    assert "bad payload" in error_response["error"]
    channel.basic_ack.assert_called_once_with(delivery_tag=1)


def test_on_message_missing_reply_to(consumer, mock_connection, mock_publisher):
    channel = mock_connection.connect.return_value
    method = Mock()
    method.delivery_tag = 1
    properties = Mock()
    properties.correlation_id = "cid"
    properties.reply_to = None

    body = json.dumps({"filters": {}}).encode("utf-8")

    consumer._on_message(channel, method, properties, body)

    assert mock_publisher.publish.call_count == 0
    channel.basic_ack.assert_called_once_with(delivery_tag=1)


def test_response_publisher_failure_bubbles(consumer):
    channel = Mock()
    consumer.response_publisher.publish.side_effect = pika.exceptions.AMQPError("failed")

    with pytest.raises(pika.exceptions.AMQPError):
        consumer._publish_response(channel, "reply", "cid", {})


def test_from_url_uses_dependencies():
    service = Mock()
    connection = Mock(spec=IRabbitMQConnection)
    request_decoder = Mock(spec=IScrapeRequestDecoder)
    response_publisher = Mock(spec=IResponsePublisher)
    service_invoker = Mock(spec=IJobsServiceInvoker)

    connection_factory = Mock(return_value=connection)
    request_decoder_factory = Mock(return_value=request_decoder)
    response_publisher_factory = Mock(return_value=response_publisher)
    service_invoker_factory = Mock(return_value=service_invoker)

    dependencies = ScrapperConsumerDependencies(
        queue_config=QueueConfig(queue_name="custom.queue"),
        make_connection=connection_factory,
        make_request_decoder=request_decoder_factory,
        make_response_publisher=response_publisher_factory,
        make_service_invoker=service_invoker_factory,
    )

    consumer = ScrapperConsumer.from_url(
        service=service,
        rabbitmq_url="amqp://localhost",
        dependencies=dependencies,
    )

    connection_factory.assert_called_once_with("amqp://localhost")
    service_invoker_factory.assert_called_once_with(service)
    request_decoder_factory.assert_called_once_with()
    response_publisher_factory.assert_called_once_with()

    assert consumer.connection is connection
    assert consumer.request_decoder is request_decoder
    assert consumer.response_publisher is response_publisher
    assert consumer.service_invoker is service_invoker
    assert consumer.queue_config.queue_name == "custom.queue"


def test_start_without_exchange_skips_bind(mock_connection):
    queue_config = QueueConfig(
        queue_name="jobs",
        durable=True,
        prefetch_count=1,
        exchange="",
    )
    channel = mock_connection.connect.return_value
    channel.start_consuming.side_effect = KeyboardInterrupt()

    consumer = ScrapperConsumer(
        connection=mock_connection,
        queue_config=queue_config,
        request_decoder=Mock(spec=IScrapeRequestDecoder),
        response_publisher=Mock(spec=IResponsePublisher),
        service_invoker=Mock(spec=IJobsServiceInvoker),
    )

    consumer.start()

    channel.queue_declare.assert_called_once_with(queue="jobs", durable=True)
    channel.queue_bind.assert_not_called()
    channel.basic_qos.assert_called_once_with(prefetch_count=1)
    channel.basic_consume.assert_called_once()


def test_from_url_uses_default_dependencies(mock_connection):
    from unittest.mock import patch

    service = Mock()

    with patch("scrapper_messaging.scrapper_consumer_config.RabbitMQConnection") as mock_conn_class:
        mock_conn_class.return_value = mock_connection
        consumer = ScrapperConsumer.from_url(
            service=service,
            rabbitmq_url="amqp://localhost",
            dependencies=None,
        )

    assert consumer.connection is mock_connection
    assert consumer.queue_config.queue_name == "job.scrape.request"
