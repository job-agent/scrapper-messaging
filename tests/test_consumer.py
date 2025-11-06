"""Tests for RabbitMQ consumer in scrapper-messaging."""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from scrapper_messaging.consumer import ScrapperConsumer


class DummyJob:
    def __init__(self, **data):
        self._data = data

    def to_dict(self):
        return self._data.copy()


@pytest.fixture
def mock_service():
    service = Mock()

    def fake_scrape_jobs_as_dicts(*, salary, employment, posted_after, timeout, on_page_complete):
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

        if on_page_complete:
            on_page_complete(1, [DummyJob(**job_data)])

        return [job_data]

    service.scrape_jobs_as_dicts.side_effect = fake_scrape_jobs_as_dicts
    return service


@pytest.fixture
def consumer(mock_service):
    with patch("scrapper_messaging.consumer.RabbitMQConnection"):
        return ScrapperConsumer(
            service=mock_service, rabbitmq_url="amqp://test:test@localhost:5672/"
        )


def test_consumer_initialization(consumer, mock_service):
    assert consumer.service == mock_service
    assert consumer.QUEUE_NAME == "job.scrape.request"


def test_process_request_basic(consumer, mock_service):
    request = {
        "salary": 5000,
        "employment": "full-time",
        "posted_after": None,
        "timeout": 30,
    }

    channel = Mock()
    consumer._send_response = Mock()

    response = consumer._process_request(
        request, channel, "test.reply.queue", "test-correlation-id"
    )

    mock_service.scrape_jobs_as_dicts.assert_called_once()
    consumer._send_response.assert_called_once()

    assert response["success"] is True
    assert response["error"] is None
    assert response["jobs_count"] == 0
    assert response["is_complete"] is True


def test_process_request_with_posted_after(consumer, mock_service):
    request = {
        "salary": 4000,
        "employment": "remote",
        "posted_after": "2024-01-01T00:00:00",
        "timeout": 60,
    }

    channel = Mock()
    consumer._send_response = Mock()

    consumer._process_request(request, channel, "reply", "cid")

    kwargs = mock_service.scrape_jobs_as_dicts.call_args.kwargs
    assert kwargs["posted_after"] == datetime(2024, 1, 1, 0, 0, 0)


def test_process_request_default_values(consumer, mock_service):
    request = {}

    channel = Mock()
    consumer._send_response = Mock()

    consumer._process_request(request, channel, "reply", "cid")

    kwargs = mock_service.scrape_jobs_as_dicts.call_args.kwargs
    assert kwargs["salary"] == 4000
    assert kwargs["employment"] == "remote"
    assert kwargs["posted_after"] is None
    assert kwargs["timeout"] == 30


def test_send_response(consumer):
    mock_channel = Mock()
    response = {
        "jobs": [],
        "success": True,
        "error": None,
        "jobs_count": 0,
    }

    consumer._send_response(
        channel=mock_channel,
        reply_to="test.reply.queue",
        correlation_id="test-correlation-id",
        response=response,
    )

    mock_channel.basic_publish.assert_called_once()
    call_args = mock_channel.basic_publish.call_args

    assert call_args.kwargs["routing_key"] == "test.reply.queue"
    assert call_args.kwargs["properties"].correlation_id == "test-correlation-id"

    body = json.loads(call_args.kwargs["body"].decode("utf-8"))
    assert body["success"] is True
    assert body["jobs_count"] == 0
