"""Tests for RabbitMQResponsePublisher."""

import json
from unittest.mock import Mock

from scrapper_messaging.rabbitmq_response_publisher import RabbitMQResponsePublisher


def test_response_publisher_sends_message():
    channel = Mock()
    publisher = RabbitMQResponsePublisher()
    response = {
        "jobs": [],
        "success": True,
        "error": None,
        "jobs_count": 0,
        "is_complete": True,
        "total_jobs": 0,
    }

    publisher.publish(
        channel=channel,
        reply_to="reply.queue",
        correlation_id="cid",
        response=response,
    )

    channel.basic_publish.assert_called_once()
    kwargs = channel.basic_publish.call_args.kwargs
    assert kwargs["routing_key"] == "reply.queue"
    assert kwargs["properties"].correlation_id == "cid"
    assert json.loads(kwargs["body"].decode("utf-8"))["is_complete"] is True
