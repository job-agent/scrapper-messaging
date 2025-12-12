"""RabbitMQ implementation of the response publisher."""

from __future__ import annotations

import json
import logging
from typing import Optional

import pika
from job_scrapper_contracts import ScrapeJobsResponse
from pika.channel import Channel

from scrapper_messaging.contracts import IResponsePublisher


class RabbitMQResponsePublisher(IResponsePublisher):
    """Publishes scrape responses using RabbitMQ direct replies."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def publish(
        self,
        *,
        channel: Channel,
        reply_to: str,
        correlation_id: Optional[str],
        response: ScrapeJobsResponse,
    ) -> None:
        channel.basic_publish(
            exchange="",
            routing_key=reply_to,
            properties=pika.BasicProperties(
                correlation_id=correlation_id,
                content_type="application/json",
            ),
            body=json.dumps(response).encode("utf-8"),
        )

        self.logger.info("Sent response to %s with correlation_id=%s", reply_to, correlation_id)
