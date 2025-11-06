"""RabbitMQ consumer for processing scrape job requests."""

import json
import logging
from datetime import datetime
from typing import List, Optional

import pika
from job_scrapper_contracts import (
    Job,
    ScrapeJobsRequest,
    ScrapeJobsResponse,
    ScrapperServiceInterface,
)

from .connection import RabbitMQConnection


class ScrapperConsumer:
    """RabbitMQ consumer that processes scrape job requests."""

    QUEUE_NAME = "job.scrape.request"

    def __init__(
        self,
        service: ScrapperServiceInterface,
        rabbitmq_url: Optional[str] = None,
    ):
        self.service = service
        self.rabbitmq_connection = RabbitMQConnection(rabbitmq_url)
        self.logger = logging.getLogger(__name__)

    def start(self) -> None:
        channel = self.rabbitmq_connection.connect()
        channel.queue_declare(queue=self.QUEUE_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.QUEUE_NAME, on_message_callback=self._on_message)

        self.logger.info(f"Started consuming from {self.QUEUE_NAME}")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer...")
            channel.stop_consuming()
        finally:
            self.rabbitmq_connection.close()

    def _on_message(
        self,
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        correlation_id = properties.correlation_id
        reply_to = properties.reply_to

        self.logger.info(
            f"Received message with correlation_id={correlation_id}, reply_to={reply_to}"
        )

        try:
            request_data = json.loads(body.decode("utf-8"))
            request = ScrapeJobsRequest(**request_data)

            self.logger.info(
                "Processing scrape request with parameters: "
                f"salary={request.get('salary', 4000)}, "
                f"employment={request.get('employment', 'remote')}, "
                f"posted_after={request.get('posted_after', 'None')}, "
                f"timeout={request.get('timeout', 30)}"
            )

            response = self._process_request(request, channel, reply_to, correlation_id)

            if reply_to:
                self._send_response(channel, reply_to, correlation_id, response)

            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.info(f"Successfully processed message {correlation_id}")

        except Exception as exc:
            self.logger.error(f"Error processing message: {exc}", exc_info=True)

            error_response: ScrapeJobsResponse = {
                "jobs": [],
                "success": False,
                "error": str(exc),
                "jobs_count": 0,
            }

            if reply_to:
                self._send_response(channel, reply_to, correlation_id, error_response)

            channel.basic_ack(delivery_tag=method.delivery_tag)

    def _process_request(
        self,
        request: ScrapeJobsRequest,
        channel: pika.channel.Channel,
        reply_to: Optional[str],
        correlation_id: Optional[str],
    ) -> ScrapeJobsResponse:
        posted_after = None
        if request.get("posted_after"):
            posted_after = datetime.fromisoformat(request["posted_after"])

        total_jobs = 0

        def on_page_complete(page_number: int, page_jobs: List[Job]):
            nonlocal total_jobs
            jobs_dicts = [job.to_dict() for job in page_jobs]
            total_jobs += len(jobs_dicts)

            page_response: ScrapeJobsResponse = {
                "jobs": jobs_dicts,
                "success": True,
                "error": None,
                "jobs_count": len(jobs_dicts),
                "is_complete": False,
                "page_number": page_number,
            }
            if reply_to:
                self._send_response(channel, reply_to, correlation_id, page_response)

        self.service.scrape_jobs_as_dicts(
            salary=request.get("salary", 4000),
            employment=request.get("employment", "remote"),
            posted_after=posted_after,
            timeout=request.get("timeout", 30),
            on_page_complete=on_page_complete,
        )

        self.logger.info(f"Scraping completed: total {total_jobs} jobs scraped")

        response: ScrapeJobsResponse = {
            "jobs": [],
            "success": True,
            "error": None,
            "jobs_count": 0,
            "is_complete": True,
            "total_jobs": total_jobs,
        }

        return response

    def _send_response(
        self,
        channel: pika.channel.Channel,
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

        self.logger.info(f"Sent response to {reply_to} with correlation_id={correlation_id}")
