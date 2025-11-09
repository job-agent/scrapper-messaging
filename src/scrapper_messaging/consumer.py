import json
import logging
from typing import List, Optional, Tuple

import pika
from job_scrapper_contracts import (
    Job,
    ScrapeJobsFilter,
    ScrapeJobsRequest,
    ScrapeJobsResponse,
    ScrapperServiceInterface,
)
from pika.channel import Channel

from .connection import RabbitMQConnection


class ScrapperConsumer:
    QUEUE_NAME = "job.scrape.request"
    DEFAULT_BATCH_SIZE = 50

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
        channel: Channel,
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

            filter_payload = request.get("filter") or {}

            self.logger.info(
                f"Processing scrape request with filter={filter_payload}, "
                f"timeout={request.get('timeout', 30)}"
            )

            response, final_emitted = self._process_request(
                request, channel, reply_to, correlation_id
            )

            if reply_to and not final_emitted:
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
        channel: Channel,
        reply_to: Optional[str],
        correlation_id: Optional[str],
    ) -> Tuple[ScrapeJobsResponse, bool]:
        filters: ScrapeJobsFilter = request.get("filter") or {}
        batch_size = request.get("batch_size", self.DEFAULT_BATCH_SIZE)
        total_jobs = 0
        final_emitted = False

        def emit_jobs(batch: List[Job], final: bool) -> None:
            nonlocal total_jobs, final_emitted
            jobs_dicts = [job.to_dict() for job in batch]
            total_jobs += len(jobs_dicts)
            if final:
                final_emitted = True
            if not reply_to:
                return
            response: ScrapeJobsResponse = {
                "jobs": jobs_dicts,
                "success": True,
                "error": None,
                "jobs_count": len(jobs_dicts),
                "is_complete": final,
            }
            if final:
                response["total_jobs"] = total_jobs
            self._send_response(channel, reply_to, correlation_id, response)

        result = self.service.scrape_jobs(
            filters=filters,
            timeout=request.get("timeout", 30),
            batch_size=batch_size,
            on_jobs_batch=emit_jobs,
        )

        if isinstance(result, list):
            total_jobs = max(total_jobs, len(result))

        self.logger.info(f"Scraping completed: total {total_jobs} jobs scraped")

        response: ScrapeJobsResponse = {
            "jobs": [],
            "success": True,
            "error": None,
            "jobs_count": 0,
            "is_complete": True,
            "total_jobs": total_jobs,
        }

        return response, final_emitted

    def _send_response(
        self,
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

        self.logger.info(f"Sent response to {reply_to} with correlation_id={correlation_id}")
