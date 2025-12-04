import logging
from typing import List, Optional, Tuple

import pika
from job_scrapper_contracts import (
    Job,
    ScrapeJobsRequest,
    ScrapeJobsResponse,
    ScrapperServiceInterface,
)
from pika.channel import Channel

from .jobs_service_invoker_interface import IJobsServiceInvoker
from .queue_config import QueueConfig
from .rabbitmq_connection_interface import IRabbitMQConnection
from .response_publisher_interface import IResponsePublisher
from .scrape_request_decoder_interface import IScrapeRequestDecoder
from .scrapper_consumer_config import DEFAULT_QUEUE_NAME, ScrapperConsumerDependencies


class ScrapperConsumer:
    """Consumes scrape requests and delegates processing to the scrapper service."""

    QUEUE_NAME = DEFAULT_QUEUE_NAME
    DEFAULT_BATCH_SIZE = 50

    def __init__(
        self,
        *,
        connection: IRabbitMQConnection,
        queue_config: QueueConfig,
        request_decoder: IScrapeRequestDecoder,
        response_publisher: IResponsePublisher,
        service_invoker: IJobsServiceInvoker,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.queue_config = queue_config
        self.request_decoder = request_decoder
        self.response_publisher = response_publisher
        self.service_invoker = service_invoker
        self.connection = connection

    @classmethod
    def from_url(
        cls,
        service: ScrapperServiceInterface,
        rabbitmq_url: Optional[str] = None,
        *,
        dependencies: Optional[ScrapperConsumerDependencies] = None,
    ) -> "ScrapperConsumer":
        deps = dependencies or ScrapperConsumerDependencies()

        return cls(
            connection=deps.make_connection(rabbitmq_url),
            queue_config=deps.queue_config,
            request_decoder=deps.make_request_decoder(),
            response_publisher=deps.make_response_publisher(),
            service_invoker=deps.make_service_invoker(service),
        )

    def start(self) -> None:
        channel = self.connection.connect()
        channel.queue_declare(queue=self.queue_config.queue_name, durable=self.queue_config.durable)
        if self.queue_config.exchange:
            routing_key = self.queue_config.routing_key or self.queue_config.queue_name
            channel.queue_bind(
                queue=self.queue_config.queue_name,
                exchange=self.queue_config.exchange,
                routing_key=routing_key,
            )
        channel.basic_qos(prefetch_count=self.queue_config.prefetch_count)
        channel.basic_consume(
            queue=self.queue_config.queue_name,
            on_message_callback=self._on_message,
        )

        self.logger.info("Started consuming from %s", self.queue_config.queue_name)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer...")
            channel.stop_consuming()
        finally:
            self.connection.close()

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
            "Received message with correlation_id=%s, reply_to=%s",
            correlation_id,
            reply_to,
        )

        try:
            request = self.request_decoder.decode(body)
            self.logger.info(
                "Processing scrape request with timeout=%s",
                request.get("timeout", 30),
            )

            response, final_emitted = self._process_request(
                request,
                channel,
                reply_to,
                correlation_id,
            )

            if reply_to and not final_emitted:
                self._publish_response(channel, reply_to, correlation_id, response)

            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.info("Successfully processed message %s", correlation_id)

        except Exception as exc:
            self.logger.error("Error processing message: %s", exc, exc_info=True)

            error_response: ScrapeJobsResponse = {
                "jobs": [],
                "success": False,
                "error": str(exc),
                "jobs_count": 0,
            }

            if reply_to:
                self._publish_response(channel, reply_to, correlation_id, error_response)

            channel.basic_ack(delivery_tag=method.delivery_tag)

    def _process_request(
        self,
        request: ScrapeJobsRequest,
        channel: Channel,
        reply_to: Optional[str],
        correlation_id: Optional[str],
    ) -> Tuple[ScrapeJobsResponse, bool]:
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
            self._publish_response(channel, reply_to, correlation_id, response)

        result = self.service_invoker.invoke(
            request=request,
            batch_size=batch_size,
            on_jobs_batch=emit_jobs,
        )

        if isinstance(result, list):
            total_jobs = max(total_jobs, len(result))

        self.logger.info("Scraping completed: total %s jobs scraped", total_jobs)

        response: ScrapeJobsResponse = {
            "jobs": [],
            "success": True,
            "error": None,
            "jobs_count": 0,
            "is_complete": True,
            "total_jobs": total_jobs,
        }

        return response, final_emitted

    def _publish_response(
        self,
        channel: Channel,
        reply_to: str,
        correlation_id: Optional[str],
        response: ScrapeJobsResponse,
    ) -> None:
        self.response_publisher.publish(
            channel=channel,
            reply_to=reply_to,
            correlation_id=correlation_id,
            response=response,
        )
