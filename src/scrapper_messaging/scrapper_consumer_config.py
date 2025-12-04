"""Configuration primitives for wiring a `ScrapperConsumer`."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

from job_scrapper_contracts import ScrapperServiceInterface

from .connection import RabbitMQConnection
from .jobs_service_invoker_interface import IJobsServiceInvoker
from .json_scrape_request_decoder import JSONScrapeRequestDecoder
from .queue_config import QueueConfig
from .rabbitmq_connection_interface import IRabbitMQConnection
from .rabbitmq_response_publisher import RabbitMQResponsePublisher
from .response_publisher_interface import IResponsePublisher
from .scrape_request_decoder_interface import IScrapeRequestDecoder
from .scrapper_service_invoker import ScrapperServiceInvoker

DEFAULT_QUEUE_NAME = "job.scrape.request"


@dataclass(frozen=True)
class ScrapperConsumerDependencies:
    """Bundles factory functions and defaults for consumer wiring."""

    queue_config: QueueConfig = field(
        default_factory=lambda: QueueConfig(queue_name=DEFAULT_QUEUE_NAME)
    )
    make_request_decoder: Callable[[], IScrapeRequestDecoder] = field(
        default=JSONScrapeRequestDecoder
    )
    make_response_publisher: Callable[[], IResponsePublisher] = field(
        default=RabbitMQResponsePublisher
    )
    make_service_invoker: Callable[[ScrapperServiceInterface], IJobsServiceInvoker] = field(
        default=lambda service: ScrapperServiceInvoker(service)
    )
    make_connection: Callable[[Optional[str]], IRabbitMQConnection] = field(
        default=lambda rabbitmq_url: RabbitMQConnection(rabbitmq_url)
    )
