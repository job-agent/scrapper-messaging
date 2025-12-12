"""Contract interfaces for scrapper messaging."""

from .jobs_service_invoker_interface import IJobsServiceInvoker
from .rabbitmq_connection_interface import IRabbitMQConnection
from .response_publisher_interface import IResponsePublisher
from .scrape_request_decoder_interface import IScrapeRequestDecoder

__all__ = [
    "IJobsServiceInvoker",
    "IRabbitMQConnection",
    "IResponsePublisher",
    "IScrapeRequestDecoder",
]
