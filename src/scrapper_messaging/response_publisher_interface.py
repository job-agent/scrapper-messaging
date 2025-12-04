"""Defines the contract for publishing scrape responses."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from job_scrapper_contracts import ScrapeJobsResponse
from pika.channel import Channel


class IResponsePublisher(ABC):
    """Publishes scrape job responses back to the requester."""

    @abstractmethod
    def publish(
        self,
        *,
        channel: Channel,
        reply_to: str,
        correlation_id: Optional[str],
        response: ScrapeJobsResponse,
    ) -> None:
        """Send a scrape jobs response message."""
