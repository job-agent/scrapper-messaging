"""Defines the contract for decoding scrape job requests."""

from __future__ import annotations

from abc import ABC, abstractmethod

from job_scrapper_contracts import ScrapeJobsRequest


class IScrapeRequestDecoder(ABC):
    """Decodes raw message payloads into scrape job requests."""

    @abstractmethod
    def decode(self, payload: bytes) -> ScrapeJobsRequest:
        """Convert raw payload bytes into a scrape jobs request."""
