"""JSON implementation of the scrape request decoder."""

from __future__ import annotations

import json
from typing import Any, Dict

from job_scrapper_contracts import ScrapeJobsRequest

from scrapper_messaging.contracts import IScrapeRequestDecoder


class JSONScrapeRequestDecoder(IScrapeRequestDecoder):
    """Decodes JSON payloads into scrape job requests."""

    def decode(self, payload: bytes) -> ScrapeJobsRequest:
        try:
            raw_request: Dict[str, Any] = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("Failed to decode request payload as JSON.") from exc

        try:
            return ScrapeJobsRequest(**raw_request)
        except TypeError as exc:
            raise ValueError("Request payload does not match ScrapeJobsRequest schema.") from exc
