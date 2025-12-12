"""Concrete implementation of the jobs service invoker."""

from __future__ import annotations

from typing import Callable, List, Optional

from job_scrapper_contracts import Job, ScrapeJobsRequest, ScrapperServiceInterface

from scrapper_messaging.contracts import IJobsServiceInvoker


class ScrapperServiceInvoker(IJobsServiceInvoker):
    """Invokes the configured scrapper service."""

    def __init__(self, service: ScrapperServiceInterface) -> None:
        self._service = service

    def invoke(
        self,
        *,
        request: ScrapeJobsRequest,
        batch_size: int,
        on_jobs_batch: Callable[[List[Job], bool], None],
    ) -> Optional[List[Job]]:
        filters = request.get("filters") or {}
        timeout = request.get("timeout", 30)
        return self._service.scrape_jobs(
            filters=filters,
            timeout=timeout,
            batch_size=batch_size,
            on_jobs_batch=on_jobs_batch,
        )
