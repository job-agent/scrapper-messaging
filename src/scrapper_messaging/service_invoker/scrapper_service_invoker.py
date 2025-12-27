"""Concrete implementation of the jobs service invoker."""

from __future__ import annotations

from datetime import datetime
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
        timeout = request.get("timeout", 30)
        salary = request.get("salary", 0)
        employment = request.get("employment", "")
        posted_after_str = request.get("posted_after")
        posted_after: Optional[datetime] = None
        if posted_after_str:
            posted_after = datetime.fromisoformat(posted_after_str)

        return self._service.scrape_jobs(
            salary=salary,
            employment=employment,
            posted_after=posted_after,
            timeout=timeout,
            batch_size=batch_size,
            on_jobs_batch=on_jobs_batch,
        )
