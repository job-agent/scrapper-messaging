"""Defines the contract for invoking the scrapper service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, List, Optional

from job_scrapper_contracts import Job, ScrapeJobsRequest


class IJobsServiceInvoker(ABC):
    """Invokes the scrapper service and streams job batches."""

    @abstractmethod
    def invoke(
        self,
        *,
        request: ScrapeJobsRequest,
        batch_size: int,
        on_jobs_batch: Callable[[List[Job], bool], None],
    ) -> Optional[List[Job]]:
        """Execute the job scraping request and return the service result."""
