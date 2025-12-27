"""Tests for ScrapperServiceInvoker."""

from unittest.mock import Mock

from scrapper_messaging.service_invoker import ScrapperServiceInvoker


def test_scrapper_service_invoker_passes_arguments():
    service = Mock()
    invoker = ScrapperServiceInvoker(service)
    request = {"salary": 6000, "employment": "remote"}
    handler = Mock()

    invoker.invoke(request=request, batch_size=25, on_jobs_batch=handler)

    service.scrape_jobs.assert_called_once_with(
        salary=6000,
        employment="remote",
        posted_after=None,
        timeout=30,
        batch_size=25,
        on_jobs_batch=handler,
    )


def test_invoke_with_custom_timeout():
    service = Mock()
    invoker = ScrapperServiceInvoker(service)
    request = {"timeout": 60}
    handler = Mock()

    invoker.invoke(request=request, batch_size=10, on_jobs_batch=handler)

    service.scrape_jobs.assert_called_once_with(
        salary=0,
        employment="",
        posted_after=None,
        timeout=60,
        batch_size=10,
        on_jobs_batch=handler,
    )


def test_invoke_with_defaults():
    service = Mock()
    invoker = ScrapperServiceInvoker(service)
    request = {}
    handler = Mock()

    invoker.invoke(request=request, batch_size=50, on_jobs_batch=handler)

    service.scrape_jobs.assert_called_once_with(
        salary=0,
        employment="",
        posted_after=None,
        timeout=30,
        batch_size=50,
        on_jobs_batch=handler,
    )


def test_invoke_with_posted_after():
    service = Mock()
    invoker = ScrapperServiceInvoker(service)
    request = {"posted_after": "2024-01-15T10:30:00"}
    handler = Mock()

    invoker.invoke(request=request, batch_size=50, on_jobs_batch=handler)

    call_kwargs = service.scrape_jobs.call_args.kwargs
    assert call_kwargs["posted_after"].year == 2024
    assert call_kwargs["posted_after"].month == 1
    assert call_kwargs["posted_after"].day == 15


def test_invoke_returns_service_result():
    service = Mock()
    expected_jobs = [Mock(), Mock()]
    service.scrape_jobs.return_value = expected_jobs
    invoker = ScrapperServiceInvoker(service)
    request = {}
    handler = Mock()

    result = invoker.invoke(request=request, batch_size=25, on_jobs_batch=handler)

    assert result is expected_jobs
