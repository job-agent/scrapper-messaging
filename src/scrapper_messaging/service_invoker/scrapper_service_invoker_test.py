"""Tests for ScrapperServiceInvoker."""

from unittest.mock import Mock

from scrapper_messaging.service_invoker import ScrapperServiceInvoker


def test_scrapper_service_invoker_passes_arguments():
    service = Mock()
    invoker = ScrapperServiceInvoker(service)
    filters = {"min_salary": 6000, "employment_location": "remote"}
    request = {"filters": filters}
    handler = Mock()

    invoker.invoke(request=request, batch_size=25, on_jobs_batch=handler)

    service.scrape_jobs.assert_called_once_with(
        filters=filters,
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
        filters={},
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
        filters={},
        timeout=30,
        batch_size=50,
        on_jobs_batch=handler,
    )


def test_invoke_with_posted_after():
    service = Mock()
    invoker = ScrapperServiceInvoker(service)
    filters = {"posted_after": "2024-01-15T10:30:00"}
    request = {"filters": filters}
    handler = Mock()

    invoker.invoke(request=request, batch_size=50, on_jobs_batch=handler)

    call_kwargs = service.scrape_jobs.call_args.kwargs
    assert call_kwargs["filters"]["posted_after"] == "2024-01-15T10:30:00"


def test_invoke_returns_service_result():
    service = Mock()
    expected_jobs = [Mock(), Mock()]
    service.scrape_jobs.return_value = expected_jobs
    invoker = ScrapperServiceInvoker(service)
    request = {}
    handler = Mock()

    result = invoker.invoke(request=request, batch_size=25, on_jobs_batch=handler)

    assert result is expected_jobs
