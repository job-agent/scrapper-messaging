"""Tests for JSONScrapeRequestDecoder."""

import json
from unittest.mock import patch

import pytest

from scrapper_messaging.request_decoder import JSONScrapeRequestDecoder


def test_json_decoder_parses_valid_payload():
    payload = json.dumps({"filters": {"location": "remote"}, "timeout": 45}).encode("utf-8")
    decoder = JSONScrapeRequestDecoder()

    request = decoder.decode(payload)

    assert request["filters"]["location"] == "remote"
    assert request["timeout"] == 45


def test_json_decoder_raises_on_invalid_payload():
    decoder = JSONScrapeRequestDecoder()

    with pytest.raises(ValueError, match="Failed to decode request payload as JSON"):
        decoder.decode(b"{not-json")


def test_json_decoder_raises_on_invalid_schema():
    decoder = JSONScrapeRequestDecoder()

    with patch(
        "scrapper_messaging.request_decoder.json_scrape_request_decoder.ScrapeJobsRequest"
    ) as mock_request_class:
        mock_request_class.side_effect = TypeError("unexpected keyword argument")

        with pytest.raises(ValueError, match="does not match ScrapeJobsRequest schema"):
            decoder.decode(b'{"invalid_field": "value"}')
