"""Tests for JSONScrapeRequestDecoder."""

import json

import pytest

from scrapper_messaging.request_decoder import JSONScrapeRequestDecoder


def test_json_decoder_parses_valid_payload():
    payload = json.dumps({"salary": 5000, "employment": "remote", "timeout": 45}).encode("utf-8")
    decoder = JSONScrapeRequestDecoder()

    request = decoder.decode(payload)

    assert request["salary"] == 5000
    assert request["employment"] == "remote"
    assert request["timeout"] == 45


def test_json_decoder_raises_on_invalid_payload():
    decoder = JSONScrapeRequestDecoder()

    with pytest.raises(ValueError, match="Failed to decode request payload as JSON"):
        decoder.decode(b"{not-json")


def test_json_decoder_handles_empty_object():
    payload = json.dumps({}).encode("utf-8")
    decoder = JSONScrapeRequestDecoder()

    request = decoder.decode(payload)

    assert request == {}


def test_json_decoder_handles_partial_fields():
    payload = json.dumps({"salary": 3000}).encode("utf-8")
    decoder = JSONScrapeRequestDecoder()

    request = decoder.decode(payload)

    assert request["salary"] == 3000
    assert request.get("employment") is None
