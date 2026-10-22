"""Unit tests for the extract step.

The real API is never called: a fake session replays prepared responses.
"""
from datetime import date
from pathlib import Path

import pytest
import requests

from etl.config import Settings
from etl.extract import CbarClient, ExtractError, InvalidResponseError, daterange

FIXTURES = Path(__file__).parent / "fixtures"
BULLETIN = (FIXTURES / "bulletin_2026-10-22.xml").read_bytes()
HTML_PAGE = b' <!DOCTYPE html><html lang="az"><head><meta charset="UTF-8"></head></html>'

SETTINGS = Settings(
    cbar_base_url="https://www.cbar.az/currencies",
    request_timeout=15,
    request_delay=0.2,
    max_retries=3,
    retry_backoff=1.0,
    log_level="INFO",
)


def make_response(status=200, content=BULLETIN, content_type="application/xml"):
    response = requests.Response()
    response.status_code = status
    response._content = content
    response.headers["Content-Type"] = content_type
    return response


class FakeSession:
    """Returns (or raises) the queued outcomes in order and records requested URLs."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.urls = []
        self.headers = {}

    def get(self, url, timeout):
        self.urls.append(url)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def make_client(*outcomes):
    sleeps = []
    client = CbarClient(SETTINGS, session=FakeSession(*outcomes), sleep=sleeps.append)
    return client, sleeps


def test_url_uses_cbar_date_format():
    client, _ = make_client()
    assert client.url_for(date(2026, 1, 5)) == "https://www.cbar.az/currencies/05.01.2026.xml"


def test_fetch_returns_xml_bytes():
    client, sleeps = make_client(make_response())
    assert client.fetch(date(2026, 10, 22)) == BULLETIN
    assert sleeps == []


def test_html_with_status_200_is_rejected():
    # finding #2: invalid dates return HTTP 200 with an HTML page
    html = make_response(content=HTML_PAGE, content_type="text/html; charset=UTF-8")
    client, _ = make_client(html)
    with pytest.raises(InvalidResponseError, match="expected XML"):
        client.fetch(date(2026, 2, 28))


def test_xml_without_valcurs_root_is_rejected():
    client, _ = make_client(make_response(content=b"<?xml version='1.0'?><error/>"))
    with pytest.raises(InvalidResponseError, match="ValCurs"):
        client.fetch(date(2026, 10, 22))


def test_server_error_is_retried_with_exponential_backoff():
    client, sleeps = make_client(
        make_response(status=503),
        requests.ConnectionError("connection reset"),
        make_response(),
    )
    assert client.fetch(date(2026, 10, 22)) == BULLETIN
    assert sleeps == [1.0, 2.0]


def test_gives_up_after_max_retries():
    client, sleeps = make_client(*[requests.Timeout("read timed out")] * 4)
    with pytest.raises(ExtractError, match="after 4 attempts"):
        client.fetch(date(2026, 10, 22))
    assert sleeps == [1.0, 2.0, 4.0]


def test_client_error_is_not_retried():
    client, sleeps = make_client(make_response(status=404, content=b""))
    with pytest.raises(ExtractError, match="unexpected status 404"):
        client.fetch(date(2026, 10, 22))
    assert sleeps == []


def test_fetch_range_requests_every_day_with_polite_delay():
    client, sleeps = make_client(make_response(), make_response(), make_response())
    bulletins = list(client.fetch_range(date(2026, 10, 16), date(2026, 10, 18)))

    assert [b.requested_date for b in bulletins] == [
        date(2026, 10, 16), date(2026, 10, 17), date(2026, 10, 18),
    ]
    assert client.session.urls[-1].endswith("/18.10.2026.xml")
    assert sleeps == [0.2, 0.2]
    assert client.stats.requested == client.stats.downloaded == 3


def test_fetch_range_skips_failed_days():
    client, _ = make_client(
        make_response(),
        make_response(content=HTML_PAGE, content_type="text/html"),
        make_response(),
    )
    bulletins = list(client.fetch_range(date(2026, 10, 20), date(2026, 10, 22)))

    assert len(bulletins) == 2
    assert client.stats.failed_dates == [date(2026, 10, 21)]


def test_fetch_range_rejects_reversed_range():
    client, _ = make_client()
    with pytest.raises(ValueError):
        list(client.fetch_range(date(2026, 10, 22), date(2026, 10, 1)))


def test_daterange_is_inclusive():
    assert list(daterange(date(2026, 2, 27), date(2026, 3, 1))) == [
        date(2026, 2, 27), date(2026, 2, 28), date(2026, 3, 1),
    ]
