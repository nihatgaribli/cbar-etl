"""Extract step: download daily CBAR bulletins as raw XML bytes.

Only HTTP concerns live here (timeouts, retries, response validation). Parsing the XML and
choosing the authoritative bulletin date belongs to the transform step.
"""
import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta

import requests

from etl.config import Settings

log = logging.getLogger(__name__)

USER_AGENT = "cbar-etl/0.1 (educational project)"


class ExtractError(Exception):
    """A bulletin could not be downloaded."""


class InvalidResponseError(ExtractError):
    """The server answered, but not with a CBAR XML bulletin.

    CBAR returns HTTP 200 with an HTML page for invalid or unavailable dates
    (README, finding #2), so the status code alone cannot be trusted.
    """


@dataclass(frozen=True)
class RawBulletin:
    requested_date: date
    content: bytes


@dataclass
class ExtractStats:
    requested: int = 0
    downloaded: int = 0
    failed_dates: list[date] = field(default_factory=list)


def daterange(start: date, end: date) -> Iterator[date]:
    """Yield every calendar day from start to end, inclusive."""
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


def validate_response(response: requests.Response) -> None:
    content_type = response.headers.get("Content-Type", "")
    if "xml" not in content_type:
        raise InvalidResponseError(f"expected XML, got {content_type or 'no Content-Type'!r}")
    if b"<ValCurs" not in response.content[:1024]:
        raise InvalidResponseError("XML document has no <ValCurs> root element")


class CbarClient:
    def __init__(
        self,
        settings: Settings,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)
        self.sleep = sleep
        self.stats = ExtractStats()

    def url_for(self, day: date) -> str:
        return f"{self.settings.cbar_base_url}/{day:%d.%m.%Y}.xml"

    def fetch(self, day: date) -> bytes:
        """Download one bulletin, retrying network errors and 5xx responses with backoff."""
        url = self.url_for(day)
        attempts = self.settings.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                response = self.session.get(url, timeout=self.settings.request_timeout)
                if response.status_code >= 500:
                    raise requests.HTTPError(f"server error {response.status_code}")
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                if attempt == attempts:
                    raise ExtractError(f"{url}: {exc} (after {attempts} attempts)") from exc
                wait = self.settings.retry_backoff * 2 ** (attempt - 1)
                log.warning("%s: %s, retry %d/%d in %.1fs",
                            url, exc, attempt, self.settings.max_retries, wait)
                self.sleep(wait)
                continue

            if response.status_code != 200:
                raise ExtractError(f"{url}: unexpected status {response.status_code}")
            validate_response(response)
            return response.content

        raise AssertionError("unreachable")

    def fetch_range(self, start: date, end: date) -> Iterator[RawBulletin]:
        """Download every day in [start, end]. Failed days are logged and skipped."""
        if start > end:
            raise ValueError(f"start {start} is after end {end}")
        log.info("Requesting %d days: %s -> %s", (end - start).days + 1, start, end)

        for index, day in enumerate(daterange(start, end)):
            if index and self.settings.request_delay:
                self.sleep(self.settings.request_delay)
            self.stats.requested += 1
            try:
                content = self.fetch(day)
            except ExtractError as exc:
                log.error("Skipping %s: %s", day, exc)
                self.stats.failed_dates.append(day)
                continue
            self.stats.downloaded += 1
            log.debug("Downloaded %s (%d bytes)", day, len(content))
            yield RawBulletin(requested_date=day, content=content)
