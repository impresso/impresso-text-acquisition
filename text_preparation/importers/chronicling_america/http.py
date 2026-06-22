"""HTTP client with rate limiting and resilient file downloads."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Any

import requests

from text_preparation.importers.chronicling_america.constants import (
    CHALLENGE_MARKERS,
    DEFAULT_DIRECTORY_DELAY,
    DEFAULT_MAX_REQUESTS_PER_MINUTE,
    DEFAULT_REQUEST_DELAY,
    HEADERS,
    LOC_RATE_LIMIT_BLOCK_SECONDS,
)

logger = logging.getLogger(__name__)


# Includes Cloudflare-specific 52x codes (e.g. 525 SSL handshake failed) which
# tile.loc.gov intermittently returns and which are safe to retry.
def is_transient_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def is_challenge_response(response: requests.Response) -> bool:
    """True when LOC/Cloudflare returns an HTML bot challenge instead of data."""
    if response.status_code not in (403, 429):
        return False
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type.lower():
        return False
    snippet = response.text[:4096].lower()
    return any(marker in snippet for marker in CHALLENGE_MARKERS)


def is_batch_directory_listing(url: str) -> bool:
    """True for batch index pages (HTML listings), not METS/ALTO/tarball files."""
    if "/data/batches/" not in url:
        return False
    last_segment = url.rstrip("/").rsplit("/", 1)[-1]
    if not last_segment:
        return True
    return "." not in last_segment


class TieredHttpClient:
    """Routes requests to separate crawl vs asset rate limiters.

    HTML batch directory listings use the crawl client (conservative cap).
    METS, tarballs, manifests, and other file GETs use the asset client.
    """

    def __init__(self, crawl_client: HttpClient, asset_client: HttpClient) -> None:
        self.crawl_client = crawl_client
        self.asset_client = asset_client

    def _client_for(self, url: str) -> HttpClient:
        if is_batch_directory_listing(url):
            return self.crawl_client
        return self.asset_client

    def request(self, url: str, method: str = "GET", **kwargs: Any) -> requests.Response:
        return self._client_for(url).request(url, method=method, **kwargs)


class HttpClient:
    """HTTP client with polite delays, timeouts, and retry/backoff."""

    def __init__(
        self,
        delay: float = DEFAULT_REQUEST_DELAY,
        max_retries: int = 5,
        timeout: float = 120.0,
        max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE,
        directory_delay: float = DEFAULT_DIRECTORY_DELAY,
        session: requests.Session | None = None,
    ) -> None:
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_requests_per_minute = max_requests_per_minute
        self.directory_delay = directory_delay
        self.session = session or requests.Session()
        self.session.headers.update(HEADERS)
        self._lock = threading.Lock()
        self._last_request_at = 0.0
        self._request_times: deque[float] = deque()

    def _effective_delay(self, url: str) -> float:
        if is_batch_directory_listing(url):
            return max(self.delay, self.directory_delay)
        return self.delay

    def _rate_limit_wait(self, url: str) -> float:
        """Enforce minimum spacing and a sliding-window requests/minute cap."""
        total_wait = 0.0
        now = time.monotonic()
        min_spacing = self._effective_delay(url)

        while self._request_times and now - self._request_times[0] >= 60.0:
            self._request_times.popleft()

        if len(self._request_times) >= self.max_requests_per_minute:
            wait = 60.0 - (now - self._request_times[0]) + 0.05
            time.sleep(wait)
            total_wait += wait
            now = time.monotonic()
            while self._request_times and now - self._request_times[0] >= 60.0:
                self._request_times.popleft()

        spacing = min_spacing - (now - self._last_request_at)
        if spacing > 0:
            time.sleep(spacing)
            total_wait += spacing

        return total_wait

    def _backoff_for_status(self, response: requests.Response, backoff: float) -> float:
        if response.status_code != 429 and not is_challenge_response(response):
            return backoff
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return max(backoff, float(retry_after))
        # LOC documents a 1-hour block when rate limits are exceeded.
        return max(backoff, float(LOC_RATE_LIMIT_BLOCK_SECONDS))

    def request(self, url: str, method: str = "GET", **kwargs: Any) -> requests.Response:
        retries = 0
        backoff = 2.0
        kwargs.setdefault("timeout", self.timeout)
        while retries < self.max_retries:
            try:
                with self._lock:
                    self._rate_limit_wait(url)
                    response = (
                        self.session.get(url, **kwargs)
                        if method == "GET"
                        else self.session.head(url, **kwargs)
                    )
                    now = time.monotonic()
                    self._last_request_at = now
                    self._request_times.append(now)
                if is_transient_status(response.status_code) or is_challenge_response(
                    response
                ):
                    backoff = self._backoff_for_status(response, backoff)
                    reason = (
                        "CAPTCHA/challenge"
                        if is_challenge_response(response)
                        else f"HTTP {response.status_code}"
                    )
                    logger.warning(
                        "%s for %s; sleeping %.0fs before retry",
                        reason,
                        url,
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    retries += 1
                    continue
                return response
            except requests.RequestException as exc:
                logger.warning("Request failed for %s: %s", url, exc)
                time.sleep(backoff)
                backoff *= 2.0
                retries += 1
        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts")


def make_http_client(
    *,
    delay: float = DEFAULT_REQUEST_DELAY,
    max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE,
    directory_delay: float = DEFAULT_DIRECTORY_DELAY,
    asset_delay: float | None = None,
    asset_max_requests_per_minute: int | None = None,
) -> HttpClient | TieredHttpClient:
    """Build a single or tier-separated HTTP client.

    When ``asset_max_requests_per_minute`` or ``asset_delay`` differ from the
    crawl settings, returns a :class:`TieredHttpClient` so directory crawls stay
    conservative while METS/tarball GETs can use a higher ceiling.
    """
    asset_rpm = (
        asset_max_requests_per_minute
        if asset_max_requests_per_minute is not None
        else max_requests_per_minute
    )
    asset_d = asset_delay if asset_delay is not None else delay
    if asset_rpm == max_requests_per_minute and asset_d == delay:
        return HttpClient(
            delay=delay,
            max_requests_per_minute=max_requests_per_minute,
            directory_delay=directory_delay,
        )
    crawl_client = HttpClient(
        delay=delay,
        max_requests_per_minute=max_requests_per_minute,
        directory_delay=directory_delay,
    )
    asset_client = HttpClient(
        delay=asset_d,
        max_requests_per_minute=asset_rpm,
        directory_delay=directory_delay,
    )
    return TieredHttpClient(crawl_client, asset_client)


def download_file(
    client: HttpClient | TieredHttpClient,
    url: str,
    dest_path: str,
    max_retries: int = 5,
) -> None:
    """Stream a URL to disk, retrying on mid-stream connection drops.

    Downloads to a temporary file first and renames on success so partially
    written files never look complete to the resume logic.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    tmp_path = f"{dest_path}.part"
    backoff = 2.0
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.request(url, stream=True)
            response.raise_for_status()
            with open(tmp_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
            os.replace(tmp_path, dest_path)
            return
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and is_transient_status(status):
                last_exc = exc
                logger.warning(
                    "Download of %s failed with HTTP %s (attempt %d/%d)",
                    url,
                    status,
                    attempt,
                    max_retries,
                )
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                time.sleep(backoff)
                backoff *= 2.0
                continue
            raise
        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            last_exc = exc
            logger.warning(
                "Download of %s interrupted (attempt %d/%d): %s",
                url,
                attempt,
                max_retries,
                exc,
            )
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            time.sleep(backoff)
            backoff *= 2.0
    raise RuntimeError(
        f"Failed to download {url} after {max_retries} attempts"
    ) from last_exc
