"""Bulk download pipeline for Chronicling America METS/ALTO data.

Uses per-batch OCR tarballs (ALTO XML) plus a lightweight METS crawl per issue.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tarfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import date
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from text_preparation.importers.chronicling_america.telegram_notify import (
    TelegramNotifier,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://chroniclingamerica.loc.gov"
# LOC migrated OCR manifests in 2025: /ocr.json now 404s after redirect; the live
# manifest is at /data/ocr/ocr.json with a list-shaped schema (archive_name, batch).
OCR_JSON_URLS = (
    f"{BASE_URL}/data/ocr/ocr.json",
    f"{BASE_URL}/ocr.json",
)
BATCHES_URL = f"{BASE_URL}/data/batches/"

# Official LOC rate limits (https://www.loc.gov/apis/json-and-yaml/working-within-limits/):
# JSON/YAML API: 20 req/min; storage/text/image: 150 req/min; block time: 1 hour.
# chroniclingamerica.loc.gov bulk crawls are throttled similarly in practice; stay
# under the JSON API limit to avoid 429 / CAPTCHA blocks.
LOC_JSON_API_REQUESTS_PER_MINUTE = 20
LOC_RATE_LIMIT_BLOCK_SECONDS = 3600
DEFAULT_REQUEST_DELAY = 60.0 / LOC_JSON_API_REQUESTS_PER_MINUTE  # 3.0 s
# Stay well under the official 20/min ceiling; directory crawls trigger CAPTCHA
# faster than tarball or XML file downloads in practice.
DEFAULT_MAX_REQUESTS_PER_MINUTE = 8
# Extra minimum spacing for HTML directory listings under /data/batches/.
DEFAULT_DIRECTORY_DELAY = 6.0
# Pause after enumerating reels for one batch/LCCN before METS downloads.
BATCH_ENUMERATION_COOLDOWN = 120.0
# Pause between finishing one OCR tarball and starting the next.
TARBALL_COOLDOWN = 180.0
# After this many METS downloads, pause to avoid sustained crawl bursts.
METS_BURST_SIZE = 15
METS_BURST_PAUSE = 90.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ImpressoTextPreparation/1.0; "
        "+https://github.com/impresso/impresso-text-acquisition)"
    )
}

CHALLENGE_MARKERS = (
    "captcha",
    "just a moment",
    "challenge-platform",
    "cloudflare",
)


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


ISSUE_DIR_RE = re.compile(r"^\d{10}$")
TARBALL_BATCH_RE = re.compile(r"^batch_(.+)\.tar\.bz2$")
PLAIN_TARBALL_RE = re.compile(r"^(.+_ver\d+)\.tar\.bz2$")
BATCH_VERSION_RE = re.compile(r"^(.+)_ver(\d+)$")
# LCCNs are either prefixed (sn83045462, mn99999999) or purely numeric (2010218500)
LCCN_RE = re.compile(r"^[a-z]{0,3}\d{8,12}$")


@dataclass(frozen=True)
class TitleSpec:
    lccn: str
    alias: str
    start_date: date | None = None
    end_date: date | None = None


@dataclass(frozen=True)
class TarballInfo:
    batch: str
    url: str
    sha1: str
    size: int
    lccns: tuple[str, ...] = ()
    issue_count: int = 0


@dataclass(frozen=True)
class IssueInfo:
    batch: str
    lccn: str
    alias: str
    date: str
    edition: str
    issue_dir_name: str
    url: str


@dataclass
class DownloadPlan:
    titles: list[TitleSpec]
    batches: list[str]
    tarballs: list[TarballInfo]
    issues: list[IssueInfo]
    total_tarball_bytes: int = 0
    estimated_issues: int | None = None


@dataclass
class DownloadState:
    completed_tarballs: dict[str, str] = field(default_factory=dict)
    completed_issues: set[str] = field(default_factory=set)

    @staticmethod
    def load(path: str) -> DownloadState:
        if not os.path.exists(path):
            return DownloadState()
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return DownloadState(
            completed_tarballs=data.get("completed_tarballs", {}),
            completed_issues=set(data.get("completed_issues", [])),
        )

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "completed_tarballs": self.completed_tarballs,
                    "completed_issues": sorted(self.completed_issues),
                },
                f,
                indent=2,
            )


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
        notifier: TelegramNotifier | None = None,
    ) -> None:
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_requests_per_minute = max_requests_per_minute
        self.directory_delay = directory_delay
        self.notifier = notifier
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
                    if is_challenge_response(response) and self.notifier:
                        self.notifier.notify_captcha(url, sleep_seconds=backoff)
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


def load_titles_config(path: str) -> list[TitleSpec]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    titles: list[TitleSpec] = []
    for entry in data.get("titles", []):
        start = entry.get("start_date")
        end = entry.get("end_date")
        titles.append(
            TitleSpec(
                lccn=entry["lccn"],
                alias=entry["alias"],
                start_date=date.fromisoformat(start) if start else None,
                end_date=date.fromisoformat(end) if end else None,
            )
        )
    return titles


def title_from_legacy(lccn: str, alias: str) -> TitleSpec:
    return TitleSpec(lccn=lccn, alias=alias)


def batch_family(batch: str) -> str:
    match = BATCH_VERSION_RE.match(batch)
    return match.group(1) if match else batch


def batch_version(batch: str) -> int:
    match = BATCH_VERSION_RE.match(batch)
    return int(match.group(2)) if match else 0


def dedupe_batch_versions(batches: list[str]) -> list[str]:
    best: dict[str, tuple[int, str]] = {}
    for batch in batches:
        family = batch_family(batch)
        version = batch_version(batch)
        if family not in best or version > best[family][0]:
            best[family] = (version, batch)
    return sorted(value[1] for value in best.values())


def tarball_batch_name(filename: str) -> str | None:
    match = TARBALL_BATCH_RE.match(filename)
    if match:
        return match.group(1)
    plain = PLAIN_TARBALL_RE.match(filename)
    return plain.group(1) if plain else None


def parse_ocr_manifest(payload: Any) -> list[TarballInfo]:
    """Parse OCR tarball metadata from legacy or post-migration loc.gov manifests."""
    if isinstance(payload, dict):
        entries = payload.get("ocr", [])
    elif isinstance(payload, list):
        entries = payload
    else:
        return []

    tarballs: list[TarballInfo] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        batch = entry.get("batch")
        if not batch:
            archive_name = entry.get("name") or entry.get("archive_name", "")
            batch = tarball_batch_name(archive_name) or archive_name.removesuffix(
                ".tar.bz2"
            )
        if not batch:
            continue
        url = entry.get("url")
        sha1 = entry.get("sha1")
        size = entry.get("size")
        if not url or not sha1 or size is None:
            continue
        raw_lccns = entry.get("lccns") or []
        issue_count = entry.get("issue_count") or 0
        tarballs.append(
            TarballInfo(
                batch=batch,
                url=url,
                sha1=sha1,
                size=int(size),
                lccns=tuple(
                    lccn
                    for lccn in raw_lccns
                    if isinstance(lccn, str) and LCCN_RE.match(lccn)
                ),
                issue_count=int(issue_count),
            )
        )
    return tarballs


def index_from_tarball_manifest(tarballs: list[TarballInfo]) -> dict[str, list[str]]:
    """Build a batch→LCCN index from OCR manifest metadata (no HTTP crawl)."""
    index: dict[str, list[str]] = {}
    for info in tarballs:
        if info.lccns:
            index[info.batch] = sorted(info.lccns)
    return index


def fetch_ocr_tarballs(client: HttpClient) -> list[TarballInfo]:
    last_error: Exception | None = None
    for manifest_url in OCR_JSON_URLS:
        try:
            response = client.request(manifest_url)
            response.raise_for_status()
            tarballs = parse_ocr_manifest(response.json())
            if tarballs:
                return tarballs
        except (requests.RequestException, ValueError, KeyError) as exc:
            last_error = exc
            logger.warning("Failed to load OCR manifest from %s: %s", manifest_url, exc)
    raise RuntimeError(
        "Could not load Chronicling America OCR tarball manifest from any known URL"
    ) from last_error


def _parse_directory_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for anchor in soup.find_all("a"):
        text = anchor.text.strip().rstrip("/")
        href = anchor.get("href", "").strip()
        if not text:
            continue
        if text in (".", "..", "../", "Parent Directory"):
            continue
        if href in ("../", "./", "/"):
            continue
        links.append(text)
    return links


def fetch_batch_lccns(client: HttpClient, batch: str) -> set[str]:
    url = f"{BATCHES_URL}{batch}/data/"
    response = client.request(url)
    if response.status_code == 404:
        return set()
    response.raise_for_status()
    return {
        link
        for link in _parse_directory_links(response.text)
        if LCCN_RE.match(link)
    }


def build_or_load_batch_index(
    client: HttpClient,
    index_path: str,
    batches: list[str] | None = None,
    manifest_index: dict[str, list[str]] | None = None,
) -> dict[str, list[str]]:
    if os.path.exists(index_path):
        with open(index_path, encoding="utf-8") as f:
            cached = json.load(f)
        return {batch: sorted(lccns) for batch, lccns in cached.items()}

    if batches is None:
        response = client.request(BATCHES_URL)
        response.raise_for_status()
        batches = [
            link
            for link in _parse_directory_links(response.text)
            if "_ver" in link
        ]

    index: dict[str, list[str]] = dict(manifest_index or {})
    batches_to_crawl = [batch for batch in sorted(batches) if batch not in index]
    if not batches_to_crawl:
        logger.info(
            "Built batch index from OCR manifest metadata for %d batches (no crawl)",
            len(index),
        )
    for idx, batch in enumerate(batches_to_crawl, start=1):
        logger.info("Indexing batch %s (%d/%d)", batch, idx, len(batches_to_crawl))
        lccns = fetch_batch_lccns(client, batch)
        if lccns:
            index[batch] = sorted(lccns)

    os.makedirs(os.path.dirname(index_path) or ".", exist_ok=True)
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)
    return index


def list_all_batches(client: HttpClient) -> list[str]:
    response = client.request(BATCHES_URL)
    response.raise_for_status()
    return sorted(
        link for link in _parse_directory_links(response.text) if "_ver" in link
    )


def batch_contains_lccn(client: HttpClient, batch: str, lccn: str) -> bool:
    url = f"{BATCHES_URL}{batch}/data/{lccn}/"
    response = client.request(url)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return bool(_parse_directory_links(response.text))


def find_first_batch_for_lccn(client: HttpClient, lccn: str) -> str | None:
    """Find the first batch whose data directory actually lists reels for the LCCN.

    Only suitable for sampling; the bulk pipeline uses the full cached index instead.
    """
    batches = list_all_batches(client)
    ordered = sorted(batches, key=lambda b: (0 if b.startswith("dlc_") else 1, b))
    for batch in ordered:
        if batch_contains_lccn(client, batch, lccn):
            return batch
    return None


def batches_for_lccns(
    index: dict[str, list[str]],
    lccns: set[str],
) -> list[str]:
    matching = [
        batch
        for batch, batch_lccns in index.items()
        if lccns.intersection(batch_lccns)
    ]
    return dedupe_batch_versions(matching)


def issue_in_date_range(issue_date: str, title: TitleSpec) -> bool:
    parsed = date.fromisoformat(issue_date)
    if title.start_date and parsed < title.start_date:
        return False
    if title.end_date and parsed > title.end_date:
        return False
    return True


def list_issues_in_batch(
    client: HttpClient,
    batch: str,
    title: TitleSpec,
) -> list[IssueInfo]:
    lccn_url = f"{BATCHES_URL}{batch}/data/{title.lccn}/"
    response = client.request(lccn_url)
    if response.status_code == 404:
        return []
    response.raise_for_status()

    issues: list[IssueInfo] = []
    for reel in _parse_directory_links(response.text):
        reel_url = f"{lccn_url}{reel}/"
        reel_response = client.request(reel_url)
        reel_response.raise_for_status()
        for issue_dir in _parse_directory_links(reel_response.text):
            if not ISSUE_DIR_RE.match(issue_dir):
                continue
            issue_date = f"{issue_dir[:4]}-{issue_dir[4:6]}-{issue_dir[6:8]}"
            if not issue_in_date_range(issue_date, title):
                continue
            edition = f"ed-{int(issue_dir[8:10])}"
            issues.append(
                IssueInfo(
                    batch=batch,
                    lccn=title.lccn,
                    alias=title.alias,
                    date=issue_date,
                    edition=edition,
                    issue_dir_name=issue_dir,
                    url=f"{reel_url}{issue_dir}/",
                )
            )
    return issues


def issue_dir_name_from_date_edition(issue_date: str, edition: str) -> str:
    year, month, day = issue_date.split("-")
    ed_num = int(edition.replace("ed-", ""))
    return f"{year}{month}{day}{ed_num:02d}"


def issue_info_from_tarball_key(
    batch: str,
    issue_key: str,
    alias_by_lccn: dict[str, str],
) -> IssueInfo:
    lccn, issue_date, edition = issue_key.split("/", 2)
    return IssueInfo(
        batch=batch,
        lccn=lccn,
        alias=alias_by_lccn[lccn],
        date=issue_date,
        edition=edition,
        issue_dir_name=issue_dir_name_from_date_edition(issue_date, edition),
        url="",
    )


def issues_from_tarball_extraction(
    extracted: dict[str, dict[int, bytes]],
    batch: str,
    titles: list[TitleSpec],
    alias_by_lccn: dict[str, str],
) -> list[IssueInfo]:
    title_by_lccn = {title.lccn: title for title in titles}
    issues: list[IssueInfo] = []
    for issue_key in sorted(extracted):
        lccn = issue_key.split("/", 1)[0]
        title = title_by_lccn.get(lccn)
        if title is None:
            continue
        issue = issue_info_from_tarball_key(batch, issue_key, alias_by_lccn)
        if issue_in_date_range(issue.date, title):
            issues.append(issue)
    return issues


def issue_url_cache_path(state_dir: str, batch: str, lccn: str) -> str:
    return os.path.join(state_dir, "issue_urls", f"{batch}_{lccn}.json")


def load_issue_url_cache(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def save_issue_url_cache(path: str, mapping: dict[str, str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(mapping, handle, indent=2)


def enumerate_issue_urls(
    client: HttpClient,
    batch: str,
    title: TitleSpec,
    cache_path: str,
    *,
    needed: set[str] | None = None,
) -> dict[str, str]:
    """Map issue_dir_name → issue base URL, with incremental on-disk cache.

    Only crawls ``chroniclingamerica.loc.gov/data/batches/{batch}/data/{lccn}/``
    directory listings. When *needed* is provided, stops as soon as every
    requested issue_dir_name has been found (typically after a handful of reel
    listings instead of the full batch).
    """
    cached = load_issue_url_cache(cache_path)
    if needed and needed.issubset(cached):
        return cached

    lccn_url = f"{BATCHES_URL}{batch}/data/{title.lccn}/"
    response = client.request(lccn_url)
    if response.status_code == 404:
        save_issue_url_cache(cache_path, cached)
        return cached
    response.raise_for_status()

    for reel in _parse_directory_links(response.text):
        if needed and needed.issubset(cached):
            break
        reel_url = f"{lccn_url}{reel}/"
        reel_response = client.request(reel_url)
        reel_response.raise_for_status()
        for issue_dir in _parse_directory_links(reel_response.text):
            if not ISSUE_DIR_RE.match(issue_dir):
                continue
            if issue_dir not in cached:
                cached[issue_dir] = f"{reel_url}{issue_dir}/"
        save_issue_url_cache(cache_path, cached)

    if BATCH_ENUMERATION_COOLDOWN > 0:
        logger.info(
            "Pausing %.0fs after enumerating %s/%s",
            BATCH_ENUMERATION_COOLDOWN,
            batch,
            title.lccn,
        )
        time.sleep(BATCH_ENUMERATION_COOLDOWN)

    return cached


def resolve_issue_urls(
    client: HttpClient,
    batch: str,
    title: TitleSpec,
    issues: list[IssueInfo],
    state_dir: str,
) -> list[IssueInfo]:
    if not issues:
        return []
    cache_path = issue_url_cache_path(state_dir, batch, title.lccn)
    needed = {issue.issue_dir_name for issue in issues}
    url_map = enumerate_issue_urls(
        client,
        batch,
        title,
        cache_path,
        needed=needed,
    )
    resolved: list[IssueInfo] = []
    for issue in issues:
        base_url = url_map.get(issue.issue_dir_name, "")
        if not base_url:
            logger.warning(
                "No batch URL for %s in %s/%s",
                issue.issue_dir_name,
                batch,
                title.lccn,
            )
            continue
        resolved.append(replace(issue, url=base_url))
    return resolved


def dedupe_issues(issues: list[IssueInfo]) -> list[IssueInfo]:
    best: dict[str, IssueInfo] = {}
    for issue in issues:
        key = f"{issue.lccn}/{issue.date}/{issue.edition}"
        current = best.get(key)
        if current is None:
            best[key] = issue
            continue
        if batch_version(issue.batch) > batch_version(current.batch):
            best[key] = issue
    return sorted(best.values(), key=lambda i: (i.alias, i.date, i.edition))


def build_download_plan(
    client: HttpClient,
    titles: list[TitleSpec],
    index_path: str,
    *,
    dry_run: bool = False,
) -> DownloadPlan:
    tarballs = fetch_ocr_tarballs(client)
    tarball_by_batch = {info.batch: info for info in tarballs}
    manifest_index = index_from_tarball_manifest(tarballs)
    index = build_or_load_batch_index(
        client,
        index_path,
        batches=sorted(tarball_by_batch.keys()),
        manifest_index=manifest_index,
    )

    selected_batches = batches_for_lccns(
        index,
        {title.lccn for title in titles},
    )
    selected_tarballs = [
        tarball_by_batch[batch]
        for batch in selected_batches
        if batch in tarball_by_batch
    ]

    estimated_issues: int | None = None
    if dry_run:
        # Estimate from the OCR manifest's per-batch issue_count (no extra HTTP).
        # This is a batch-level upper bound: batches are not date-filtered and may
        # bundle multiple LCCNs. The loc.gov JSON API cannot be used here because
        # www.loc.gov is behind a Cloudflare bot challenge (403 for API clients).
        estimated_issues = sum(info.issue_count for info in selected_tarballs)
        logger.info(
            "Dry-run: estimated up to %d issues from OCR manifest (batch-level upper bound)",
            estimated_issues,
        )
    else:
        # Issue discovery happens lazily per tarball during download (see
        # run_bulk_download) to avoid a long upfront directory crawl that
        # triggers LOC/Cloudflare CAPTCHA blocks.
        logger.info(
            "Skipping upfront issue enumeration; issues will be derived from OCR tarballs"
        )

    issues: list[IssueInfo] = []
    total_bytes = sum(info.size for info in selected_tarballs)
    return DownloadPlan(
        titles=titles,
        batches=selected_batches,
        tarballs=selected_tarballs,
        issues=issues,
        total_tarball_bytes=total_bytes,
        estimated_issues=estimated_issues,
    )


def format_bytes(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def format_dry_run_report(plan: DownloadPlan) -> str:
    lines = [
        "Chronicling America bulk download plan",
        "=" * 40,
    ]
    for title in plan.titles:
        date_range = ""
        if title.start_date or title.end_date:
            date_range = f" ({title.start_date or '...'} to {title.end_date or '...'})"
        lines.append(f"- {title.alias} [{title.lccn}]{date_range}")
    lines.append(f"Batches: {len(plan.batches)}")
    lines.append(
        f"ALTO (OCR tarballs): {len(plan.tarballs)} "
        f"({format_bytes(plan.total_tarball_bytes)} compressed)"
    )
    lines.append(
        "  Source: chroniclingamerica.loc.gov/data/ocr/*.tar.bz2 "
        "(per-page ALTO XML, extracted and renamed to METS hrefs)"
    )
    if plan.issues:
        lines.append(f"METS (per-issue crawl): {len(plan.issues)}")
    elif plan.estimated_issues is not None:
        lines.append(
            f"METS (per-issue crawl): ~{plan.estimated_issues} "
            "(OCR manifest estimate, batch-level upper bound)"
        )
    else:
        lines.append("METS (per-issue crawl): unknown")
    lines.append(
        "  Source: chroniclingamerica.loc.gov/data/batches/{batch}/data/…/{issue}.xml"
    )
    if plan.batches:
        lines.extend(["", "Batch list:"])
        lines.extend(f"  - {batch}" for batch in plan.batches)
    return "\n".join(lines) + "\n"


def print_dry_run_report(plan: DownloadPlan) -> None:
    print(format_dry_run_report(plan), end="")


def verify_sha1(path: str, expected: str) -> bool:
    digest = hashlib.sha1()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest() == expected.lower()


def download_file(
    client: HttpClient,
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


def extract_alto_members(
    tarball_path: str,
    lccns: set[str],
) -> dict[str, dict[int, bytes]]:
    """Return {issue_key: {seq_num: alto_xml_bytes}} extracted from tarball."""
    tarball_size = os.path.getsize(tarball_path)
    logger.info(
        "Streaming ALTO from %s (%s)",
        os.path.basename(tarball_path),
        format_bytes(tarball_size),
    )
    extracted: dict[str, dict[int, bytes]] = {}
    member_count = 0
    alto_count = 0
    started_at = time.monotonic()
    last_log_at = started_at
    with tarfile.open(tarball_path, mode="r:bz2") as archive:
        for member in archive:
            member_count += 1
            now = time.monotonic()
            if member_count % 5000 == 0 or now - last_log_at >= 60.0:
                logger.info(
                    "Tarball scan: %d members scanned, %d ALTO files kept (%.0fs)",
                    member_count,
                    alto_count,
                    now - started_at,
                )
                last_log_at = now
            if not member.isfile():
                continue
            # Expected layout: lccn/YYYY/MM/DD/ed-N/seq-N/ocr.xml
            parts = member.name.split("/")
            if len(parts) != 7:
                continue
            lccn, year, month, day, edition, seq_dir, filename = parts
            if lccn not in lccns:
                continue
            if not filename.endswith(".xml"):
                continue
            seq_match = re.match(r"seq-(\d+)$", seq_dir)
            if not seq_match:
                continue
            issue_key = f"{lccn}/{year}-{month}-{day}/{edition}"
            file_obj = archive.extractfile(member)
            if file_obj is None:
                continue
            content = file_obj.read()
            extracted.setdefault(issue_key, {})[int(seq_match.group(1))] = content
            alto_count += 1
    logger.info(
        "Tarball extraction done: %d issue(s), %d ALTO file(s) from %d member(s) (%.0fs)",
        len(extracted),
        alto_count,
        member_count,
        time.monotonic() - started_at,
    )
    return extracted


def parse_mets_alto_filenames(mets_xml: bytes) -> list[str]:
    soup = BeautifulSoup(mets_xml, "xml")
    file_map: dict[str, str] = {}
    file_sec = soup.find("fileSec")
    if file_sec:
        for file_tag in file_sec.find_all("file"):
            file_id = file_tag.get("ID")
            flocat = file_tag.find("FLocat")
            if file_id and flocat and flocat.get("xlink:href"):
                file_map[file_id] = os.path.basename(flocat["xlink:href"])

    ordered: list[str] = []
    struct_map = soup.find("structMap")
    if struct_map:
        page_divs = struct_map.find_all(
            "div",
            {"TYPE": lambda value: value and "page" in value.lower()},
        )
        for div in page_divs:
            fptr = div.find("fptr", {"FILEID": lambda value: value and value.startswith("ocrFile")})
            if not fptr:
                continue
            filename = file_map.get(fptr.get("FILEID", ""))
            if filename:
                ordered.append(filename)
    return ordered


def issue_local_dir(output_dir: str, issue: IssueInfo) -> str:
    year, month, day = issue.date.split("-")
    return os.path.join(output_dir, issue.alias, year, month, day, issue.edition)


def issue_state_key(issue: IssueInfo) -> str:
    return f"{issue.alias}/{issue.date}/{issue.edition}"


def write_issue_layout(
    output_dir: str,
    issue: IssueInfo,
    mets_bytes: bytes,
    alto_by_seq: dict[int, bytes],
) -> None:
    issue_dir = issue_local_dir(output_dir, issue)
    alto_dir = os.path.join(issue_dir, "alto")
    os.makedirs(alto_dir, exist_ok=True)

    mets_path = os.path.join(issue_dir, f"{issue.issue_dir_name}.xml")
    with open(mets_path, "wb") as handle:
        handle.write(mets_bytes)

    href_names = parse_mets_alto_filenames(mets_bytes)
    if href_names:
        for seq_num, alto_bytes in sorted(alto_by_seq.items()):
            if seq_num <= len(href_names):
                filename = href_names[seq_num - 1]
            else:
                filename = f"seq-{seq_num}.xml"
            with open(os.path.join(alto_dir, filename), "wb") as handle:
                handle.write(alto_bytes)
    else:
        for seq_num, alto_bytes in sorted(alto_by_seq.items()):
            with open(os.path.join(alto_dir, f"seq-{seq_num}.xml"), "wb") as handle:
                handle.write(alto_bytes)


def download_issue_mets(
    client: HttpClient,
    issue: IssueInfo,
    output_dir: str,
) -> None:
    if not issue.url:
        logger.warning(
            "Skipping METS for %s (no batch URL resolved)",
            issue_state_key(issue),
        )
        return
    issue_dir = issue_local_dir(output_dir, issue)
    mets_name = f"{issue.issue_dir_name}.xml"
    mets_path = os.path.join(issue_dir, mets_name)
    if os.path.exists(mets_path) and os.path.getsize(mets_path) > 0:
        return

    os.makedirs(issue_dir, exist_ok=True)
    mets_url = urljoin(issue.url, mets_name)
    logger.info("Downloading METS for %s", issue_state_key(issue))
    download_file(client, mets_url, mets_path)


def download_mets_with_pacing(
    client: HttpClient,
    issues: list[IssueInfo],
    output_dir: str,
    *,
    burst_size: int = METS_BURST_SIZE,
    burst_pause: float = METS_BURST_PAUSE,
) -> None:
    """Download METS files with periodic pauses to avoid sustained request bursts."""
    pending = list(issues)
    for index, issue in enumerate(pending, start=1):
        download_issue_mets(client, issue, output_dir)
        if burst_size > 0 and burst_pause > 0 and index % burst_size == 0:
            if index < len(pending):
                logger.info(
                    "Pausing %.0fs after %d METS downloads (burst limit)",
                    burst_pause,
                    index,
                )
                time.sleep(burst_pause)


def finalize_issue_from_tarball(
    output_dir: str,
    issue: IssueInfo,
    alto_by_seq: dict[int, bytes],
    state: DownloadState,
    state_path: str,
) -> None:
    key = issue_state_key(issue)
    if key in state.completed_issues:
        return

    issue_dir = issue_local_dir(output_dir, issue)
    mets_path = os.path.join(issue_dir, f"{issue.issue_dir_name}.xml")
    if not os.path.exists(mets_path):
        return

    with open(mets_path, "rb") as handle:
        mets_bytes = handle.read()
    write_issue_layout(output_dir, issue, mets_bytes, alto_by_seq)
    state.completed_issues.add(key)
    state.save(state_path)


def process_tarball(
    client: HttpClient,
    tarball: TarballInfo,
    titles: list[TitleSpec],
    output_dir: str,
    scratch_dir: str,
    state_dir: str,
    state: DownloadState,
    state_path: str,
    keep_tarballs: bool,
    *,
    mets_burst_size: int = METS_BURST_SIZE,
    mets_burst_pause: float = METS_BURST_PAUSE,
    notifier: TelegramNotifier | None = None,
) -> None:
    if tarball.sha1 in state.completed_tarballs:
        logger.info("Skipping tarball %s (already completed)", tarball.batch)
        return

    lccns = {title.lccn for title in titles}
    alias_by_lccn = {title.lccn: title.alias for title in titles}
    titles_by_lccn = {title.lccn: title for title in titles}
    tarball_path = os.path.join(
        scratch_dir,
        os.path.basename(tarball.url.rstrip("/").split("/")[-1]),
    )
    tarball_ready = False
    if os.path.exists(tarball_path):
        logger.info(
            "Verifying cached tarball %s (%s)",
            tarball.batch,
            format_bytes(os.path.getsize(tarball_path)),
        )
        if verify_sha1(tarball_path, tarball.sha1):
            logger.info("Reusing cached tarball %s at %s", tarball.batch, tarball_path)
            tarball_ready = True
        else:
            logger.warning(
                "Cached tarball %s failed SHA-1 check; re-downloading",
                tarball.batch,
            )
            os.remove(tarball_path)

    if not tarball_ready:
        logger.info(
            "Downloading tarball %s (%s)", tarball.batch, format_bytes(tarball.size)
        )
        download_file(client, tarball.url, tarball_path)
        if not verify_sha1(tarball_path, tarball.sha1):
            os.remove(tarball_path)
            raise RuntimeError(f"SHA-1 mismatch for tarball {tarball.batch}")

    extracted = extract_alto_members(tarball_path, lccns)
    batch_issues = issues_from_tarball_extraction(
        extracted,
        tarball.batch,
        titles,
        alias_by_lccn,
    )
    batch_issues = [
        issue
        for issue in batch_issues
        if issue_state_key(issue) not in state.completed_issues
    ]

    resolved_issues: list[IssueInfo] = []
    for lccn in sorted({issue.lccn for issue in batch_issues}):
        title = titles_by_lccn[lccn]
        lccn_issues = [issue for issue in batch_issues if issue.lccn == lccn]
        logger.info(
            "Resolving METS URLs for %d issue(s) in %s/%s",
            len(lccn_issues),
            tarball.batch,
            lccn,
        )
        resolved_issues.extend(
            resolve_issue_urls(client, tarball.batch, title, lccn_issues, state_dir)
        )

    logger.info(
        "Downloading METS for %d issue(s) from batch %s",
        len(resolved_issues),
        tarball.batch,
    )
    download_mets_with_pacing(
        client,
        resolved_issues,
        output_dir,
        burst_size=mets_burst_size,
        burst_pause=mets_burst_pause,
    )

    issue_lookup = {
        f"{issue.lccn}/{issue.date}/{issue.edition}": issue
        for issue in resolved_issues
    }
    issues_finalized = 0
    for issue_key, alto_by_seq in extracted.items():
        issue = issue_lookup.get(issue_key)
        if issue is None:
            continue
        before_count = len(state.completed_issues)
        finalize_issue_from_tarball(
            output_dir,
            issue,
            alto_by_seq,
            state,
            state_path,
        )
        if len(state.completed_issues) > before_count:
            issues_finalized += 1

    state.completed_tarballs[tarball.sha1] = tarball.batch
    state.save(state_path)

    if notifier:
        notifier.notify_batch_complete(
            tarball.batch,
            issues_finalized=issues_finalized,
            tarball_size=tarball.size,
        )

    if not keep_tarballs and os.path.exists(tarball_path):
        os.remove(tarball_path)


def run_bulk_download(
    titles: list[TitleSpec],
    output_dir: str,
    state_dir: str,
    index_path: str,
    scratch_dir: str,
    dry_run: bool = False,
    keep_tarballs: bool = True,
    workers: int = 1,
    delay: float = DEFAULT_REQUEST_DELAY,
    max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE,
    directory_delay: float = DEFAULT_DIRECTORY_DELAY,
    tarball_cooldown: float = TARBALL_COOLDOWN,
    mets_burst_size: int = METS_BURST_SIZE,
    mets_burst_pause: float = METS_BURST_PAUSE,
    client: HttpClient | None = None,
    telegram_notifier: TelegramNotifier | None = None,
) -> DownloadPlan:
    http = client or HttpClient(
        delay=delay,
        max_requests_per_minute=max_requests_per_minute,
        directory_delay=directory_delay,
        notifier=telegram_notifier,
    )
    if client is not None and telegram_notifier is not None and http.notifier is None:
        http.notifier = telegram_notifier
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(state_dir, exist_ok=True)
    os.makedirs(scratch_dir, exist_ok=True)

    plan = build_download_plan(http, titles, index_path, dry_run=dry_run)
    if dry_run:
        print_dry_run_report(plan)
        return plan

    state_path = os.path.join(state_dir, "download_state.json")
    state = DownloadState.load(state_path)

    pending_started = False
    for tarball in plan.tarballs:
        if tarball.sha1 in state.completed_tarballs:
            continue
        if pending_started and tarball_cooldown > 0:
            logger.info(
                "Pausing %.0fs before starting tarball %s",
                tarball_cooldown,
                tarball.batch,
            )
            time.sleep(tarball_cooldown)
        pending_started = True
        process_tarball(
            http,
            tarball,
            titles,
            output_dir,
            scratch_dir,
            state_dir,
            state,
            state_path,
            keep_tarballs,
            mets_burst_size=mets_burst_size,
            mets_burst_pause=mets_burst_pause,
            notifier=telegram_notifier,
        )

    logger.info("Bulk download completed for %d titles", len(titles))
    return plan
