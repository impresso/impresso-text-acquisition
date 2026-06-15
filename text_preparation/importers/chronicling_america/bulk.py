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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# #region agent log
_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_DEBUG_LOG_PATH = os.path.join(_REPO_ROOT, ".cursor", "debug-5a3354.log")
_DEBUG_REQUEST_COUNT = 0
_DEBUG_LAST_REQUEST_TS: float | None = None


def _agent_debug_log(
    *,
    hypothesis_id: str,
    location: str,
    message: str,
    data: dict[str, Any],
    run_id: str = "pre-fix",
) -> None:
    payload = {
        "sessionId": "5a3354",
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        os.makedirs(os.path.dirname(_DEBUG_LOG_PATH), exist_ok=True)
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")
    except OSError:
        pass


# #endregion

BASE_URL = "https://chroniclingamerica.loc.gov"
# LOC migrated OCR manifests in 2025: /ocr.json now 404s after redirect; the live
# manifest is at /data/ocr/ocr.json with a list-shaped schema (archive_name, batch).
OCR_JSON_URLS = (
    f"{BASE_URL}/data/ocr/ocr.json",
    f"{BASE_URL}/ocr.json",
)
BATCHES_URL = f"{BASE_URL}/data/batches/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ImpressoTextPreparation/1.0; "
        "+https://github.com/impresso/impresso-text-acquisition)"
    )
}

# Includes Cloudflare-specific 52x codes (e.g. 525 SSL handshake failed) which
# tile.loc.gov intermittently returns and which are safe to retry.
def is_transient_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


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
        delay: float = 1.0,
        max_retries: int = 5,
        timeout: float = 120.0,
        session: requests.Session | None = None,
    ) -> None:
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update(HEADERS)
        self._lock = threading.Lock()
        self._last_request_at = 0.0

    def _rate_limit_wait(self) -> float:
        """Sleep until the configured delay has elapsed since the last request."""
        now = time.monotonic()
        wait = self.delay - (now - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
            return wait
        return 0.0

    def _backoff_for_status(self, response: requests.Response, backoff: float) -> float:
        if response.status_code != 429:
            return backoff
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return max(backoff, float(retry_after))
        # LOC documents a 1-hour block when rate limits are exceeded.
        return max(backoff, 60.0)

    def request(self, url: str, method: str = "GET", **kwargs: Any) -> requests.Response:
        global _DEBUG_REQUEST_COUNT, _DEBUG_LAST_REQUEST_TS
        retries = 0
        backoff = 2.0
        kwargs.setdefault("timeout", self.timeout)
        while retries < self.max_retries:
            try:
                with self._lock:
                    wait_elapsed = self._rate_limit_wait()
                    lock_acquired = time.monotonic()
                    gap_since_last = (
                        None
                        if _DEBUG_LAST_REQUEST_TS is None
                        else lock_acquired - _DEBUG_LAST_REQUEST_TS
                    )
                    response = (
                        self.session.get(url, **kwargs)
                        if method == "GET"
                        else self.session.head(url, **kwargs)
                    )
                    request_finished = time.monotonic()
                    self._last_request_at = request_finished
                    _DEBUG_REQUEST_COUNT += 1
                    _DEBUG_LAST_REQUEST_TS = request_finished
                    # #region agent log
                    _agent_debug_log(
                        hypothesis_id="H5",
                        location="bulk.py:HttpClient.request",
                        message="http_request_completed",
                        data={
                            "request_num": _DEBUG_REQUEST_COUNT,
                            "status": response.status_code,
                            "method": method,
                            "url_host": url.split("/")[2] if "://" in url else url,
                            "url_path_prefix": "/".join(url.split("/")[3:6]),
                            "thread": threading.current_thread().name,
                            "retry": retries,
                            "delay_setting": self.delay,
                            "rate_limit_wait_s": round(wait_elapsed, 3),
                            "gap_since_last_request_s": (
                                None
                                if gap_since_last is None
                                else round(gap_since_last, 3)
                            ),
                            "request_duration_s": round(
                                request_finished - lock_acquired,
                                3,
                            ),
                        },
                    )
                    if response.status_code == 429:
                        _agent_debug_log(
                            hypothesis_id="H3",
                            location="bulk.py:HttpClient.request",
                            message="rate_limit_429",
                            data={
                                "url": url,
                                "retry": retries,
                                "backoff_s": backoff,
                                "retry_after": response.headers.get("Retry-After"),
                            },
                        )
                    # #endregion
                if is_transient_status(response.status_code):
                    backoff = self._backoff_for_status(response, backoff)
                    logger.warning(
                        "HTTP %s for %s; sleeping %.0fs before retry",
                        response.status_code,
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
                # #region agent log
                _agent_debug_log(
                    hypothesis_id="H3",
                    location="bulk.py:HttpClient.request",
                    message="request_exception",
                    data={"url": url, "error": str(exc), "retry": retries},
                )
                # #endregion
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
                # #region agent log
                _agent_debug_log(
                    hypothesis_id="H6",
                    location="bulk.py:fetch_ocr_tarballs",
                    message="ocr_manifest_loaded",
                    data={
                        "manifest_url": manifest_url,
                        "tarball_count": len(tarballs),
                        "sample_batch": tarballs[0].batch,
                    },
                )
                # #endregion
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
) -> DownloadPlan:
    # #region agent log
    _agent_debug_log(
        hypothesis_id="H1",
        location="bulk.py:build_download_plan",
        message="plan_start",
        data={
            "titles": [t.alias for t in titles],
            "index_cached": os.path.exists(index_path),
            "index_path": index_path,
        },
    )
    # #endregion
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

    all_issues: list[IssueInfo] = []
    for title in titles:
        title_batches = [
            batch
            for batch in selected_batches
            if title.lccn in index.get(batch, [])
        ]
        for batch in title_batches:
            all_issues.extend(list_issues_in_batch(client, batch, title))

    issues = dedupe_issues(all_issues)
    total_bytes = sum(info.size for info in selected_tarballs)
    # #region agent log
    _agent_debug_log(
        hypothesis_id="H1",
        location="bulk.py:build_download_plan",
        message="plan_complete",
        data={
            "selected_batches": len(selected_batches),
            "issues": len(issues),
            "tarballs": len(selected_tarballs),
            "total_requests_so_far": _DEBUG_REQUEST_COUNT,
        },
    )
    # #endregion
    return DownloadPlan(
        titles=titles,
        batches=selected_batches,
        tarballs=selected_tarballs,
        issues=issues,
        total_tarball_bytes=total_bytes,
    )


def format_bytes(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def print_dry_run_report(plan: DownloadPlan) -> None:
    print("Chronicling America bulk download plan")
    print("=" * 40)
    for title in plan.titles:
        date_range = ""
        if title.start_date or title.end_date:
            date_range = f" ({title.start_date or '...'} to {title.end_date or '...'})"
        print(f"- {title.alias} [{title.lccn}]{date_range}")
    print(f"Batches: {len(plan.batches)}")
    print(f"Tarballs: {len(plan.tarballs)} ({format_bytes(plan.total_tarball_bytes)} compressed)")
    print(f"Issues (METS files): {len(plan.issues)}")
    if plan.batches:
        print("\nBatch list:")
        for batch in plan.batches:
            print(f"  - {batch}")


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
    extracted: dict[str, dict[int, bytes]] = {}
    with tarfile.open(tarball_path, mode="r:bz2") as archive:
        for member in archive.getmembers():
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
    issue_dir = issue_local_dir(output_dir, issue)
    mets_name = f"{issue.issue_dir_name}.xml"
    mets_path = os.path.join(issue_dir, mets_name)
    if os.path.exists(mets_path) and os.path.getsize(mets_path) > 0:
        return

    os.makedirs(issue_dir, exist_ok=True)
    mets_url = urljoin(issue.url, mets_name)
    logger.info("Downloading METS for %s", issue_state_key(issue))
    download_file(client, mets_url, mets_path)


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
    state: DownloadState,
    state_path: str,
    keep_tarballs: bool,
    issue_lookup: dict[str, IssueInfo],
) -> None:
    if tarball.sha1 in state.completed_tarballs:
        logger.info("Skipping tarball %s (already completed)", tarball.batch)
        return

    lccns = {title.lccn for title in titles}
    tarball_path = os.path.join(
        scratch_dir,
        os.path.basename(tarball.url.rstrip("/").split("/")[-1]),
    )
    logger.info("Downloading tarball %s (%s)", tarball.batch, format_bytes(tarball.size))
    download_file(client, tarball.url, tarball_path)

    if not verify_sha1(tarball_path, tarball.sha1):
        os.remove(tarball_path)
        raise RuntimeError(f"SHA-1 mismatch for tarball {tarball.batch}")

    extracted = extract_alto_members(tarball_path, lccns)
    for issue_key, alto_by_seq in extracted.items():
        issue = issue_lookup.get(issue_key)
        if issue is None:
            continue
        finalize_issue_from_tarball(
            output_dir,
            issue,
            alto_by_seq,
            state,
            state_path,
        )

    state.completed_tarballs[tarball.sha1] = tarball.batch
    state.save(state_path)

    if not keep_tarballs and os.path.exists(tarball_path):
        os.remove(tarball_path)


def run_bulk_download(
    titles: list[TitleSpec],
    output_dir: str,
    state_dir: str,
    index_path: str,
    scratch_dir: str,
    dry_run: bool = False,
    keep_tarballs: bool = False,
    workers: int = 6,
    delay: float = 1.0,
    client: HttpClient | None = None,
) -> DownloadPlan:
    http = client or HttpClient(delay=delay)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(state_dir, exist_ok=True)
    os.makedirs(scratch_dir, exist_ok=True)

    plan = build_download_plan(http, titles, index_path)
    if dry_run:
        print_dry_run_report(plan)
        return plan

    state_path = os.path.join(state_dir, "download_state.json")
    state = DownloadState.load(state_path)

    issue_lookup = {
        f"{issue.lccn}/{issue.date}/{issue.edition}": issue for issue in plan.issues
    }

    pending_mets = [
        issue for issue in plan.issues if issue_state_key(issue) not in state.completed_issues
    ]
    # #region agent log
    _agent_debug_log(
        hypothesis_id="H5",
        location="bulk.py:run_bulk_download",
        message="mets_phase_start",
        data={
            "pending_mets": len(pending_mets),
            "workers": workers,
            "delay": delay,
            "requests_before_mets": _DEBUG_REQUEST_COUNT,
        },
    )
    # #endregion
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(download_issue_mets, http, issue, output_dir)
            for issue in pending_mets
        ]
        for future in as_completed(futures):
            future.result()

    for tarball in plan.tarballs:
        process_tarball(
            http,
            tarball,
            titles,
            output_dir,
            scratch_dir,
            state,
            state_path,
            keep_tarballs,
            issue_lookup,
        )

    incomplete = [
        issue_state_key(issue)
        for issue in plan.issues
        if issue_state_key(issue) not in state.completed_issues
    ]
    if incomplete:
        logger.warning(
            "%d issues still incomplete after tarball processing (missing ALTO in tarballs?)",
            len(incomplete),
        )

    logger.info("Bulk download completed for %d titles", len(titles))
    return plan
