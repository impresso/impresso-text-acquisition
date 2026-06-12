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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://chroniclingamerica.loc.gov"
OCR_JSON_URL = f"{BASE_URL}/ocr.json"
BATCHES_URL = f"{BASE_URL}/data/batches/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ImpressoTextPreparation/1.0; "
        "+https://github.com/impresso/impresso-text-acquisition)"
    )
}

ISSUE_DIR_RE = re.compile(r"^\d{10}$")
TARBALL_BATCH_RE = re.compile(r"^batch_(.+)\.tar\.bz2$")
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
        self._lock = __import__("threading").Lock()

    def request(self, url: str, method: str = "GET", **kwargs: Any) -> requests.Response:
        retries = 0
        backoff = 2.0
        kwargs.setdefault("timeout", self.timeout)
        while retries < self.max_retries:
            time.sleep(self.delay)
            try:
                with self._lock:
                    response = (
                        self.session.get(url, **kwargs)
                        if method == "GET"
                        else self.session.head(url, **kwargs)
                    )
                if response.status_code in (429, 503):
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
    return match.group(1) if match else None


def fetch_ocr_tarballs(client: HttpClient) -> list[TarballInfo]:
    response = client.request(OCR_JSON_URL)
    response.raise_for_status()
    payload = response.json()
    tarballs: list[TarballInfo] = []
    for entry in payload.get("ocr", []):
        batch = tarball_batch_name(entry["name"])
        if not batch:
            continue
        tarballs.append(
            TarballInfo(
                batch=batch,
                url=entry["url"],
                sha1=entry["sha1"],
                size=int(entry["size"]),
            )
        )
    return tarballs


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

    index: dict[str, list[str]] = {}
    for idx, batch in enumerate(sorted(batches), start=1):
        logger.info("Indexing batch %s (%d/%d)", batch, idx, len(batches))
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
    tarballs = fetch_ocr_tarballs(client)
    tarball_by_batch = {info.batch: info for info in tarballs}
    index = build_or_load_batch_index(
        client,
        index_path,
        batches=sorted(tarball_by_batch.keys()),
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


def download_file(client: HttpClient, url: str, dest_path: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    response = client.request(url, stream=True)
    response.raise_for_status()
    with open(dest_path, "wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)


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
