"""loc.gov JSON API helpers for Chronicling America issue discovery and downloads.

Official documentation:
https://libraryofcongress.github.io/data-exploration/loc.gov%20JSON%20API/Chronicling_America/README.html

The API returns direct tile.loc.gov storage URLs for METS and ALTO XML files.
This is the recommended LOC approach for sampling specific issues; bulk tarball
downloads remain preferable for large-scale ingestion.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urljoin

from text_preparation.importers.chronicling_america.bulk import (
    HttpClient,
    IssueInfo,
    TitleSpec,
    download_file,
    issue_local_dir,
)

logger = logging.getLogger(__name__)

LOC_COLLECTION_URL = "https://www.loc.gov/collections/chronicling-america/"
ITEM_URL_RE = re.compile(
    r"https?://www\.loc\.gov/item/([^/]+)/(\d{4}-\d{2}-\d{2})/(ed-\d+)/?"
)


@dataclass(frozen=True)
class ApiIssueFiles:
    """Resolved download URLs for one newspaper issue."""

    item_url: str
    lccn: str
    date: str
    edition: str
    issue_dir_name: str
    batch: str
    mets_url: str
    mets_filename: str
    alto_files: tuple[tuple[str, str], ...]  # (filename, url)


def build_collection_search_url(
    lccn: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    """Build a loc.gov collection search URL for issue-level results."""
    params = [f"fa=number_lccn:{lccn}", "dl=issue", "fo=json"]
    if start_date:
        params.append(f"start_date={start_date.isoformat()}")
    if end_date:
        params.append(f"end_date={end_date.isoformat()}")
    return LOC_COLLECTION_URL + "?" + "&".join(params)


def build_item_url(lccn: str, issue_date: str, edition: str = "ed-1") -> str:
    return f"https://www.loc.gov/item/{lccn}/{issue_date}/{edition}/"


def issue_dir_name_from_parts(issue_date: str, edition: str) -> str:
    year, month, day = issue_date.split("-")
    edition_num = int(edition.removeprefix("ed-"))
    return f"{year}{month}{day}{edition_num:02d}"


def parse_item_url(item_url: str) -> tuple[str, str, str] | None:
    match = ITEM_URL_RE.match(item_url.rstrip("/") + "/")
    if not match:
        return None
    return match.group(1), match.group(2), match.group(3)


def _issue_matches_title(
    issue_date: str,
    title: TitleSpec,
) -> bool:
    parsed = date.fromisoformat(issue_date)
    if title.start_date and parsed < title.start_date:
        return False
    if title.end_date and parsed > title.end_date:
        return False
    return True


def _iter_search_pages(
    client: HttpClient,
    search_url: str,
) -> list[dict[str, Any]]:
    """Paginate a loc.gov collection search and return all result dicts."""
    params = {"fo": "json", "c": 100, "at": "results,pagination"}
    url = search_url
    results: list[dict[str, Any]] = []
    while url:
        response = client.request(url, params=params)
        response.raise_for_status()
        payload = response.json()
        results.extend(payload.get("results", []))
        url = payload.get("pagination", {}).get("next")
        params = {}
    return results


def discover_issue_item_urls(
    client: HttpClient,
    title: TitleSpec,
    *,
    issue_date: str | None = None,
    edition: str = "ed-1",
    limit: int | None = None,
) -> list[str]:
    """Return loc.gov item URLs for issues to download."""
    if issue_date:
        if not _issue_matches_title(issue_date, title):
            return []
        return [build_item_url(title.lccn, issue_date, edition)]

    search_url = build_collection_search_url(
        title.lccn,
        start_date=title.start_date,
        end_date=title.end_date,
    )
    item_urls: list[str] = []
    for result in _iter_search_pages(client, search_url):
        item_id = result.get("id", "")
        if not item_id.startswith("http://www.loc.gov/item/"):
            continue
        parsed = parse_item_url(item_id)
        if parsed is None:
            continue
        lccn, found_date, _edition = parsed
        if lccn != title.lccn:
            continue
        if not _issue_matches_title(found_date, title):
            continue
        item_urls.append(item_id)

    item_urls = sorted(set(item_urls), key=lambda url: parse_item_url(url) or ("", "", ""))
    if limit is not None:
        item_urls = item_urls[:limit]
    return item_urls


def _pick_mets_url(other_formats: list[str], issue_dir_name: str) -> tuple[str, str] | None:
    """Prefer {issue_dir_name}.xml; fall back to the API's *_1.xml variant."""
    candidates: list[str] = []
    for url in other_formats:
        if not url.endswith(".xml"):
            continue
        if "marcxml" in url:
            continue
        candidates.append(url)

    for url in candidates:
        filename = os.path.basename(url)
        if filename == f"{issue_dir_name}.xml":
            return url, filename
    for url in candidates:
        filename = os.path.basename(url)
        if filename.startswith(issue_dir_name):
            preferred = f"{issue_dir_name}.xml"
            return url.rsplit("/", 1)[0] + "/" + preferred, preferred
    return None


def _extract_alto_files(resources: list[dict[str, Any]]) -> list[tuple[str, str]]:
    alto_files: list[tuple[str, str]] = []
    for resource in resources:
        for page_group in resource.get("files", []):
            if not isinstance(page_group, list):
                continue
            for file_info in page_group:
                if not isinstance(file_info, dict):
                    continue
                if file_info.get("mimetype") != "text/xml":
                    continue
                url = file_info.get("url")
                if not url:
                    continue
                alto_files.append((os.path.basename(url), url))
    return alto_files


def fetch_issue_files(
    client: HttpClient,
    item_url: str,
) -> ApiIssueFiles:
    """Resolve METS and ALTO download URLs from a loc.gov item JSON record."""
    response = client.request(item_url, params={"fo": "json"})
    response.raise_for_status()
    payload = response.json()

    parsed = parse_item_url(item_url)
    if parsed is None:
        raise ValueError(f"Unrecognized Chronicling America item URL: {item_url}")

    lccn, issue_date, edition = parsed
    issue_dir_name = issue_dir_name_from_parts(issue_date, edition)
    item_meta = payload.get("item", {}).get("item", payload.get("item", {}))
    batch = (item_meta.get("batch") or ["unknown"])[0]
    other_formats = item_meta.get("other_formats", [])

    mets_choice = _pick_mets_url(other_formats, issue_dir_name)
    if mets_choice is None and payload.get("resources"):
        # Fall back to deriving the METS URL from the first ALTO page URL.
        alto_files = _extract_alto_files(payload.get("resources", []))
        if alto_files:
            _, first_alto_url = alto_files[0]
            base = first_alto_url.rsplit("/", 1)[0] + "/"
            mets_filename = f"{issue_dir_name}.xml"
            mets_choice = (urljoin(base, mets_filename), mets_filename)

    if mets_choice is None:
        raise RuntimeError(f"No METS XML found in loc.gov item record: {item_url}")

    mets_url, mets_filename = mets_choice
    alto_files = _extract_alto_files(payload.get("resources", []))
    if not alto_files:
        raise RuntimeError(f"No ALTO XML files found in loc.gov item record: {item_url}")

    return ApiIssueFiles(
        item_url=item_url,
        lccn=lccn,
        date=issue_date,
        edition=edition,
        issue_dir_name=issue_dir_name,
        batch=batch,
        mets_url=mets_url,
        mets_filename=mets_filename,
        alto_files=tuple(alto_files),
    )


def discover_issues_for_bulk_plan(
    client: HttpClient,
    title: TitleSpec,
) -> list[IssueInfo]:
    """Discover issues via loc.gov API search (official path, avoids batch reel crawls)."""
    item_urls = discover_issue_item_urls(client, title)
    issues: list[IssueInfo] = []
    for item_url in item_urls:
        normalized = item_url.replace("http://", "https://")
        parsed = parse_item_url(normalized)
        if parsed is None:
            continue
        lccn, issue_date, edition = parsed
        issues.append(
            IssueInfo(
                batch="",
                lccn=lccn,
                alias=title.alias,
                date=issue_date,
                edition=edition,
                issue_dir_name=issue_dir_name_from_parts(issue_date, edition),
                url=normalized,
            )
        )
    return issues


def estimate_issue_count_via_api(
    client: HttpClient,
    title: TitleSpec,
) -> int | None:
    """Return the loc.gov search hit count for a title without paginating all results."""
    search_url = build_collection_search_url(
        title.lccn,
        start_date=title.start_date,
        end_date=title.end_date,
    )
    response = client.request(
        search_url,
        params={"fo": "json", "c": 1, "at": "results,pagination"},
    )
    if not response.ok:
        return None
    total = response.json().get("pagination", {}).get("total")
    return int(total) if total is not None else None


def api_issue_to_issue_info(files: ApiIssueFiles, alias: str) -> IssueInfo:
    return IssueInfo(
        batch=files.batch,
        lccn=files.lccn,
        alias=alias,
        date=files.date,
        edition=files.edition,
        issue_dir_name=files.issue_dir_name,
        url="",
    )


def download_issue_from_api(
    client: HttpClient,
    files: ApiIssueFiles,
    alias: str,
    output_dir: str,
) -> str:
    """Download METS + ALTO for one issue using loc.gov-resolved tile.loc.gov URLs."""
    issue = api_issue_to_issue_info(files, alias)
    issue_dir = issue_local_dir(output_dir, issue)
    alto_dir = os.path.join(issue_dir, "alto")
    os.makedirs(alto_dir, exist_ok=True)

    mets_path = os.path.join(issue_dir, files.mets_filename)
    download_file(client, files.mets_url, mets_path)

    for filename, url in files.alto_files:
        alto_path = os.path.join(alto_dir, filename)
        if os.path.exists(alto_path) and os.path.getsize(alto_path) > 0:
            continue
        download_file(client, url, alto_path)

    return issue_dir


def run_api_download(
    client: HttpClient,
    title: TitleSpec,
    output_dir: str,
    *,
    issue_date: str | None = None,
    edition: str = "ed-1",
    limit: int = 1,
) -> list[str]:
    """Discover issues via loc.gov API and download METS/ALTO for each."""
    item_urls = discover_issue_item_urls(
        client,
        title,
        issue_date=issue_date,
        edition=edition,
        limit=limit,
    )
    if not item_urls:
        return []

    downloaded_dirs: list[str] = []
    for item_url in item_urls:
        logger.info("Resolving issue files from loc.gov API: %s", item_url)
        files = fetch_issue_files(client, item_url)
        logger.info(
            "Downloading issue %s (%s) with %d ALTO files",
            files.issue_dir_name,
            files.date,
            len(files.alto_files),
        )
        downloaded_dirs.append(
            download_issue_from_api(client, files, title.alias, output_dir)
        )
    return downloaded_dirs
