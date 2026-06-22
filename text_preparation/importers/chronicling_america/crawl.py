"""Batch directory crawling and issue URL resolution."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import replace

from bs4 import BeautifulSoup

from text_preparation.importers.chronicling_america.constants import (
    BATCH_ENUMERATION_COOLDOWN,
    BATCHES_URL,
    ISSUE_DIR_RE,
    LCCN_RE,
)
from text_preparation.importers.chronicling_america.http import HttpClient
from text_preparation.importers.chronicling_america.issues import issue_in_date_range
from text_preparation.importers.chronicling_america.models import IssueInfo, TitleSpec

logger = logging.getLogger(__name__)


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
    enumeration_cooldown: float = BATCH_ENUMERATION_COOLDOWN,
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

    if enumeration_cooldown > 0:
        logger.info(
            "Pausing %.0fs after enumerating %s/%s",
            enumeration_cooldown,
            batch,
            title.lccn,
        )
        time.sleep(enumeration_cooldown)

    return cached


def resolve_issue_urls(
    client: HttpClient,
    batch: str,
    title: TitleSpec,
    issues: list[IssueInfo],
    state_dir: str,
    *,
    enumeration_cooldown: float = BATCH_ENUMERATION_COOLDOWN,
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
        enumeration_cooldown=enumeration_cooldown,
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
