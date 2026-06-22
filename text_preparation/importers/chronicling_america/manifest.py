"""OCR manifest parsing and batch selection helpers."""

from __future__ import annotations

import logging
from typing import Any

import requests

from text_preparation.importers.chronicling_america.constants import (
    BATCH_VERSION_RE,
    LCCN_RE,
    OCR_JSON_URLS,
    PLAIN_TARBALL_RE,
    TARBALL_BATCH_RE,
)
from text_preparation.importers.chronicling_america.http import HttpClient
from text_preparation.importers.chronicling_america.models import TarballInfo

logger = logging.getLogger(__name__)


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
