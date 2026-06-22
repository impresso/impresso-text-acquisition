"""Bulk download pipeline for Chronicling America METS/ALTO data.

Uses per-batch OCR tarballs (ALTO XML) plus a lightweight METS crawl per issue.

Submodules:
  constants  — URLs, rate limits, regexes
  models     — TitleSpec, TarballInfo, IssueInfo, DownloadPlan, DownloadState
  http       — HttpClient, download_file
  manifest   — OCR manifest parsing and batch selection
  crawl      — batch directory crawling and issue URL caches
  issues     — issue identity and deduplication
  layout     — local METS/ALTO filesystem layout
  tarball    — OCR tarball extraction and SHA-1 verification

This module re-exports the public API and hosts download orchestration.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date
from urllib.parse import urljoin

from text_preparation.importers.chronicling_america.constants import (
    BASE_URL,
    BATCH_ENUMERATION_COOLDOWN,
    BATCHES_URL,
    BATCH_VERSION_RE,
    CHALLENGE_MARKERS,
    DEFAULT_DIRECTORY_DELAY,
    DEFAULT_MAX_REQUESTS_PER_MINUTE,
    DEFAULT_REQUEST_DELAY,
    HEADERS,
    ISSUE_DIR_RE,
    LCCN_RE,
    LOC_JSON_API_REQUESTS_PER_MINUTE,
    LOC_RATE_LIMIT_BLOCK_SECONDS,
    METS_BURST_PAUSE,
    METS_BURST_SIZE,
    OCR_JSON_URLS,
    PLAIN_TARBALL_RE,
    TARBALL_BATCH_RE,
    TARBALL_COOLDOWN,
)
from text_preparation.importers.chronicling_america.crawl import (
    _parse_directory_links,
    batch_contains_lccn,
    build_or_load_batch_index,
    enumerate_issue_urls,
    fetch_batch_lccns,
    find_first_batch_for_lccn,
    issue_url_cache_path,
    list_all_batches,
    list_issues_in_batch,
    load_issue_url_cache,
    resolve_issue_urls,
    save_issue_url_cache,
)
from text_preparation.importers.chronicling_america.http import (
    HttpClient,
    TieredHttpClient,
    download_file,
    is_batch_directory_listing,
    is_challenge_response,
    is_transient_status,
    make_http_client,
)
from text_preparation.importers.chronicling_america.issues import (
    dedupe_issues,
    issue_dir_name_from_date_edition,
    issue_info_from_tarball_key,
    issue_in_date_range,
    issue_local_dir,
    issue_state_key,
    issues_from_tarball_extraction,
)
from text_preparation.importers.chronicling_america.layout import (
    parse_mets_alto_filenames,
    write_issue_layout,
)
from text_preparation.importers.chronicling_america.manifest import (
    batch_family,
    batch_version,
    batches_for_lccns,
    dedupe_batch_versions,
    fetch_ocr_tarballs,
    index_from_tarball_manifest,
    parse_ocr_manifest,
    tarball_batch_name,
)
from text_preparation.importers.chronicling_america.models import (
    DownloadPlan,
    DownloadState,
    IssueInfo,
    TarballInfo,
    TitleSpec,
)
from text_preparation.importers.chronicling_america.tarball import (
    extract_alto_members,
    verify_sha1,
)

logger = logging.getLogger(__name__)

__all__ = [
    "BASE_URL",
    "BATCHES_URL",
    "BATCH_ENUMERATION_COOLDOWN",
    "BATCH_VERSION_RE",
    "CHALLENGE_MARKERS",
    "DEFAULT_DIRECTORY_DELAY",
    "DEFAULT_MAX_REQUESTS_PER_MINUTE",
    "DEFAULT_REQUEST_DELAY",
    "DownloadPlan",
    "DownloadState",
    "HEADERS",
    "HttpClient",
    "TieredHttpClient",
    "ISSUE_DIR_RE",
    "LCCN_RE",
    "LOC_JSON_API_REQUESTS_PER_MINUTE",
    "LOC_RATE_LIMIT_BLOCK_SECONDS",
    "METS_BURST_PAUSE",
    "METS_BURST_SIZE",
    "OCR_JSON_URLS",
    "PLAIN_TARBALL_RE",
    "TARBALL_BATCH_RE",
    "TARBALL_COOLDOWN",
    "IssueInfo",
    "TarballInfo",
    "TitleSpec",
    "_parse_directory_links",
    "batch_contains_lccn",
    "batch_family",
    "batch_version",
    "batches_for_lccns",
    "build_download_plan",
    "build_or_load_batch_index",
    "dedupe_batch_versions",
    "dedupe_issues",
    "download_file",
    "download_issue_mets",
    "download_mets_with_pacing",
    "enumerate_issue_urls",
    "extract_alto_members",
    "fetch_batch_lccns",
    "fetch_ocr_tarballs",
    "find_first_batch_for_lccn",
    "format_bytes",
    "format_dry_run_report",
    "index_from_tarball_manifest",
    "is_batch_directory_listing",
    "is_challenge_response",
    "is_transient_status",
    "issue_dir_name_from_date_edition",
    "issue_info_from_tarball_key",
    "issue_in_date_range",
    "issue_local_dir",
    "issue_state_key",
    "issue_url_cache_path",
    "issues_from_tarball_extraction",
    "list_all_batches",
    "list_issues_in_batch",
    "load_issue_url_cache",
    "load_titles_config",
    "make_http_client",
    "parse_mets_alto_filenames",
    "parse_ocr_manifest",
    "print_dry_run_report",
    "process_tarball",
    "resolve_issue_urls",
    "run_bulk_download",
    "save_issue_url_cache",
    "tarball_batch_name",
    "title_from_legacy",
    "verify_sha1",
    "write_issue_layout",
]


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Download planning and dry-run reporting
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Per-issue METS download and tarball processing
# ---------------------------------------------------------------------------


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
    client: HttpClient | TieredHttpClient,
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
    enumeration_cooldown: float = BATCH_ENUMERATION_COOLDOWN,
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
    if os.path.exists(tarball_path) and verify_sha1(tarball_path, tarball.sha1):
        logger.info("Reusing cached tarball %s at %s", tarball.batch, tarball_path)
    else:
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
        resolved_issues.extend(
            resolve_issue_urls(
                client,
                tarball.batch,
                title,
                lccn_issues,
                state_dir,
                enumeration_cooldown=enumeration_cooldown,
            )
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
    keep_tarballs: bool = True,
    workers: int = 1,
    delay: float = DEFAULT_REQUEST_DELAY,
    max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE,
    directory_delay: float = DEFAULT_DIRECTORY_DELAY,
    asset_delay: float | None = None,
    asset_max_requests_per_minute: int | None = None,
    tarball_cooldown: float = TARBALL_COOLDOWN,
    enumeration_cooldown: float = BATCH_ENUMERATION_COOLDOWN,
    mets_burst_size: int = METS_BURST_SIZE,
    mets_burst_pause: float = METS_BURST_PAUSE,
    client: HttpClient | TieredHttpClient | None = None,
) -> DownloadPlan:
    http = client or make_http_client(
        delay=delay,
        max_requests_per_minute=max_requests_per_minute,
        directory_delay=directory_delay,
        asset_delay=asset_delay,
        asset_max_requests_per_minute=asset_max_requests_per_minute,
    )
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
            enumeration_cooldown=enumeration_cooldown,
        )

    logger.info("Bulk download completed for %d titles", len(titles))
    return plan
