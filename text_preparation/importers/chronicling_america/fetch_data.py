#!/usr/bin/env python3
"""Downloader utility for fetching raw METS/ALTO files from Chronicling America.

Bulk mode (recommended for server runs):
  python -m text_preparation.importers.chronicling_america.fetch_data \\
    --config text_preparation/config/download_config/chronicling_america_titles.json \\
    --output-dir /data/ca/raw \\
    --state-dir /data/ca/state \\
    --dry-run

Legacy single-title mode (small samples, uses loc.gov JSON API by default):
  python -m text_preparation.importers.chronicling_america.fetch_data \\
    --output-dir text_preparation/data/sample_data \\
    --lccn sn83045462 --alias eveningstar --date 1932-06-20

  # Or use the older batch-directory crawler:
  python -m text_preparation.importers.chronicling_america.fetch_data \\
    --output-dir text_preparation/data/sample_data \\
    --lccn sn83045462 --alias eveningstar --limit 1 --use-crawl
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from text_preparation.importers.chronicling_america import api, bulk
from text_preparation.importers.chronicling_america.bulk import (
    HttpClient,
    build_download_plan,
    dedupe_issues,
    download_file,
    issue_local_dir,
    list_issues_in_batch,
    print_dry_run_report,
    run_bulk_download,
    title_from_legacy,
)
from text_preparation.importers.chronicling_america.bulk import DEFAULT_REQUEST_DELAY
from text_preparation.importers.chronicling_america.bulk import (
    DEFAULT_MAX_REQUESTS_PER_MINUTE,
)
from text_preparation.importers.chronicling_america.bulk import (
    DEFAULT_DIRECTORY_DELAY,
    METS_BURST_PAUSE,
    METS_BURST_SIZE,
    TARBALL_COOLDOWN,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _download_issue_legacy(
    client: HttpClient,
    issue: bulk.IssueInfo,
    output_dir: str,
) -> str:
    """Download METS and ALTO files for one issue via per-file crawl.

    In the batch directory, ALTO files already carry the filenames referenced by
    the METS fileSec hrefs, so no renaming is needed.
    """
    issue_dir = issue_local_dir(output_dir, issue)
    alto_dir = os.path.join(issue_dir, "alto")
    os.makedirs(alto_dir, exist_ok=True)

    mets_name = f"{issue.issue_dir_name}.xml"
    mets_path = os.path.join(issue_dir, mets_name)
    download_file(client, f"{issue.url}{mets_name}", mets_path)

    with open(mets_path, "rb") as handle:
        href_names = bulk.parse_mets_alto_filenames(handle.read())

    for filename in href_names:
        alto_path = os.path.join(alto_dir, filename)
        if os.path.exists(alto_path) and os.path.getsize(alto_path) > 0:
            continue
        download_file(
            client,
            f"{issue.url}{filename}",
            alto_path,
        )

    return issue_dir


def _run_legacy_api(args: argparse.Namespace) -> None:
    client = HttpClient(delay=args.delay)
    title = title_from_legacy(args.lccn, args.alias)
    downloaded = api.run_api_download(
        client,
        title,
        args.output_dir,
        issue_date=args.date,
        edition=args.edition,
        limit=args.limit,
    )
    if not downloaded:
        logger.error("No issues found matching parameters.")
        sys.exit(1)
    logger.info("Download completed successfully!")


def _run_legacy_crawl(args: argparse.Namespace) -> None:
    client = HttpClient(delay=args.delay)
    title = title_from_legacy(args.lccn, args.alias)

    if args.batch:
        batches = [args.batch]
    else:
        # Sampling mode: stop at the first batch containing the LCCN instead of
        # building the full batch index (which takes hours).
        first_batch = bulk.find_first_batch_for_lccn(client, title.lccn)
        if first_batch is None:
            logger.error("Could not find any batch containing LCCN %s", args.lccn)
            sys.exit(1)
        logger.info("Found matching batch: %s", first_batch)
        batches = [first_batch]

    issues: list[bulk.IssueInfo] = []
    for batch in batches:
        issues.extend(list_issues_in_batch(client, batch, title))
    issues = dedupe_issues(issues)

    if args.date:
        issues = [issue for issue in issues if issue.date == args.date]
    else:
        issues = issues[: args.limit]

    if not issues:
        logger.error("No issues found matching parameters.")
        sys.exit(1)

    for issue in issues:
        logger.info("Downloading issue %s (%s)", issue.issue_dir_name, issue.date)
        _download_issue_legacy(client, issue, args.output_dir)

    logger.info("Download completed successfully!")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download METS/ALTO files from Chronicling America.",
    )
    parser.add_argument(
        "--config",
        type=str,
        help="JSON file listing LCCN/alias titles for bulk download",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Directory to save downloaded files",
    )
    parser.add_argument(
        "--state-dir",
        type=str,
        default=None,
        help="Directory for resume state and batch index cache",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        default=None,
        help="Temporary directory for tarball downloads",
    )
    parser.add_argument(
        "--index-path",
        type=str,
        default=None,
        help="Path to cached batch-to-LCCN index JSON",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Parallel workers for METS downloads (requests are still rate-limited "
            "globally; keep at 1 unless you know what you are doing)"
        ),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help=(
            "Minimum seconds between HTTP requests (LOC JSON API: 20/min → 3.0 s; "
            "see https://www.loc.gov/apis/json-and-yaml/working-within-limits/)"
        ),
    )
    parser.add_argument(
        "--max-rpm",
        type=int,
        default=DEFAULT_MAX_REQUESTS_PER_MINUTE,
        help=(
            "Maximum HTTP requests per rolling 60 s window (default 8; LOC JSON "
            "API limit is 20/min)"
        ),
    )
    parser.add_argument(
        "--directory-delay",
        type=float,
        default=DEFAULT_DIRECTORY_DELAY,
        help=(
            "Minimum seconds between batch directory listing requests "
            "(HTML /data/batches/ crawls; default 6.0 s)"
        ),
    )
    parser.add_argument(
        "--batch-cooldown",
        type=float,
        default=TARBALL_COOLDOWN,
        help="Seconds to pause between OCR tarballs (default 180)",
    )
    parser.add_argument(
        "--mets-burst-size",
        type=int,
        default=METS_BURST_SIZE,
        help="Pause after this many METS downloads (default 15; 0 disables)",
    )
    parser.add_argument(
        "--mets-burst-pause",
        type=float,
        default=METS_BURST_PAUSE,
        help="Seconds to pause after each METS burst (default 90)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print download plan without fetching data",
    )
    parser.add_argument(
        "--keep-tarballs",
        action="store_true",
        help="Keep downloaded .tar.bz2 files after extraction",
    )

    # Legacy single-title options
    parser.add_argument(
        "--lccn",
        type=str,
        default="sn83045462",
        help="LCCN for legacy single-title mode",
    )
    parser.add_argument(
        "--alias",
        type=str,
        default="eveningstar",
        help="Alias for legacy single-title mode",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Specific date to download (YYYY-MM-DD) in legacy mode",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Max issues in legacy mode",
    )
    parser.add_argument(
        "--batch",
        type=str,
        help="Batch name for legacy crawl mode (skip batch search)",
    )
    parser.add_argument(
        "--edition",
        type=str,
        default="ed-1",
        help="Edition folder for legacy API mode (default: ed-1)",
    )
    parser.add_argument(
        "--use-crawl",
        action="store_true",
        help="Use batch-directory crawling instead of the loc.gov JSON API",
    )

    args = parser.parse_args()
    state_dir = args.state_dir or os.path.join(args.output_dir, ".ca_state")
    scratch_dir = args.scratch_dir or os.path.join(state_dir, "scratch")
    index_path = args.index_path or os.path.join(state_dir, "batch_index.json")

    if args.config:
        titles = bulk.load_titles_config(args.config)
        run_bulk_download(
            titles=titles,
            output_dir=args.output_dir,
            state_dir=state_dir,
            index_path=index_path,
            scratch_dir=scratch_dir,
            dry_run=args.dry_run,
            keep_tarballs=args.keep_tarballs,
            workers=args.workers,
            delay=args.delay,
            max_requests_per_minute=args.max_rpm,
            directory_delay=args.directory_delay,
            tarball_cooldown=args.batch_cooldown,
            mets_burst_size=args.mets_burst_size,
            mets_burst_pause=args.mets_burst_pause,
        )
        return

    if args.dry_run:
        client = HttpClient(delay=args.delay)
        titles = [title_from_legacy(args.lccn, args.alias)]
        plan = build_download_plan(client, titles, index_path, dry_run=True)
        print_dry_run_report(plan)
        return

    if args.use_crawl:
        _run_legacy_crawl(args)
    else:
        _run_legacy_api(args)


if __name__ == "__main__":
    main()
