#!/usr/bin/env python3
"""Downloader utility for fetching raw METS/ALTO files from Chronicling America.

Bulk mode (recommended for server runs):
  python -m text_preparation.importers.chronicling_america.fetch_data \\
    --config text_preparation/config/download_config/chronicling_america_titles.json \\
    --output-dir /data/ca/raw \\
    --state-dir /data/ca/state \\
    --dry-run

Legacy single-title mode (small samples):
  python -m text_preparation.importers.chronicling_america.fetch_data \\
    --output-dir text_preparation/data/sample_data \\
    --lccn sn83045462 --alias eveningstar --limit 1
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from text_preparation.importers.chronicling_america import bulk
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


def _run_legacy(args: argparse.Namespace) -> None:
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
        default=6,
        help="Parallel workers for METS downloads",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between HTTP requests",
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
        help="Batch name for legacy mode (skip batch search)",
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
        )
        return

    if args.dry_run:
        client = HttpClient(delay=args.delay)
        titles = [title_from_legacy(args.lccn, args.alias)]
        plan = build_download_plan(client, titles, index_path)
        print_dry_run_report(plan)
        return

    _run_legacy(args)


if __name__ == "__main__":
    main()
