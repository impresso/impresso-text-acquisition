#!/usr/bin/env python3
"""Generate per-title Chronicling America bulk download plan .txt files.

Fetches the live OCR manifest from chroniclingamerica.loc.gov and writes one
report per configured title (batches, tarball sizes, issue estimates).

Example:
  python -m text_preparation.importers.chronicling_america.generate_plans
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from text_preparation.importers.chronicling_america.bulk import (
    HttpClient,
    TitleSpec,
    build_download_plan,
    format_dry_run_report,
    load_titles_config,
)

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(_PACKAGE_DIR, "chronicling_america_pilot_titles.json")
DEFAULT_OUTPUT_DIR = os.path.join(_PACKAGE_DIR, "download_plans")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TITLE_DISPLAY_NAMES: dict[str, str] = {
    "sn84024738": "The Daily Dispatch",
    "sn83045160": "Memphis Daily Appeal",
    "sn83030214": "New-York Tribune",
    "sn85066387": "The San Francisco Call",
    "sn87093407": "The Seattle Star",
    "sn83045462": "Evening Star",
}


def plan_output_path(output_dir: str, title: TitleSpec) -> str:
    return os.path.join(output_dir, f"{title.alias}_{title.lccn}.txt")


def write_plan_report(
    output_dir: str,
    title: TitleSpec,
    report: str,
) -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = plan_output_path(output_dir, title)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(report)
    return path


def generate_plans(
    titles: list[TitleSpec],
    output_dir: str,
    index_path: str,
    delay: float = 1.0,
) -> list[str]:
    client = HttpClient(delay=delay)
    written: list[str] = []
    for title in titles:
        display_name = TITLE_DISPLAY_NAMES.get(title.lccn, title.alias)
        logger.info("Building download plan for %s [%s]", display_name, title.lccn)
        plan = build_download_plan(client, [title], index_path, dry_run=True)
        path = write_plan_report(output_dir, title, format_dry_run_report(plan))
        written.append(path)
        logger.info(
            "Wrote %s (%d batches, ~%s issues)",
            path,
            len(plan.batches),
            plan.estimated_issues if plan.estimated_issues is not None else "?",
        )
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate per-title Chronicling America bulk download plans.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG,
        help="JSON file listing LCCN/alias titles",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where .txt plan files are written",
    )
    parser.add_argument(
        "--index-path",
        type=str,
        default=None,
        help="Path to cached batch-to-LCCN index JSON (optional)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between HTTP requests",
    )
    args = parser.parse_args()
    index_path = args.index_path or os.path.join(
        args.output_dir, "..", ".ca_plan_state", "batch_index.json"
    )

    titles = load_titles_config(args.config)
    if not titles:
        logger.error("No titles found in %s", args.config)
        sys.exit(1)

    written = generate_plans(
        titles=titles,
        output_dir=args.output_dir,
        index_path=index_path,
        delay=args.delay,
    )
    print(f"Generated {len(written)} plan file(s) in {args.output_dir}")


if __name__ == "__main__":
    main()
