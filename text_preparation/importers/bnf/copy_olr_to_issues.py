"""Attach downloaded BNF OLR files to their issues in the issue index.

The BnF SFTP deposit (fetched by ``bash_scripts/fetch_BNF_olr.sh`` into
``BNF/new_olr/<1..N>/``) contains one XML file per fascicule ARK, named
``<ark_id>_olr.xml``. This script:

1. Walks the OLR deposit once to build an ``ark_id -> file path`` index.
2. Iterates the nested issue index (``{alias: {year: {month: [entry, ...]}}}``)
   alias by alias, and for every issue entry whose ``ark_id`` has a matching
   OLR file, copies that file into the issue's ``local_path`` directory and
   sets ``entry["olr_file"] = True``.
3. Writes back the updated issue index, plus two reports:
   - a per-alias OLR coverage snapshot (issues with/without an OLR file),
   - the list of OLR files that had no matching issue anywhere in the index.

Reads (path resolution, OLR source directory) use the read-only mount
``/mnt/project_impresso/original`` by default. Writes (the actual file
copies) go to the read-write mount ``/mnt/project_impresso_rw/original`` --
both are the same underlying share mounted twice, so paths line up, but only
the rw mount can actually be written to.

Processing is alias-by-alias and resumable: a small progress file records
which aliases have already been fully copied, so an interrupted run can be
re-launched with the same command and will skip completed aliases. Re-running
an already-completed alias is also safe/idempotent (files are re-copied
identically, ``olr_file`` stays ``True``).

Usage:
    python copy_olr_to_issues.py [--dry-run]
        [--issue-index text_preparation/data/issue_indices/issue_index.bnf_new.json]
        [--olr-dir /mnt/project_impresso/original/BNF/new_olr]
        [--base-dir /mnt/project_impresso/original]
        [--write-base-dir /mnt/project_impresso_rw/original]
        [--aliases figaro1839 lafronde] [--skip-aliases letemps]
        [--force] [--workers 8]
"""

import argparse
import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("copy_olr_to_issues")

DEFAULT_BASE_DIR = "/mnt/project_impresso/original"
DEFAULT_WRITE_BASE_DIR = "/mnt/project_impresso_rw/original"
DEFAULT_ISSUE_INDEX = "text_preparation/data/issue_indices/issue_index.bnf_new.json"
DEFAULT_PROGRESS_FILE = "text_preparation/importers/bnf/.copy_olr_progress.json"
DEFAULT_STATS_REPORT = "text_preparation/data/sample_data/BNF_API/olr_coverage_by_alias.json"
DEFAULT_UNMATCHED_REPORT = "text_preparation/data/sample_data/BNF_API/olr_files_without_issues.txt"
DEFAULT_MISSING_REPORT = "text_preparation/data/sample_data/BNF_API/olr_missing_unexplained.txt"

OLR_SUFFIX = "_olr.xml"


def load_failed_arks(path: str) -> set:
    """Load the BnF-provided list of ark_ids for which no OLR was found.

    One ark_id per line, no header. Missing file -> empty set (with a
    warning), so the "unexplained" classification degrades gracefully.
    """
    if not path or not os.path.exists(path):
        logger.warning("failed_arks.txt not found at %s -- treating as empty", path)
        return set()
    with open(path, encoding="utf-8") as f:
        arks = {line.strip() for line in f if line.strip()}
    logger.info("Loaded %d ark_ids from %s", len(arks), path)
    return arks


def build_olr_index(olr_dir: str) -> dict:
    """Walk the OLR deposit once, return {ark_id: absolute file path}."""
    index = {}
    n_subdirs = 0
    for entry in sorted(os.scandir(olr_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        n_subdirs += 1
        for f in os.scandir(entry.path):
            if f.is_file() and f.name.endswith(OLR_SUFFIX):
                index[f.name[: -len(OLR_SUFFIX)]] = f.path
        if n_subdirs % 10 == 0:
            logger.info(
                "Scanned %d OLR subdirectories, %d files indexed so far",
                n_subdirs,
                len(index),
            )
    logger.info(
        "OLR deposit scan complete: %d subdirectories, %d files indexed",
        n_subdirs,
        len(index),
    )
    return index


def load_progress(progress_file: str) -> set:
    if not os.path.exists(progress_file):
        return set()
    with open(progress_file, encoding="utf-8") as f:
        return set(json.load(f).get("completed_aliases", []))


def save_progress(progress_file: str, completed_aliases: set) -> None:
    os.makedirs(os.path.dirname(progress_file), exist_ok=True)
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump({"completed_aliases": sorted(completed_aliases)}, f, indent=2)


def flatten_alias(year_dict: dict):
    """Yield (year, month, entry) for every issue entry of one alias."""
    for year, month_dict in year_dict.items():
        for month, entries in month_dict.items():
            for entry in entries:
                yield year, month, entry


def copy_one(olr_path: str, write_base_dir: str, rel_path: str, ark_id: str) -> str | None:
    """Copy a single OLR file into its issue's directory. Returns error string, or None.

    Never creates the destination directory: issue directories are expected
    to already exist, so a missing one is treated as an error to investigate
    rather than silently created (which could land the file somewhere wrong
    if rel_path is off).
    """
    dest_dir = os.path.join(write_base_dir, rel_path)
    if not os.path.isdir(dest_dir):
        return f"destination directory does not exist: {dest_dir}"
    dest_path = os.path.join(dest_dir, f"{ark_id}{OLR_SUFFIX}")
    try:
        if os.path.exists(dest_path) and os.path.getsize(dest_path) == os.path.getsize(olr_path):
            return None  # already copied (e.g. a previous run), skip re-writing it
        # copyfile (content only): copy2/copystat raise "[Errno 1] Operation
        # not permitted" on this CIFS/SMB mount when preserving mtime, even
        # though the content copy itself succeeds -- and we don't need the
        # BnF deposit's original mtime on the destination anyway.
        shutil.copyfile(olr_path, dest_path)
        return None
    except OSError as exc:
        return str(exc)


def process_alias(
    alias: str,
    year_dict: dict,
    olr_index: dict,
    base_dir: str,
    write_base_dir: str,
    workers: int,
    dry_run: bool,
) -> dict:
    """Copy matched OLR files for one alias, mutate entries in place. Returns stats."""
    items = list(flatten_alias(year_dict))
    to_copy = [entry for _, _, entry in items if entry["ark_id"] in olr_index]

    n_errors = 0

    def submit(entry):
        ark_id = entry["ark_id"]
        rel_path = entry["local_path"][0]
        if not os.path.isdir(os.path.join(base_dir, rel_path)):
            logger.warning(
                "[%s] local_path not found on read-only mount for ark_id=%s: %s",
                alias,
                ark_id,
                rel_path,
            )
        if dry_run:
            return entry, None
        err = copy_one(olr_index[ark_id], write_base_dir, rel_path, ark_id)
        return entry, err

    if to_copy:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(submit, entry) for entry in to_copy]
            for fut in as_completed(futures):
                entry, err = fut.result()
                if err is not None:
                    n_errors += 1
                    logger.error(
                        "[%s] failed to copy OLR for ark_id=%s: %s", alias, entry["ark_id"], err
                    )
                else:
                    entry["olr_file"] = True

    total_issues = len(items)
    matched = sum(1 for _, _, e in items if e.get("olr_file"))
    return {
        "alias": alias,
        "total_issues": total_issues,
        "matched_this_run": len(to_copy) - n_errors,
        "copy_errors": n_errors,
        "olr_file_true": matched,
        "coverage_pct": round(100 * matched / total_issues, 2) if total_issues else 0.0,
    }


def write_unmatched_report(olr_index: dict, ark_to_alias: dict, out_path: str) -> int:
    unmatched = [ark_id for ark_id in olr_index if ark_id not in ark_to_alias]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for ark_id in sorted(unmatched):
            f.write(ark_id + "\n")
    return len(unmatched)


def write_stats_report(data: dict, failed_arks: set, out_path: str) -> list:
    """Write per-alias OLR coverage stats. For issues with no OLR file, split
    the count between "accounted for" (ark_id is in the BnF failed_arks.txt,
    i.e. BnF confirms no OLR exists) and "unexplained" (neither an OLR file
    nor a failed_arks.txt entry was found -- worth investigating).

    Returns the list of (alias, ark_id, local_path) unexplained-missing rows.
    """
    stats = {}
    unexplained_rows = []
    for alias, year_dict in data.items():
        items = list(flatten_alias(year_dict))
        total = len(items)
        matched = sum(1 for _, _, e in items if e.get("olr_file"))
        missing_in_failed_arks = 0
        for _, _, entry in items:
            if entry.get("olr_file"):
                continue
            if entry["ark_id"] in failed_arks:
                missing_in_failed_arks += 1
            else:
                unexplained_rows.append((alias, entry["ark_id"], entry["local_path"][0]))
        stats[alias] = {
            "total_issues": total,
            "olr_file_true": matched,
            "missing_in_failed_arks": missing_in_failed_arks,
            "missing_unexplained": total - matched - missing_in_failed_arks,
            "coverage_pct": round(100 * matched / total, 2) if total else 0.0,
        }
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    return unexplained_rows


def write_missing_report(unexplained_rows: list, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("alias\tark_id\tlocal_path\n")
        for alias, ark_id, local_path in unexplained_rows:
            f.write(f"{alias}\t{ark_id}\t{local_path}\n")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--issue-index", default=DEFAULT_ISSUE_INDEX)
    parser.add_argument("--olr-dir", default=None, help="default: <base-dir>/BNF/new_olr")
    parser.add_argument(
        "--failed-arks-file", default=None, help="default: <olr-dir>/failed_arks.txt"
    )
    parser.add_argument("--base-dir", default=DEFAULT_BASE_DIR, help="read-only mount")
    parser.add_argument("--write-base-dir", default=DEFAULT_WRITE_BASE_DIR, help="read-write mount")
    parser.add_argument("--aliases", nargs="+", default=None, help="only process these aliases")
    parser.add_argument("--skip-aliases", nargs="+", default=None, help="skip these aliases")
    parser.add_argument(
        "--force", action="store_true", help="reprocess aliases already marked complete"
    )
    parser.add_argument("--progress-file", default=DEFAULT_PROGRESS_FILE)
    parser.add_argument("--stats-report", default=DEFAULT_STATS_REPORT)
    parser.add_argument("--unmatched-report", default=DEFAULT_UNMATCHED_REPORT)
    parser.add_argument("--missing-report", default=DEFAULT_MISSING_REPORT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        filename=args.log_file,
    )
    if args.log_file:
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.getLogger().addHandler(console)

    olr_dir = args.olr_dir or os.path.join(args.base_dir, "BNF", "new_olr")
    failed_arks_file = args.failed_arks_file or os.path.join(olr_dir, "failed_arks.txt")

    if args.dry_run:
        logger.info("DRY RUN: no files will be copied, no index/progress files written")
    logger.warning(
        "Writes will target read-write mount: %s (reads/resolution use: %s)",
        args.write_base_dir,
        args.base_dir,
    )

    logger.info("Loading issue index: %s", args.issue_index)
    with open(args.issue_index, encoding="utf-8") as f:
        data = json.load(f)

    logger.info("Scanning OLR deposit: %s", olr_dir)
    olr_index = build_olr_index(olr_dir)

    # global ark_id -> alias map, built over the WHOLE index regardless of
    # --aliases/--skip-aliases, so the unmatched report stays accurate.
    ark_to_alias = {
        entry["ark_id"]: alias
        for alias, year_dict in data.items()
        for _, _, entry in flatten_alias(year_dict)
    }

    all_aliases = sorted(data.keys())
    selected = set(args.aliases) if args.aliases else set(all_aliases)
    selected -= set(args.skip_aliases or [])
    n_selected_before_resume = len(selected)
    completed = load_progress(args.progress_file)
    if not args.force:
        selected -= completed

    aliases_to_process = [a for a in all_aliases if a in selected]
    logger.info(
        "%d/%d aliases to process (%d already completed and skipped)",
        len(aliases_to_process),
        len(all_aliases),
        n_selected_before_resume - len(aliases_to_process),
    )

    failed_arks = load_failed_arks(failed_arks_file)

    start = time.time()
    for i, alias in enumerate(aliases_to_process, 1):
        stats = process_alias(
            alias,
            data[alias],
            olr_index,
            args.base_dir,
            args.write_base_dir,
            args.workers,
            args.dry_run,
        )
        logger.info(
            "[%d/%d] %s: %d/%d issues have olr_file (+%d this run, %d errors) elapsed=%.0fs",
            i,
            len(aliases_to_process),
            alias,
            stats["olr_file_true"],
            stats["total_issues"],
            stats["matched_this_run"],
            stats["copy_errors"],
            time.time() - start,
        )
        if not args.dry_run:
            if stats["copy_errors"] == 0:
                completed.add(alias)
                save_progress(args.progress_file, completed)
            else:
                logger.warning(
                    "[%s] not marked complete (%d copy errors) -- will be retried next run",
                    alias,
                    stats["copy_errors"],
                )

        # log and report after each alias
        n_unmatched = write_unmatched_report(olr_index, ark_to_alias, args.unmatched_report)
        logger.info("%d OLR files have no matching issue -> %s", n_unmatched, args.unmatched_report)

        unexplained_rows = write_stats_report(data, failed_arks, args.stats_report)
        logger.info("Wrote per-alias coverage report -> %s", args.stats_report)
        write_missing_report(unexplained_rows, args.missing_report)
        logger.info(
            "%d issues missing an OLR file are NOT in failed_arks.txt (unexplained) -> %s",
            len(unexplained_rows),
            args.missing_report,
        )

        if not args.dry_run:
            # write to a temp file + atomic rename: this now runs once per
            # alias, so a kill/crash mid-write must never truncate the real
            # (previously-good) index file on disk.
            tmp_path = args.issue_index + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, args.issue_index)
            logger.info("Wrote updated issue index -> %s", args.issue_index)

    if args.dry_run:
        logger.info("DRY RUN complete, issue index NOT written")
    logger.info("Done.")


if __name__ == "__main__":
    main()
