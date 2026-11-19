#!/usr/bin/env python3
"""Fetch IIIF presentation manifests for legacy BNF-EN ("Europeana") issues.

Legacy `en_olr` issues (batch ``BNF_EN_old`` in ``issue_index.bnf_en.json``) have
real METS+ALTO data on disk but no `manifest.json` — unlike the newer BNF API
batches (`no_olr`, `mp_olr`, and the ``BNF_API_NEW`` slice of `en_olr` itself),
which already come with one. Comparing ALTO/METS-reported page dimensions against
the live IIIF Image API for a sample issue showed the two don't always agree (and
not by a fixed ratio — it varies per page), so `en_olr` needs the *actual* served
image dimensions per page to correctly rescale OCR coordinates for that page. The
IIIF *presentation* manifest already carries every page's width/height in one
fetch per issue — the same data `helpers.get_manifest_info` already knows how to
parse out of `no_olr`'s/`mp_olr`'s `manifest.json` — so fetching and saving one
real `manifest.json` per legacy issue means `en_olr` needs no code path different
from what those two importers already do.

This script walks ``issue_index.bnf_en.json``, and for every ``BNF_EN_old`` entry
fetches that issue's IIIF v3 presentation manifest (by its ``ark_id``) and writes
it as ``manifest.json`` inside the issue's own directory (from ``local_path``) —
exactly where `no_olr`/`mp_olr` already expect to find one. Reuses `BnfApiClient`
from ``fetch_data_via_API.py`` for its already-proven auth/rate-limit/retry/WSO2-
throttle handling, rather than reimplementing any of that.

Reads (issue index, existence checks) use the read-only mount
``/mnt/project_impresso/original`` by default. Writes (the manifest.json files) go
to the read-write mount ``/mnt/project_impresso_rw/original`` — both are the same
underlying share mounted twice, so paths line up, but only the rw mount can
actually be written to (mirrors ``copy_olr_to_issues.py``'s convention).

Idempotent / resumable: an issue whose ``manifest.json`` already exists on the
write mount is skipped (unless ``--overwrite``), so an interrupted run can simply
be re-launched with the same arguments.

Usage::

    conda activate cpu
    source ~/.env  # BNF_API_KEY / BNF_API_SECRET (API is IP-whitelisted to dhlab)

    # Safe default: fetch + parse, write nothing.
    python fetch_en_manifest_dims.py --dry_run=True --limit=5

    # Real run, all legacy EN issues.
    python fetch_en_manifest_dims.py --dry_run=False

    # Real run, a couple of aliases only.
    python fetch_en_manifest_dims.py --dry_run=False --aliases=jdpl,lematin

    # Retry a curated subset of a previous run's failures: open the
    # failed_report jsonl, delete the lines for issues not worth chasing,
    # save it (in place or as a copy), then:
    python fetch_en_manifest_dims.py --dry_run=False --retry_from=<path-to-that-jsonl>
"""

import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime

import fire
from tqdm import tqdm

from impresso_essentials.utils import init_logger
from text_preparation.importers.bnf.preprocessing.fetch_data_via_API import (
    BnfApiClient,
    ThrottleHalt,
    _split_csv,
)

logger = logging.getLogger(__name__)

DEFAULT_ISSUE_INDEX = "/home/piconti/impresso-text-acquisition/text_preparation/data/issue_indices/issue_index.bnf_en.json"
DEFAULT_BASE_DIR = "/mnt/project_impresso/original"
DEFAULT_WRITE_BASE_DIR = "/mnt/project_impresso_rw/original"
DEFAULT_FAILED_REPORT_BASE = ".fetch_en_manifest_dims_failed"
TARGET_BATCH = "BNF_EN_old"
MANIFEST_FILENAME = "manifest.json"


@dataclass
class Work:
    """One legacy EN issue to fetch a manifest for."""

    issue_id: str
    alias: str
    ark_id: str
    local_path: str


def _atomic_write_json(path: str, data: dict) -> None:
    """Write ``data`` to ``path`` as JSON, atomically (temp file + ``os.replace``)."""
    tmp = f"{path}.part"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_retry_arks(path: str) -> set[str]:
    """Read a failed-report jsonl (this script's own output format) -> a set of ark_ids.

    Lets a retry run be restricted to a hand-curated subset: open the failed
    report, delete the lines for issues you've decided aren't worth chasing
    (permanently missing on the API, etc.), save it (in place or as a copy),
    and pass that path as ``retry_from``.
    """
    arks: set[str] = set()
    with open(path, "r", encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed line %d in %s.", line_num, path)
                continue
            ark = entry.get("ark_id")
            if ark:
                arks.add(ark)
    logger.info("Loaded %d ark_id(s) to retry from %s.", len(arks), path)
    return arks


def build_work_list(
    issue_index_path: str,
    aliases_include: list[str],
    aliases_exclude: list[str],
    retry_arks: set[str] | None = None,
) -> list[Work]:
    """Flatten ``issue_index.bnf_en.json`` into the ``BNF_EN_old`` entries to fetch.

    Args:
        issue_index_path: Path to ``issue_index.bnf_en.json``.
        aliases_include: Alias include-list (empty = all aliases).
        aliases_exclude: Alias exclude-list, applied after the include-list.
        retry_arks: If given, restrict the result to entries whose ``ark_id``
            is in this set (see `load_retry_arks`) — applied after the alias
            filters.

    Returns:
        list[Work]: One entry per legacy EN issue (``batch == "BNF_EN_old"``).
    """
    with open(issue_index_path, "r", encoding="utf-8") as fh:
        issue_index = json.load(fh)

    work: list[Work] = []
    for alias, years in issue_index.items():
        if aliases_include and alias not in aliases_include:
            continue
        if alias in aliases_exclude:
            continue
        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:
                    if entry.get("batch") != TARGET_BATCH:
                        continue
                    if retry_arks is not None and entry["ark_id"] not in retry_arks:
                        continue
                    day = entry.get("day")
                    edition = entry.get("edition", "a")
                    issue_id = f"{alias}-{year}-{month}-{day}-{edition}"
                    work.append(
                        Work(
                            issue_id=issue_id,
                            alias=alias,
                            ark_id=entry["ark_id"],
                            local_path=entry["local_path"][0],
                        )
                    )

    if retry_arks is not None:
        missing = retry_arks - {w.ark_id for w in work}
        if missing:
            logger.warning(
                "%d ark_id(s) from retry_from were not found in the issue index: %s",
                len(missing),
                ", ".join(sorted(missing)),
            )

    logger.info(
        "Built work list: %d '%s' issues across %d alias(es).",
        len(work),
        TARGET_BATCH,
        len({w.alias for w in work}),
    )
    return work


async def fetch_one(
    api: BnfApiClient,
    item: Work,
    base_dir: str,
    write_base_dir: str,
    *,
    overwrite: bool,
    dry_run: bool,
    stats: dict,
    progress: tqdm,
    failures: list[dict],
) -> None:
    """Fetch and save one issue's manifest, updating ``stats``/``failures`` in place."""
    try:
        read_dir = os.path.join(base_dir, item.local_path)
        if not os.path.isdir(read_dir):
            logger.warning(
                "%s: local_path not found on read-only mount: %s", item.issue_id, read_dir
            )
            stats["missing_dir"] += 1
            failures.append(
                {
                    "issue_id": item.issue_id,
                    "ark_id": item.ark_id,
                    "error": "missing local_path dir",
                }
            )
            return

        target = os.path.join(write_base_dir, item.local_path, MANIFEST_FILENAME)
        if not overwrite and os.path.isfile(target) and os.path.getsize(target) > 0:
            stats["skipped"] += 1
            return

        if dry_run:
            logger.info(
                "[dry-run] would fetch manifest for %s (%s) -> %s",
                item.issue_id,
                item.ark_id,
                target,
            )
            stats["would_fetch"] += 1
            return

        manifest = await api.get_manifest(item.ark_id)
        await asyncio.to_thread(_atomic_write_json, target, manifest)
        stats["fetched"] += 1
    except ThrottleHalt:
        raise
    except Exception as exc:  # noqa: BLE001 — one bad issue must not abort the run
        logger.warning("%s (%s): manifest fetch failed: %s", item.issue_id, item.ark_id, exc)
        stats["failed"] += 1
        failures.append({"issue_id": item.issue_id, "ark_id": item.ark_id, "error": str(exc)})
    finally:
        progress.update(1)


async def run(
    work: list[Work],
    base_dir: str,
    write_base_dir: str,
    *,
    overwrite: bool,
    dry_run: bool,
    manifest_rate_per_min: float,
    manifest_concurrency: int,
    max_retries: int,
    timeout: float,
    throttle_sleep_threshold_min: float,
    failed_report: str,
) -> dict:
    """Drive the fetch for the whole work list through one shared `BnfApiClient`."""
    stats = {"fetched": 0, "skipped": 0, "would_fetch": 0, "failed": 0, "missing_dir": 0}
    failures: list[dict] = []

    async with BnfApiClient(
        manifest_rate_per_min=manifest_rate_per_min,
        alto_rate_per_min=1.0,  # unused here — this script never calls get_alto
        manifest_concurrency=manifest_concurrency,
        alto_concurrency=1,
        max_retries=max_retries,
        timeout=timeout,
        throttle_sleep_threshold_min=throttle_sleep_threshold_min,
    ) as api:
        sem = asyncio.Semaphore(manifest_concurrency)

        async def bounded(item: Work, progress: tqdm) -> None:
            async with sem:
                await fetch_one(
                    api,
                    item,
                    base_dir,
                    write_base_dir,
                    overwrite=overwrite,
                    dry_run=dry_run,
                    stats=stats,
                    progress=progress,
                    failures=failures,
                )

        with tqdm(total=len(work), unit="issue", desc="EN manifests") as progress:
            try:
                await asyncio.gather(*[bounded(item, progress) for item in work])
            except ThrottleHalt as halt:
                logger.error(
                    "Throttled until %s — halting; re-run with the same arguments to resume "
                    "(already-written manifests are skipped).",
                    halt.info.get("nextAccessTime", "?"),
                )
                raise

    if failures and failed_report:
        os.makedirs(os.path.dirname(failed_report) or ".", exist_ok=True)
        with open(failed_report, "w", encoding="utf-8") as fh:
            for entry in failures:
                entry["timestamp"] = datetime.now().isoformat()
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info("Wrote %d failure(s) to %s", len(failures), failed_report)

    return stats


def main(
    issue_index: str = DEFAULT_ISSUE_INDEX,
    base_dir: str = DEFAULT_BASE_DIR,
    write_base_dir: str = DEFAULT_WRITE_BASE_DIR,
    aliases: str = "",
    exclude: str = "",
    retry_from: str = "",
    limit: int = 0,
    overwrite: bool = False,
    dry_run: bool = True,
    manifest_rate_per_min: float = 50.0,
    manifest_concurrency: int = 4,
    timeout: float = 30.0,
    max_retries: int = 5,
    throttle_sleep_threshold_min: float = 15.0,
    failed_report: str = "",
    log_level: str = "INFO",
    log_file: str = "",
) -> None:
    """Fetch and save one IIIF presentation manifest.json per legacy BNF-EN issue.

    Args:
        issue_index: Path to ``issue_index.bnf_en.json``.
        base_dir: Read-only mount root for existence checks (``local_path`` is
            relative to this).
        write_base_dir: Read-write mount root the ``manifest.json`` files are
            actually written under (same share as ``base_dir``, writable).
        aliases: Comma-separated alias include-list (empty = all legacy EN aliases).
        exclude: Comma-separated alias exclude-list.
        retry_from: Path to a failed-report jsonl (this script's own output format,
            e.g. a hand-trimmed copy of a previous ``failed_report``) — restricts
            the run to just those ``ark_id``s, on top of any alias filters. Empty
            (default) = process the full alias-filtered set.
        limit: If > 0, only process the first N issues (after filtering) — for a
            smoke test before a full run.
        overwrite: Re-fetch and overwrite a manifest.json even if one already
            exists on the write mount.
        dry_run: When True (default), fetch nothing — just report what would be
            fetched. Set ``--dry_run=False`` for a real run.
        manifest_rate_per_min: BnF's presentation-API rate cap (50/min).
        manifest_concurrency: Max in-flight manifest requests.
        timeout: Per-request timeout in seconds.
        max_retries: Retry attempts for transient network/5xx errors.
        throttle_sleep_threshold_min: On a WSO2 throttle, sleep if the wait is
            within this many minutes; otherwise halt (re-run to resume).
        failed_report: Where to write the list of issues whose fetch failed
            (JSON lines). Empty (default) = auto-name it
            ``{DEFAULT_FAILED_REPORT_BASE}.{run timestamp}.jsonl``, so each
            run gets its own file instead of overwriting the previous one.
            Pass an explicit path (or ``"none"``) to opt out of that.
        log_level: One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
        log_file: Log destination. Empty = stdout.
    """
    if not os.path.isfile(issue_index):
        print(f"Error: issue_index not found: {issue_index}", file=sys.stderr)
        sys.exit(1)

    level = getattr(logging, log_level.upper(), None)
    if level is None:
        print(f"Error: invalid log_level: {log_level}", file=sys.stderr)
        sys.exit(1)
    init_logger(logger, level, log_file or None)

    missing_creds = [v for v in ("BNF_API_KEY", "BNF_API_SECRET") if not os.environ.get(v)]
    if missing_creds:
        logger.error(
            "Credentials not set: %s. `source ~/.env` on dhlab (the API is IP-whitelisted "
            "there) before running.",
            ", ".join(missing_creds),
        )
        sys.exit(1)

    if failed_report == "none":
        failed_report = ""
    elif not failed_report:
        failed_report = f"{DEFAULT_FAILED_REPORT_BASE}.{datetime.now():%Y-%m-%dT%H%M%S}.jsonl"

    print(f"Dry run:    {dry_run}", flush=True)
    print(f"Issue index: {issue_index}", flush=True)
    print(f"Write to:   {write_base_dir}/<local_path>/{MANIFEST_FILENAME}", flush=True)
    print(f"Failed report: {failed_report or '(none)'}", flush=True)

    retry_arks = None
    if retry_from:
        if not os.path.isfile(retry_from):
            print(f"Error: retry_from not found: {retry_from}", file=sys.stderr)
            sys.exit(1)
        retry_arks = load_retry_arks(retry_from)
        if not retry_arks:
            logger.warning("retry_from (%s) contained no ark_id(s) — nothing to do.", retry_from)
            return

    work = build_work_list(issue_index, _split_csv(aliases), _split_csv(exclude), retry_arks)
    if limit > 0:
        work = work[:limit]
        logger.info("Limiting to the first %d issue(s).", limit)
    if not work:
        logger.warning("No matching '%s' issues to process — nothing to do.", TARGET_BATCH)
        return

    try:
        stats = asyncio.run(
            run(
                work,
                base_dir,
                write_base_dir,
                overwrite=overwrite,
                dry_run=dry_run,
                manifest_rate_per_min=manifest_rate_per_min,
                manifest_concurrency=manifest_concurrency,
                max_retries=max_retries,
                timeout=timeout,
                throttle_sleep_threshold_min=throttle_sleep_threshold_min,
                failed_report=failed_report,
            )
        )
    except ThrottleHalt:
        sys.exit(75)  # EX_TEMPFAIL — re-run to resume (already-written manifests are skipped)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("Interrupted — re-run with the same arguments to resume.")
        sys.exit(130)  # 128 + SIGINT

    logger.info(
        "Done: %d fetched, %d skipped (already present), %d would-fetch (dry-run), "
        "%d failed, %d missing local_path dir.",
        stats["fetched"],
        stats["skipped"],
        stats["would_fetch"],
        stats["failed"],
        stats["missing_dir"],
    )


if __name__ == "__main__":
    fire.Fire(main)
