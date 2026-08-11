"""Classify BNF ALTO page formats across the three legacy/new issue indices.

This script answers the practical question: *"Can the page files of these
issues be processed in the same way?"* for the whole BNF collection (Set 0
Europeana with OLR, Set 1 Marche Presse with OLR, Set 2 Colonial Press + all
titles without OLR).

For each issue listed in the three issue-index JSON files, one ALTO page file
is sampled (first page by default) and classified according to two structural
criteria that are known to affect the shared parsing code in
``text_preparation/importers/mets_alto/alto.py`` and
``text_preparation/importers/mets_alto/classes.py``:

1. ``block_structure``: whether the direct children of ``<PrintSpace>`` are
   flat ``<TextBlock>`` elements ("flat") or include nested
   ``<ComposedBlock>`` containers ("nested"). ``parse_printspace()`` only
   inspects *direct* children of ``<PrintSpace>`` when building content-item
   mappings, so nested blocks change how regions must be walked.
2. ``coords_status``: whether every ``<TextBlock>``, ``<ComposedBlock>`` and
   ``<TextLine>`` element carries valid ``HPOS``/``VPOS``/``WIDTH``/``HEIGHT``
   attributes ("complete") or not ("incomplete"). ``distill_coordinates()``
   raises an uncaught exception on blocks/lines missing these attributes.

These two criteria are combined into a small, finite ``alto_format`` label,
e.g. ``flat-complete``, ``nested-incomplete``. Special labels are used for
issues that could not be classified at all: ``blank-page`` (no <PrintSpace>),
``empty-printspace`` (no blocks under <PrintSpace>), ``path-not-found``,
``missing-file``, ``read-error``, ``parse-error``, ``no-local-path``.

The image dimensions declared on the ALTO ``<Page>`` element (``WIDTH``,
``HEIGHT``) are also extracted and stored as ``img_size`` (``[w, h]``) on
each issue entry, as this is independently useful during ingestion.

The root element's default namespace (``xmlns``) is recorded per issue as
``alto_schema`` (e.g. ``http://www.loc.gov/standards/alto/ns-v3#``,
``http://bibnum.bnf.fr/ns/alto_prod``, or ``null`` for the un-namespaced
BNF-EN legacy files). It is kept for reference/statistics only, since it
does not reliably predict either criterion above and is *not* part of the
``alto_format`` classification key.

Usage:
    python classify_alto_formats.py [--indices bnf_ocr,bnf_mp,bnf_en]
        [--base-dir /mnt/project_impresso/original]
        [--index-dir text_preparation/data/issue_indices]
        [--out-dir text_preparation/data/sample_data/BNF_API/alto_format_reports]
        [--workers 24] [--page-choice first|random] [--seed 42]
        [--limit-per-alias N] [--log-file path] [--log-level INFO]

Outputs (written to --out-dir, originals are never modified):
    issue_index.<key>.with_alto_format.json   (copy + "alto_format"/"img_size")
    alto_format_flags.csv                     (path/parsing inconsistencies)
    alto_format_stats.json                    (raw counts, for the .md report)
"""

import argparse
import csv
import gzip
import json
import logging
import os
import random
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

logger = logging.getLogger("classify_alto_formats")

DEFAULT_BASE_DIR = "/mnt/project_impresso/original"
DEFAULT_INDEX_DIR = "text_preparation/data/issue_indices"
DEFAULT_OUT_DIR = "text_preparation/data/sample_data/BNF_API/alto_format_reports"

INDEX_FILES = {
    "bnf_ocr": "issue_index.bnf_ocr.json",
    "bnf_mp": "issue_index.bnf_mp.json",
    "bnf_en": "issue_index.bnf_en.json",
}

COORD_ATTRS = ("HPOS", "VPOS", "WIDTH", "HEIGHT")

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]")


def _normalize(s: str) -> str:
    """Lowercase and strip all non-alphanumeric chars, for loose alias/path matching."""
    return _NON_ALNUM_RE.sub("", s.lower())


def alias_matches_path(alias: str, rel_path: str) -> bool:
    """Loose check that the alias appears in the issue's relative path,
    ignoring case and punctuation (e.g. alias 'lematin' vs dir 'Le-Matin')."""
    return _normalize(alias) in _normalize(rel_path)


# Labels that mean "could not classify" (as opposed to real alto_format groups)
ERROR_LABELS = {
    "no-local-path",
    "path-not-found",
    "missing-file",
    "read-error",
    "parse-error",
}


def find_page_dir(issue_abs: str, sort: bool = False):
    """Return (page_dir, sorted xml filenames) for the first existing dir among
    ocr/, ALTO/, or the issue root that contains xml files. ([], []) if none."""
    for sub in ("ocr", "ALTO", ""):
        d = os.path.join(issue_abs, sub) if sub else issue_abs
        if os.path.isdir(d):
            if sort:
                files = sorted(
                    f
                    for f in os.listdir(d)
                    if f.lower().endswith(".xml") or f.lower().endswith(".xml.gz")
                )
            else:
                files = [
                    f
                    for f in os.listdir(d)
                    if f.lower().endswith(".xml") or f.lower().endswith(".xml.gz")
                ]
            if files:
                return d, files
    return None, []


def read_xml_bytes(path: str) -> bytes:
    if path.endswith(".gz"):
        with gzip.open(path, "rb") as fh:
            return fh.read()
    with open(path, "rb") as fh:
        return fh.read()


def has_valid_coords(tag) -> bool:
    for attr in COORD_ATTRS:
        v = tag.get(attr)
        if v is None:
            return False
        try:
            float(v)
        except (TypeError, ValueError):
            return False
    return True


def classify_page(xml_bytes: bytes) -> dict:
    """Classify a single ALTO page's structure. Returns a result dict with
    keys: label, block_structure, coords_status, img_size, xmlns, schema."""
    soup = BeautifulSoup(xml_bytes, "xml")
    root = soup.find()
    xmlns = root.get("xmlns") if root is not None else None
    schema = root.get("xsi:schemaLocation") if root is not None else None

    page_tag = soup.find("Page")
    img_size = None
    if page_tag is not None:
        w, h = page_tag.get("WIDTH"), page_tag.get("HEIGHT")
        try:
            img_size = [int(float(w)), int(float(h))]
        except (TypeError, ValueError):
            img_size = None

    base = {"img_size": img_size, "xmlns": xmlns, "schema": schema}

    printspace = soup.find("PrintSpace")
    if printspace is None:
        return {**base, "label": "blank-page", "block_structure": None, "coords_status": None}

    direct_children = [c for c in printspace.children if getattr(c, "name", None)]
    if not direct_children:
        return {**base, "label": "empty-printspace", "block_structure": None, "coords_status": None}

    child_names = {c.name for c in direct_children}
    block_structure = "nested" if "ComposedBlock" in child_names else "flat"

    coords_ok = True
    for tag_name in ("TextBlock", "ComposedBlock", "TextLine"):
        for el in printspace.find_all(tag_name):
            if not has_valid_coords(el):
                coords_ok = False
                break
        if not coords_ok:
            break

    coords_status = "complete" if coords_ok else "incomplete"
    return {
        **base,
        "label": f"{block_structure}-{coords_status}",
        "block_structure": block_structure,
        "coords_status": coords_status,
    }


def process_issue(
    base_dir: str, alias: str, rel_path: str, page_choice: str, rng: random.Random
) -> dict:
    """Locate + classify one sampled page for a single issue. Never raises."""
    issue_abs = os.path.join(base_dir, rel_path)

    if not os.path.isdir(issue_abs):
        return {
            "label": "path-not-found",
            "img_size": None,
            "xmlns": None,
            "schema": None,
            "block_structure": None,
            "coords_status": None,
            "alias_mismatch": not alias_matches_path(alias, rel_path),
            "page_file": None,
        }

    # if choosing at random, no need to sort
    page_dir, files = find_page_dir(issue_abs, sort=page_choice == "first")
    if not files:
        return {
            "label": "missing-file",
            "img_size": None,
            "xmlns": None,
            "schema": None,
            "block_structure": None,
            "coords_status": None,
            "alias_mismatch": not alias_matches_path(alias, rel_path),
            "page_file": None,
        }

    # chosen = rng.choice(files) if page_choice == "random" else files[0]
    chosen = files[0]
    page_path = os.path.join(page_dir, chosen)

    try:
        xml_bytes = read_xml_bytes(page_path)
    except Exception as exc:
        return {
            "label": "read-error",
            "img_size": None,
            "xmlns": None,
            "schema": None,
            "block_structure": None,
            "coords_status": None,
            "alias_mismatch": not alias_matches_path(alias, rel_path),
            "page_file": page_path,
            "error": str(exc),
        }

    try:
        result = classify_page(xml_bytes)
    except Exception as exc:
        return {
            "label": "parse-error",
            "img_size": None,
            "xmlns": None,
            "schema": None,
            "block_structure": None,
            "coords_status": None,
            "alias_mismatch": not alias_matches_path(alias, rel_path),
            "page_file": page_path,
            "error": str(exc),
        }

    result["alias_mismatch"] = not alias_matches_path(alias, rel_path)
    result["page_file"] = page_path
    return result


def flatten_index(data: dict, limit_per_alias: int | None):
    """Yield (alias, year, month, entry_dict) for every issue entry, in place."""
    for alias, year_dict in data.items():
        count = 0
        stop_alias = False
        for year, month_dict in year_dict.items():
            if stop_alias:
                break
            for month, entries in month_dict.items():
                if stop_alias:
                    break
                for entry in entries:
                    if limit_per_alias is not None and count >= limit_per_alias:
                        stop_alias = True
                        break
                    yield alias, year, month, entry
                    count += 1


def run_for_index(
    index_key: str,
    index_path: str,
    base_dir: str,
    workers: int,
    page_choice: str,
    seed: int,
    limit_per_alias: int | None,
    out_dir: str,
) -> dict:
    """Process one issue-index file end to end. Returns stats dict."""
    logger.info("[%s] Loading %s", index_key, index_path)
    with open(index_path, encoding="utf-8") as f:
        data = json.load(f)

    entries = list(flatten_index(data, limit_per_alias))
    total = len(entries)
    logger.info(
        "[%s] %d issues to classify (workers=%d, page_choice=%s)",
        index_key,
        total,
        workers,
        page_choice,
    )

    label_counter = Counter()
    alias_label_sets = defaultdict(set)
    flags = []  # rows for the flags CSV
    schema_counter = Counter()

    rng_master = random.Random(seed)
    start = time.time()
    done = 0
    log_every = max(1, total // 200) if total else 1

    def worker(item):
        alias, year, month, entry = item
        local_paths = entry.get("local_path") or []
        if not local_paths:
            return item, {
                "label": "no-local-path",
                "img_size": None,
                "xmlns": None,
                "schema": None,
                "block_structure": None,
                "coords_status": None,
                "alias_mismatch": True,
                "page_file": None,
            }
        rel_path = local_paths[0]
        # deterministic per-issue rng seed for reproducible "random" page choice
        seed_str = f"{seed}-{alias}-{year}-{month}-{entry.get('day')}-{entry.get('edition')}"
        rng = random.Random(seed_str)
        result = process_issue(base_dir, alias, rel_path, page_choice, rng)
        return item, result

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, item) for item in entries]
        for fut in as_completed(futures):
            (alias, year, month, entry), result = fut.result()

            entry["alto_format"] = result["label"]
            entry["img_size"] = result["img_size"]
            entry["alto_schema"] = result.get("xmlns")

            label_counter[result["label"]] += 1
            alias_label_sets[alias].add(result["label"])
            if result.get("xmlns"):
                schema_counter[(result["xmlns"], result.get("schema"))] += 1

            if result["label"] in ERROR_LABELS or result.get("alias_mismatch"):
                flags.append(
                    {
                        "index_file": index_key,
                        "alias": alias,
                        "year": year,
                        "month": month,
                        "day": entry.get("day"),
                        "edition": entry.get("edition"),
                        "local_path": entry.get("local_path", [None])[0],
                        "flag_type": (
                            result["label"]
                            if result["label"] in ERROR_LABELS
                            else "alias-path-mismatch"
                        ),
                        "detail": result.get("error", ""),
                    }
                )

            done += 1
            if done % log_every == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else float("inf")
                logger.info(
                    "[%s] %d/%d (%.1f%%) elapsed=%.0fs rate=%.1f/s eta=%.0fs",
                    index_key,
                    done,
                    total,
                    100 * done / max(total, 1),
                    elapsed,
                    rate,
                    eta,
                )

    # write the annotated copy
    os.makedirs(out_dir, exist_ok=True)
    out_json_path = os.path.join(out_dir, f"issue_index.{index_key}.with_alto_format.json")
    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("[%s] Wrote annotated index -> %s", index_key, out_json_path)

    mixed_aliases = {a: sorted(labels) for a, labels in alias_label_sets.items() if len(labels) > 1}

    return {
        "index_key": index_key,
        "total_issues": total,
        "label_counts": dict(label_counter),
        "mixed_format_aliases": mixed_aliases,
        "schema_counts": {f"{ns} | {sch}": c for (ns, sch), c in schema_counter.items()},
        "flags": flags,
        "n_aliases": len(alias_label_sets),
    }


def write_flags_csv(all_flags: list, out_dir: str) -> str:
    path = os.path.join(out_dir, "alto_format_flags.csv")
    fieldnames = [
        "index_file",
        "alias",
        "year",
        "month",
        "day",
        "edition",
        "local_path",
        "flag_type",
        "detail",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_flags:
            writer.writerow(row)
    return path


def write_stats_json(all_stats: dict, out_dir: str) -> str:
    path = os.path.join(out_dir, "alto_format_stats.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, indent=2, ensure_ascii=False)
    return path


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--indices",
        default="bnf_ocr,bnf_mp,bnf_en",
        help="Comma-separated subset of bnf_ocr,bnf_mp,bnf_en",
    )
    parser.add_argument("--base-dir", default=DEFAULT_BASE_DIR)
    parser.add_argument("--index-dir", default=DEFAULT_INDEX_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--page-choice", choices=["first", "random"], default="random")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--limit-per-alias",
        type=int,
        default=None,
        help="For smoke-testing: cap number of issues processed per alias",
    )
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        filename=args.log_file,
    )
    if args.log_file:
        # also echo to stdout
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.getLogger().addHandler(console)

    keys = [k.strip() for k in args.indices.split(",") if k.strip()]
    all_stats = {}
    all_flags = []

    for key in keys:
        if key not in INDEX_FILES:
            logger.warning("Unknown index key '%s', skipping", key)
            continue
        index_path = os.path.join(args.index_dir, INDEX_FILES[key])
        if not os.path.exists(index_path):
            logger.error("Index file not found: %s, skipping", index_path)
            continue
        stats = run_for_index(
            index_key=key,
            index_path=index_path,
            base_dir=args.base_dir,
            workers=args.workers,
            page_choice=args.page_choice,
            seed=args.seed,
            limit_per_alias=args.limit_per_alias,
            out_dir=args.out_dir,
        )
        all_flags.extend(stats.pop("flags"))
        all_stats[key] = stats

    flags_path = write_flags_csv(all_flags, args.out_dir)
    stats_path = write_stats_json(all_stats, args.out_dir)
    logger.info("Wrote flags CSV -> %s (%d rows)", flags_path, len(all_flags))
    logger.info("Wrote stats JSON -> %s", stats_path)
    logger.info("Done.")


if __name__ == "__main__":
    main()
