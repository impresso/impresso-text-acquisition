#!/usr/bin/env python3
"""Analyze and classify the errors in a BnF downloader ``failed.jsonl`` report.

The BnF downloader (``fetch_data_via_API.py``) writes one JSON object per failed
issue to ``failed.jsonl``. This standalone tool triages that report: it classifies
every error, prints a terminal summary, and (by default) writes a CSV with one row
per error entry — including the *raw* error message — for drill-down.

Two-tier classification:

* **Curated rules** name the failure modes we already understand (HTTP 403 on the
  manifest, year-only date parse errors, "no usable pages", per-page ALTO 500s, …).
* **Auto-signatures** catch everything else: any unrecognized error is normalized
  (arks / URLs / quoted literals / numbers stripped) and aggregated under an
  ``auto: <signature>`` bucket, so a brand-new failure mode that appears N times
  shows up as one named bucket with count N — no code change required. New upcoming
  errors are therefore picked up and aggregated on their own, and surfaced in a
  dedicated "newly-discovered" section of the summary.

Standalone: standard library only (runs under a bare ``python3`` anywhere, incl.
the dhlab server, with zero install).

Usage
-----
    python3 analyze_failed.py FAILED_JSONL [-o OUT.csv] [--no-csv] [--top N]

Examples
--------
    python3 analyze_failed.py reports/failed.jsonl
    python3 analyze_failed.py reports/failed.jsonl -o triage.csv --top 15
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Iterator

# --- classification ---------------------------------------------------------

# HTTP errors surfaced by httpx's raise_for_status(), e.g.
#   Client error '403 ' for url 'https://.../manifest.json'
#   Server error '500 Internal Server Error' for url 'https://.../f2/alto.xml'
HTTP_RX = re.compile(r"(?:Client|Server) error '(\d{3})")

# Content-specific messages raised by parse_manifest()/the downloader. Order
# matters: these win over the generic HTTP text (a "cache load: …: unparseable
# Date …" is a date_parse, not a cache_load).
CONTENT_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("date_parse", re.compile(r"unparseable Date")),
    ("no_usable_pages", re.compile(r"no usable pages")),
    ("no_canvases", re.compile(r"no items \(canvases\)")),
    ("no_date_metadata", re.compile(r"no 'Date' metadata")),
    ("empty_alto", re.compile(r"empty ALTO for")),
    ("non_canonical", re.compile(r"non-canonical issue_id")),
    ("throttle", re.compile(r"Message throttled out|\b9008\d{2}\b")),
]

# Transport-layer / decode messages, checked after HTTP status errors.
TAIL_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("timeout", re.compile(r"Timeout|timed out", re.IGNORECASE)),
    ("transport", re.compile(r"ConnectError|TransportError|connection", re.IGNORECASE)),
    ("json_decode", re.compile(r"Expecting value|JSONDecode|Extra data")),
]


def url_target(error_str: str) -> str:
    """Which resource the (HTTP) error was on, derived from the URL in the message."""
    if "manifest.json" in error_str:
        return "manifest"
    if "alto.xml" in error_str or "/alto" in error_str:
        return "alto"
    if re.search(r"https?://", error_str):
        return "other_url"
    return ""


def http_status(error_str: str) -> str:
    """The 3-digit HTTP status embedded in an httpx error, or ''."""
    m = HTTP_RX.search(error_str)
    return m.group(1) if m else ""


def normalize(msg: str) -> str:
    """Collapse the variable parts of an error so recurring unknowns aggregate.

    URLs, quoted literals, ark identifiers and digit runs become placeholders, so
    e.g. "bpt6k12: boom at page 7 (id 'abc')" and "bd6t99: boom at page 42 (id
    'xyz')" both normalize to "ARK: boom at page N (id 'X')".
    """
    s = msg.strip()
    s = re.sub(r"https?://\S+", "<url>", s)
    s = re.sub(r"'[^']*'", "'X'", s)
    s = re.sub(r'"[^"]*"', '"X"', s)
    s = re.sub(r"ark:/?\S*", "ARK", s, flags=re.IGNORECASE)
    s = re.sub(r"\bb[a-z0-9]{6,}\b", "ARK", s, flags=re.IGNORECASE)
    s = re.sub(r"\d+", "N", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def classify(error_str: str) -> tuple[str, str]:
    """Return ``(error_category, classified)`` where classified is 'known'|'auto'."""
    s = error_str.strip()
    if not s:
        return ("empty_error", "known")

    for cat, rx in CONTENT_RULES:
        if rx.search(error_str):
            return (cat, "known")

    if HTTP_RX.search(error_str):
        tgt = url_target(error_str)
        if tgt == "manifest":
            return ("http_manifest", "known")
        if tgt == "alto":
            return ("http_alto", "known")
        return ("http_other", "known")

    for cat, rx in TAIL_RULES:
        if rx.search(error_str):
            return (cat, "known")

    if s.startswith("cache load:"):
        return ("cache_load", "known")

    return ("auto: " + normalize(error_str), "auto")


# --- input ------------------------------------------------------------------

CSV_FIELDS = [
    "alias",
    "ark",
    "issue_id",
    "status",
    "num_pages",
    "pages_ok",
    "page",
    "error_category",
    "classified",
    "http_status",
    "url_target",
    "raw_error",
    "timestamp",
]


def iter_rows(path: Path) -> Iterator[tuple[dict | None, int]]:
    """Yield ``(record, lineno)`` for each JSON line; ``None`` for malformed lines."""
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line), lineno
            except json.JSONDecodeError:
                yield None, lineno


def record_to_rows(rec: dict) -> list[dict]:
    """Explode one report record into one CSV row per error entry."""
    status = rec.get("status")

    if status == "throttle_halt":
        raw = f"code={rec.get('code')} nextAccessTime={rec.get('nextAccessTime')}"
        return [
            {
                "alias": "",
                "ark": "",
                "issue_id": "",
                "status": status,
                "num_pages": "",
                "pages_ok": "",
                "page": "",
                "error_category": "throttle",
                "classified": "known",
                "http_status": "",
                "url_target": "",
                "raw_error": raw,
                "timestamp": rec.get("timestamp", ""),
            }
        ]

    base = {
        "alias": rec.get("alias", ""),
        "ark": rec.get("ark", ""),
        "issue_id": rec.get("issue_id") or "",
        "status": status or "",
        "num_pages": rec.get("num_pages", ""),
        "pages_ok": rec.get("pages_ok", ""),
        "timestamp": rec.get("timestamp", ""),
    }

    errors = rec.get("errors") or [{"page": None, "error": ""}]
    rows = []
    for err in errors:
        raw = err.get("error", "") if isinstance(err, dict) else str(err)
        page = err.get("page") if isinstance(err, dict) else None
        category, classified = classify(raw)
        rows.append(
            {
                **base,
                "page": "" if page is None else page,
                "error_category": category,
                "classified": classified,
                "http_status": http_status(raw),
                "url_target": url_target(raw),
                "raw_error": raw,
            }
        )
    return rows


# --- reporting --------------------------------------------------------------

def _bar(count: int, total: int, width: int = 24) -> str:
    filled = 0 if total == 0 else round(width * count / total)
    return "█" * filled + " " * (width - filled)


def print_summary(
    *,
    path: Path,
    total_lines: int,
    n_records: int,
    n_malformed: int,
    n_throttle: int,
    rows: list[dict],
    top: int,
    csv_path: Path | None,
) -> None:
    cat_counts = Counter(r["error_category"] for r in rows)
    total_errors = sum(cat_counts.values())
    classified_counts = Counter(r["classified"] for r in rows)
    status_counts = Counter(r["http_status"] for r in rows if r["http_status"])
    target_counts = Counter(r["url_target"] for r in rows if r["url_target"])

    # record-level alias tally (dedup per record via (ark, timestamp))
    seen: set[tuple] = set()
    alias_counts: Counter[str] = Counter()
    for r in rows:
        if r["status"] == "throttle_halt":
            continue
        key = (r["ark"], r["timestamp"])
        if key in seen:
            continue
        seen.add(key)
        alias_counts[r["alias"]] += 1

    # first-seen raw example per auto: bucket
    auto_examples: dict[str, str] = {}
    for r in rows:
        cat = r["error_category"]
        if r["classified"] == "auto" and cat not in auto_examples:
            auto_examples[cat] = r["raw_error"]

    w = print  # local alias

    w("")
    w("=" * 72)
    w(f" BnF failure analysis — {path}")
    w("=" * 72)
    w(f" lines read           : {total_lines}")
    w(f" records parsed       : {n_records}")
    w(f" malformed skipped    : {n_malformed}")
    w(f" throttle_halt records: {n_throttle}")
    w(f" failed records       : {n_records - n_throttle}")
    w(f" error entries (rows) : {total_errors}"
      f"  ({classified_counts['known']} known, {classified_counts['auto']} auto)")

    w("")
    w(" error categories (by error entry)")
    w(" " + "-" * 70)
    label_w = max((len(c) for c in cat_counts), default=10)
    label_w = min(max(label_w, 12), 42)
    for cat, n in cat_counts.most_common():
        tag = "•" if cat.startswith("auto: ") else " "
        pct = 100 * n / total_errors if total_errors else 0
        w(f" {tag} {cat[:label_w]:<{label_w}}  {n:>6}  {pct:5.1f}%  {_bar(n, total_errors)}")

    if status_counts:
        w("")
        w(" HTTP status codes            url target")
        w(" " + "-" * 70)
        codes = "   ".join(f"{c}×{n}" for c, n in status_counts.most_common())
        targets = "   ".join(f"{t}×{n}" for t, n in target_counts.most_common())
        w(f"   {codes:<28} {targets}")

    if alias_counts:
        w("")
        w(f" top {top} aliases by failed records")
        w(" " + "-" * 70)
        a_w = min(max((len(a) for a in alias_counts), default=8), 30)
        for alias, n in alias_counts.most_common(top):
            w(f"   {alias[:a_w]:<{a_w}}  {n:>6}")

    w("")
    if auto_examples:
        w(f" newly-discovered errors (not covered by curated rules) — {len(auto_examples)} bucket(s)")
        w(" " + "-" * 70)
        for cat, n in cat_counts.most_common():
            if not cat.startswith("auto: "):
                continue
            example = auto_examples[cat].replace("\n", " ⏎ ")
            w(f"   [{n:>5}] {cat[len('auto: '):]}")
            w(f"           e.g. {example[:110]}")
    else:
        w(" newly-discovered errors: none — every error matched a curated rule ✓")

    w("")
    if csv_path is not None:
        w(f" CSV written: {csv_path}  ({total_errors} rows)")
    else:
        w(" CSV: skipped (--no-csv)")
    w("=" * 72)
    w("")


# --- main -------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Classify and summarize a BnF downloader failed.jsonl report.",
    )
    parser.add_argument("failed_jsonl", type=Path, help="path to failed.jsonl")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="CSV output path (default: <input>_classified.csv beside the input)",
    )
    parser.add_argument(
        "--no-csv", action="store_true", help="print the summary only, write no CSV"
    )
    parser.add_argument(
        "--top", type=int, default=10, help="how many top aliases to list (default 10)"
    )
    args = parser.parse_args(argv)

    path: Path = args.failed_jsonl
    if not path.is_file():
        parser.error(f"no such file: {path}")

    rows: list[dict] = []
    n_records = n_malformed = n_throttle = total_lines = 0
    for rec, lineno in iter_rows(path):
        total_lines += 1
        if rec is None:
            n_malformed += 1
            print(f"  ! skipping malformed JSON on line {lineno}", file=sys.stderr)
            continue
        n_records += 1
        if rec.get("status") == "throttle_halt":
            n_throttle += 1
        rows.extend(record_to_rows(rec))

    csv_path: Path | None = None
    if not args.no_csv:
        csv_path = args.output or path.with_name(path.stem + "_classified.csv")
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    print_summary(
        path=path,
        total_lines=total_lines,
        n_records=n_records,
        n_malformed=n_malformed,
        n_throttle=n_throttle,
        rows=rows,
        top=args.top,
        csv_path=csv_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
