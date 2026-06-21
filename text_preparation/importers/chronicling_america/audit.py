"""Read-only audit of Chronicling America (NDNP METS/ALTO) downloads.

Chronicling America is a single format (NDNP METS/ALTO) with variation along a
handful of axes (measurement unit / scan resolution, how images are encoded in
ALTO, METS structMap conventions, on-disk layout, language, ...). Rather than
guess which variants a given download contains, this tool *measures* them: it
walks a directory tree, inspects every issue's METS + ALTO, and tabulates the
distribution of the attributes the importer actually depends on.

It performs **no conversion and no network access** — it only reads files and
prints a summary. Use it before a large import to learn what the converter must
cope with, and to spot issues that need special handling.

Example::

    python -m text_preparation.importers.chronicling_america.audit \\
        --input-dir "/path/to/downloads" --alto-sample 3

    # machine-readable:
    python -m text_preparation.importers.chronicling_america.audit \\
        --input-dir "/path/to/downloads" --json report.json
"""

import argparse
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass, field

from bs4 import BeautifulSoup

# METS file name = 10-digit issue id (YYYYMMDDEE) + ".xml"
METS_NAME_RE = re.compile(r"^\d{8}\d{2}\.xml$")
# tarball-internal page file (no METS alongside)
TARBALL_OCR_NAME = "ocr.xml"
IMG_COMPOSED_TYPES = {"illustration"}


@dataclass
class Audit:
    """Accumulates distributions across all inspected issues."""

    issues_found: int = 0
    layouts: Counter = field(default_factory=Counter)
    mets_name_patterns: Counter = field(default_factory=Counter)
    structmap_div_types: Counter = field(default_factory=Counter)
    fptr_fileid_prefixes: Counter = field(default_factory=Counter)
    measurement_units: Counter = field(default_factory=Counter)
    page_attributes: Counter = field(default_factory=Counter)
    block_kinds: Counter = field(default_factory=Counter)
    composedblock_types: Counter = field(default_factory=Counter)
    textblock_types: Counter = field(default_factory=Counter)
    mods_languages: Counter = field(default_factory=Counter)
    mods_title_present: Counter = field(default_factory=Counter)
    lccns: Counter = field(default_factory=Counter)
    warnings: list = field(default_factory=list)


def _soup(path: str) -> BeautifulSoup | None:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return BeautifulSoup(f.read(), "xml")
    except OSError:
        return None


def classify_layout(issue_dir: str, files: list[str]) -> tuple[str, str | None]:
    """Return (layout_label, mets_filename_or_None) for an issue directory.

    Layouts:
      - "mets+alto_subdir": METS in dir, page ALTO under alto/
      - "mets+flat":        METS in dir, page ALTO alongside it
      - "tarball_ocr":      ocr.xml present, no METS (tarball internal layout)
      - "no_mets":          page-like XML present but no recognizable METS
    """
    xmls = [f for f in files if f.lower().endswith(".xml")]
    mets = None
    for f in xmls:
        if METS_NAME_RE.match(f):
            mets = f
            break
    if mets is None:
        for f in xmls:
            if "mets" in f.lower():
                mets = f
                break

    has_alto_subdir = os.path.isdir(os.path.join(issue_dir, "alto"))
    if mets:
        return ("mets+alto_subdir" if has_alto_subdir else "mets+flat"), mets
    if TARBALL_OCR_NAME in [f.lower() for f in files]:
        return "tarball_ocr", None
    if xmls:
        return "no_mets", None
    return "empty", None


def name_pattern(fname: str) -> str:
    """Collapse a METS filename to a coarse pattern for tallying."""
    stem = fname[:-4] if fname.lower().endswith(".xml") else fname
    if METS_NAME_RE.match(fname):
        return "YYYYMMDDEE.xml"
    if "mets" in fname.lower():
        return "*mets*.xml"
    if stem.isdigit():
        return f"<{len(stem)} digits>.xml"
    return "other"


def inspect_mets(path: str, audit: Audit) -> None:
    soup = _soup(path)
    if soup is None:
        audit.warnings.append(f"unreadable METS: {path}")
        return

    for div in soup.find_all("div"):
        t = div.get("TYPE")
        if t:
            audit.structmap_div_types[t] += 1
    for fptr in soup.find_all("fptr"):
        fid = fptr.get("FILEID", "")
        m = re.match(r"^[A-Za-z]+", fid)
        if m:
            audit.fptr_fileid_prefixes[m.group(0)] += 1

    lccn = soup.find("identifier", {"type": "lccn"})
    if lccn and lccn.text.strip():
        audit.lccns[lccn.text.strip()] += 1

    # MODS language term(s)
    lang_found = False
    for lt in soup.find_all("languageTerm"):
        if lt.text.strip():
            audit.mods_languages[lt.text.strip().lower()] += 1
            lang_found = True
    if not lang_found:
        audit.mods_languages["<none>"] += 1

    # MODS title (namespace-agnostic: tag local-name == "title")
    title = soup.find(lambda t: getattr(t, "name", None) == "title" and t.text.strip())
    audit.mods_title_present["yes" if title else "no"] += 1


def inspect_alto(path: str, audit: Audit) -> None:
    soup = _soup(path)
    if soup is None:
        audit.warnings.append(f"unreadable ALTO: {path}")
        return

    mu = soup.find("MeasurementUnit")
    audit.measurement_units[mu.text.strip() if mu and mu.text.strip() else "<none>"] += 1

    page = soup.find("Page")
    if page:
        for attr in page.attrs:
            audit.page_attributes[attr] += 1

    ps = soup.find("PrintSpace")
    scope = ps if ps else soup
    n_illus = len(scope.find_all("Illustration"))
    n_graphic = len(scope.find_all("GraphicalElement"))
    audit.block_kinds["Illustration"] += n_illus
    audit.block_kinds["GraphicalElement"] += n_graphic
    for cb in scope.find_all("ComposedBlock"):
        audit.block_kinds["ComposedBlock"] += 1
        audit.composedblock_types[cb.get("TYPE", "<none>")] += 1
    for tb in scope.find_all("TextBlock"):
        audit.block_kinds["TextBlock"] += 1
        audit.textblock_types[tb.get("TYPE", "<none>")] += 1


def find_issue_dirs(input_dir: str) -> list[tuple[str, str, list[str]]]:
    """Yield (issue_dir, layout, files) for every recognizable issue directory."""
    found = []
    for root, _dirs, files in os.walk(input_dir):
        layout, _mets = classify_layout(root, files)
        if layout in ("mets+alto_subdir", "mets+flat", "tarball_ocr"):
            found.append((root, layout, files))
    return found


def run_audit(input_dir: str, alto_sample: int) -> Audit:
    audit = Audit()
    for issue_dir, layout, files in find_issue_dirs(input_dir):
        audit.issues_found += 1
        audit.layouts[layout] += 1

        _layout, mets = classify_layout(issue_dir, files)
        if mets:
            audit.mets_name_patterns[name_pattern(mets)] += 1
            inspect_mets(os.path.join(issue_dir, mets), audit)

        # locate page ALTO files and sample a few (full parse of every page is slow)
        alto_dir = os.path.join(issue_dir, "alto")
        if os.path.isdir(alto_dir):
            alto_files = sorted(
                os.path.join(alto_dir, f) for f in os.listdir(alto_dir) if f.lower().endswith(".xml")
            )
        else:
            alto_files = sorted(
                os.path.join(issue_dir, f)
                for f in files
                if f.lower().endswith(".xml") and (mets is None or f != mets)
            )
        for alto_path in alto_files[: max(alto_sample, 0)]:
            inspect_alto(alto_path, audit)

    return audit


def audit_to_dict(a: Audit) -> dict:
    return {
        "issues_found": a.issues_found,
        "layouts": dict(a.layouts),
        "mets_name_patterns": dict(a.mets_name_patterns),
        "structmap_div_types": dict(a.structmap_div_types),
        "fptr_fileid_prefixes": dict(a.fptr_fileid_prefixes),
        "measurement_units": dict(a.measurement_units),
        "page_attributes": dict(a.page_attributes),
        "block_kinds": dict(a.block_kinds),
        "composedblock_types": dict(a.composedblock_types),
        "textblock_types": dict(a.textblock_types),
        "mods_languages": dict(a.mods_languages),
        "mods_title_present": dict(a.mods_title_present),
        "distinct_lccns": len(a.lccns),
        "lccns": dict(a.lccns),
        "warnings": a.warnings,
    }


def _fmt_counter(c: Counter, limit: int = 20) -> str:
    if not c:
        return "    (none)"
    items = c.most_common(limit)
    lines = [f"    {k!r}: {v}" for k, v in items]
    if len(c) > limit:
        lines.append(f"    … (+{len(c) - limit} more)")
    return "\n".join(lines)


def print_report(a: Audit) -> None:
    print(f"\n=== Chronicling America audit ===")
    print(f"Issues found: {a.issues_found}")
    print(f"Distinct LCCNs (titles): {len(a.lccns)}")
    sections = [
        ("On-disk layouts", a.layouts),
        ("METS filename patterns", a.mets_name_patterns),
        ("structMap div TYPEs", a.structmap_div_types),
        ("fptr FILEID prefixes", a.fptr_fileid_prefixes),
        ("ALTO MeasurementUnit (CRITICAL for coordinate scaling)", a.measurement_units),
        ("ALTO <Page> attributes present", a.page_attributes),
        ("ALTO block kinds (totals)", a.block_kinds),
        ("ComposedBlock TYPE values", a.composedblock_types),
        ("TextBlock TYPE values", a.textblock_types),
        ("MODS language terms", a.mods_languages),
        ("MODS title present", a.mods_title_present),
        ("LCCNs", a.lccns),
    ]
    for title, counter in sections:
        print(f"\n{title}:")
        print(_fmt_counter(counter))
    if a.warnings:
        print(f"\nWarnings ({len(a.warnings)}):")
        for w in a.warnings[:20]:
            print(f"    {w}")
        if len(a.warnings) > 20:
            print(f"    … (+{len(a.warnings) - 20} more)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit Chronicling America downloads (read-only).")
    p.add_argument("--input-dir", required=True, help="Directory to scan recursively.")
    p.add_argument(
        "--alto-sample",
        type=int,
        default=2,
        help="Number of page ALTO files to inspect per issue (default 2; 0 = METS only).",
    )
    p.add_argument("--json", default=None, help="Also write the full report as JSON to this path.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not os.path.isdir(args.input_dir):
        print(f"ERROR: not a directory: {args.input_dir}", file=sys.stderr)
        return 2
    audit = run_audit(args.input_dir, args.alto_sample)
    print_report(audit)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(audit_to_dict(audit), f, indent=2, ensure_ascii=False)
        print(f"\nJSON report written to {args.json}")
    if audit.issues_found == 0:
        print("\nNo issues found.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
