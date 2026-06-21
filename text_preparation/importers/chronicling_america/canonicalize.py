"""Convert downloaded Chronicling America issues into Impresso canonical JSON.

Chronicling America (LOC NDNP) downloads arrive in the server's own ordering
(e.g. ``.../<lccn>/<reel>/<YYYYMMDDEE>/`` for crawls, ``<lccn>/YYYY/MM/DD/ed-N/``
for tarballs, or — as with manually fetched samples — bare ``<day>/ed-1/`` folders).
The Impresso importer, on the other hand, discovers issues under the fixed layout
``<base>/<alias>/<year>/<month>/<day>/<edition>/``.

Rather than reshuffle (potentially many GB of) files on disk, this script locates
every issue *in place* by its METS file, derives the issue's canonical identity
(alias, date, edition) from the METS file name + the LCCN recorded in the METS, and
hands the resulting ``IssueDir`` list straight to the standard importer engine
(:func:`text_preparation.importers.core.import_issues`). That engine writes the
canonical page/issue JSON and validates each record against the canonical JSON
schemas before writing, so the output complies with the schemas by construction.

A Chronicling America issue directory is recognised by a METS file whose name is the
10-digit issue id ``YYYYMMDDEE`` (e.g. ``1896110301.xml``); the per-page ALTO files
live alongside it (typically in an ``alto/`` sub-directory), exactly as the
:class:`ChroniclingAmericaNewspaperIssue` parser expects.

Example::

    SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \\
    python -m text_preparation.importers.chronicling_america.canonicalize \\
        --input-dir "/path/to/downloads" \\
        --output-dir "/path/to/canonical" \\
        --clear --verbose

If the LCCN found in a METS file is not in the known alias registry (the pilot and
download config files), pass ``--alias`` to force a single alias for every discovered
issue, or ``--lccn-alias sn85066387=sanfranciscocall`` (repeatable) to extend the map.
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup

from impresso_essentials.utils import IssueDir
from impresso_essentials.versioning.data_manifest import DataManifest

from text_preparation.importers.core import import_issues
from text_preparation.importers.chronicling_america.classes import (
    ChroniclingAmericaNewspaperIssue,
)
from text_preparation.utils import edition_num_to_code

logger = logging.getLogger(__name__)

PROVIDER = "LOC"
REPO_ROOT = Path(__file__).resolve().parents[3]
# METS file name = 10-digit issue id (YYYYMMDDEE) + ".xml"
METS_NAME_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})(\d{2})\.xml$")
# config files shipping known {lccn: alias} mappings
ALIAS_CONFIG_PATHS = [
    REPO_ROOT
    / "text_preparation/importers/chronicling_america/chronicling_america_pilot_titles.json",
    REPO_ROOT / "text_preparation/config/download_config/chronicling_america_titles.json",
]


def load_lccn_alias_map() -> dict[str, str]:
    """Build a ``{lccn: alias}`` map from the shipped CA title config files."""
    mapping: dict[str, str] = {}
    for cfg_path in ALIAS_CONFIG_PATHS:
        if not cfg_path.exists():
            continue
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Could not read alias config %s: %s", cfg_path, e)
            continue
        for entry in cfg.get("titles", []):
            lccn, alias = entry.get("lccn"), entry.get("alias")
            if lccn and alias:
                mapping[lccn] = alias
    return mapping


def read_mets_meta(mets_path: str) -> tuple[str | None, str | None]:
    """Return ``(lccn, title)`` from a CA METS file.

    The LCCN comes from MODS ``identifier type='lccn'``; the title from a MODS
    ``<title>`` element, falling back to the METS ``LABEL`` (date stripped).
    """
    try:
        with open(mets_path, "r", encoding="utf-8", errors="replace") as f:
            soup = BeautifulSoup(f.read(), "xml")
    except OSError as e:
        logger.warning("Could not read METS %s: %s", mets_path, e)
        return None, None

    lccn = None
    tag = soup.find("identifier", {"type": "lccn"})
    if tag and tag.text.strip():
        lccn = tag.text.strip()

    title = None
    title_tag = soup.find(lambda t: getattr(t, "name", None) == "title" and t.get_text(strip=True))
    if title_tag:
        title = title_tag.get_text(strip=True)
    else:
        mets_tag = soup.find("mets")
        label = mets_tag.get("LABEL") if mets_tag else None
        if label:
            title = re.sub(r",\s*\d{4}(-\d{2}-\d{2})?\s*$", "", label).strip() or None
    return lccn, title


def slugify_alias(title: str | None) -> str | None:
    """Turn a title into a bare alphanumeric alias (must start with a letter)."""
    if not title:
        return None
    slug = re.sub(r"[^a-z0-9]+", "", title.lower())
    return slug if slug and slug[0].isalpha() else None


def find_mets_in_dir(root: str, files: list[str]) -> str | None:
    """Return the METS filename in a directory, or None.

    Prefers the ``YYYYMMDDEE.xml`` issue-id name; falls back to any ``*mets*.xml``.
    """
    xmls = [f for f in files if f.lower().endswith(".xml")]
    for f in xmls:
        if METS_NAME_RE.match(f):
            return f
    for f in xmls:
        if "mets" in f.lower():
            return f
    return None


def date_edition_from_name(fname: str) -> tuple[date | None, int | None]:
    """Parse ``(date, edition)`` from a ``YYYYMMDDEE.xml`` METS filename."""
    m = METS_NAME_RE.match(fname)
    if not m:
        return None, None
    y, mo, d, e = (int(g) for g in m.groups())
    try:
        return date(y, mo, d), e
    except ValueError:
        return None, None


def date_edition_from_path(root: str) -> tuple[date | None, int | None]:
    """Parse ``(date, edition)`` from an issue's directory path.

    Recognizes a 10-digit ``YYYYMMDDEE`` ancestor directory, or a
    ``.../YYYY/MM/DD[/ed-N]`` structure (the tarball/crawl layouts).
    """
    parts = Path(root).parts
    for seg in reversed(parts):
        if re.fullmatch(r"\d{10}", seg):
            try:
                return date(int(seg[:4]), int(seg[4:6]), int(seg[6:8])), int(seg[8:10])
            except ValueError:
                pass
    for i in range(len(parts) - 2):
        if (
            re.fullmatch(r"\d{4}", parts[i])
            and re.fullmatch(r"\d{2}", parts[i + 1])
            and re.fullmatch(r"\d{2}", parts[i + 2])
        ):
            edition = 1
            if i + 3 < len(parts):
                me = re.fullmatch(r"ed-?(\d+)", parts[i + 3])
                if me:
                    edition = int(me.group(1))
            try:
                return date(int(parts[i]), int(parts[i + 1]), int(parts[i + 2])), edition
            except ValueError:
                pass
    return None, None


def batch_version(path: str) -> int:
    """Highest ``_verNN`` found in a path (for dedup), or -1 if none."""
    vers = re.findall(r"_ver(\d+)", path)
    return max(int(v) for v in vers) if vers else -1


def discover_issues(
    input_dir: str,
    lccn_alias_map: dict[str, str],
    forced_alias: str | None = None,
    title_slug: bool = False,
) -> tuple[list[IssueDir], list[str]]:
    """Walk ``input_dir`` and build an ``IssueDir`` for every CA issue found.

    A directory is treated as a Chronicling America issue when it contains a METS
    file carrying an LCCN (this gate keeps non-CA METS/ALTO out). Date and edition
    are taken from the ``YYYYMMDDEE.xml`` filename when present, otherwise derived
    from the directory path (``YYYYMMDDEE`` or ``YYYY/MM/DD/ed-N``). The alias is
    ``forced_alias``, else the configured LCCN mapping, else (with ``title_slug``)
    a slug of the title. When the same issue appears in several batch versions,
    the highest ``_verNN`` is kept.

    Returns:
        tuple[list[IssueDir], list[str]]: deduplicated issues and warnings.
    """
    # canonical_id -> (version, IssueDir, mets_path)
    chosen: dict[str, tuple[int, IssueDir, str]] = {}
    warnings: list[str] = []
    saw_no_mets = 0

    for root, _dirs, files in os.walk(input_dir):
        lowered = {f.lower() for f in files}
        mets = find_mets_in_dir(root, files)
        if mets is None:
            # Tarball-internal layout ships ocr.xml with no METS; the parser needs
            # a METS, so flag these rather than silently ignoring them.
            if "ocr.xml" in lowered:
                saw_no_mets += 1
            continue

        mets_path = os.path.join(root, mets)

        issue_date, edition = date_edition_from_name(mets)
        if issue_date is None:
            issue_date, edition = date_edition_from_path(root)
        if issue_date is None:
            warnings.append(f"{mets_path}: could not determine date; skipped")
            continue

        lccn, title = read_mets_meta(mets_path)
        if lccn is None:
            # No LCCN => not a Chronicling America issue (e.g. another provider).
            warnings.append(f"{mets_path}: no LCCN in METS; not treated as CA; skipped")
            continue

        if forced_alias:
            alias = forced_alias
        else:
            alias = lccn_alias_map.get(lccn)
            if alias is None and title_slug:
                alias = slugify_alias(title)
                if alias:
                    warnings.append(
                        f"{mets_path}: LCCN '{lccn}' unmapped; using slug alias "
                        f"'{alias}' (NOT a registered Impresso medium)"
                    )
            if alias is None:
                warnings.append(
                    f"{mets_path}: LCCN '{lccn}' not in alias map; pass --alias, "
                    f"--lccn-alias {lccn}=<alias>, or --title-slug; skipped"
                )
                continue

        edition_code = edition_num_to_code(edition if edition and edition > 0 else 1)
        canonical_id = f"{alias}-{issue_date.isoformat()}-{edition_code}"
        version = batch_version(mets_path)
        issue = IssueDir(
            provider=PROVIDER,
            alias=alias,
            date=issue_date,
            edition=edition_code,
            path=root,
        )

        prev = chosen.get(canonical_id)
        if prev is None:
            chosen[canonical_id] = (version, issue, mets_path)
        elif version > prev[0]:
            warnings.append(
                f"{canonical_id}: superseding {prev[2]} (ver {prev[0]}) with "
                f"{mets_path} (ver {version})"
            )
            chosen[canonical_id] = (version, issue, mets_path)
        else:
            warnings.append(
                f"{mets_path}: duplicate of {canonical_id} (ver {version} <= "
                f"{prev[0]}); keeping {prev[2]}"
            )

    if saw_no_mets:
        warnings.append(
            f"{saw_no_mets} director(ies) had OCR (ocr.xml) but no METS; the importer "
            f"requires a METS per issue. Use the bulk downloader to fetch issue METS."
        )

    issues = [v[1] for v in chosen.values()]
    issues.sort(key=lambda i: (i.alias, i.date, i.edition))
    return issues, warnings


def build_manifest(out_dir: str, git_repo: str, temp_dir: str) -> DataManifest:
    """Create a local-only canonical DataManifest (no S3 upload, no git push)."""
    return DataManifest(
        data_stage="canonical",
        s3_output_bucket="10-canonical-sandbox",
        s3_input_bucket=None,
        git_repo=git_repo,
        temp_dir=temp_dir,
        is_patch=False,
        patched_fields=None,
        previous_mft_path=None,
        only_counting=False,
        push_to_git=False,
        notes="Manifest from Chronicling America canonicalize.py.",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert downloaded Chronicling America issues to Impresso canonical JSON.",
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory holding the downloaded CA files (searched recursively).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where canonical JSON output is written.",
    )
    parser.add_argument(
        "--alias",
        default=None,
        help="Force this alias for every discovered issue (skips LCCN lookup).",
    )
    parser.add_argument(
        "--lccn-alias",
        action="append",
        default=[],
        metavar="LCCN=ALIAS",
        help="Add an LCCN->alias mapping (repeatable), e.g. sn85066387=sanfranciscocall.",
    )
    parser.add_argument(
        "--title-slug",
        action="store_true",
        help="For unmapped LCCNs, derive an alias from the title (warns; not a "
        "registered Impresso medium).",
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Temporary directory for the manifest (default: <output-dir>/_tmp).",
    )
    parser.add_argument(
        "--git-repo",
        default=str(REPO_ROOT),
        help="Path to the impresso-text-acquisition git repo (for the manifest).",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Remove the output directory before running.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list the issues that would be converted, then exit.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    lccn_alias_map = load_lccn_alias_map()
    for pair in args.lccn_alias:
        if "=" not in pair:
            logger.error("Invalid --lccn-alias '%s' (expected LCCN=ALIAS).", pair)
            return 2
        lccn, alias = pair.split("=", 1)
        lccn_alias_map[lccn.strip()] = alias.strip()

    issues, warnings = discover_issues(
        args.input_dir, lccn_alias_map, args.alias, title_slug=args.title_slug
    )

    for w in warnings:
        logger.warning(w)

    if not issues:
        logger.error("No Chronicling America issues found under %s.", args.input_dir)
        return 1

    logger.info("Discovered %d issue(s):", len(issues))
    for i in issues:
        logger.info("  %s %s ed-%s  <-  %s", i.alias, i.date.isoformat(), i.edition, i.path)

    if args.dry_run:
        return 0

    out_dir = args.output_dir
    if args.clear and os.path.exists(out_dir):
        import shutil

        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    temp_dir = args.temp_dir or os.path.join(out_dir, "_tmp")
    os.makedirs(temp_dir, exist_ok=True)

    manifest = build_manifest(out_dir, args.git_repo, temp_dir)

    # read_manifest_from_s3 is patched out: we run fully local, with no prior manifest.
    with patch(
        "impresso_essentials.versioning.data_manifest.read_manifest_from_s3",
        return_value=(None, None),
    ):
        import_issues(
            issues,
            out_dir,
            s3_bucket=None,
            issue_class=ChroniclingAmericaNewspaperIssue,
            image_dirs=None,
            temp_dir=None,
            chunk_size=None,
            manifest=manifest,
            provider=PROVIDER,
        )

    logger.info("Done. Canonical JSON written under %s/%s/", out_dir, PROVIDER)
    return 0


if __name__ == "__main__":
    sys.exit(main())
