"""Prepare collection media for the Impresso server.

Copies, converts, renames, and reorganizes collection media into the Impresso
directory structure. Handles two media types, auto-detected per issue from the
``ext`` field:

- **images** (tif/jp2/pdf/png/jpg): converted to lossless JP2, named
  ``{issue_id}-p{NNNN}.jp2``.
- **audio** (mp3, …): mp3 copied unchanged (other formats reserved for future
  transcoding), named ``{issue_id}-r{NNNN}.mp3``.

Usage:
    impresso-structure-media --config config.yaml
    impresso-structure-media --config config.yaml --dry_run
"""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from functools import partial
from pathlib import Path
from time import gmtime, strftime

import fire
import yaml
from mutagen.mp3 import MP3
from PIL import Image
from tqdm import tqdm

from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

IMAGE_EXTENSIONS = {".jp2", ".tif", ".tiff", ".png", ".jpg", ".jpeg"}
PDF_EXTENSIONS = {".pdf"}
AUDIO_EXTENSIONS = {".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac"}
ALL_SOURCE_EXTENSIONS = IMAGE_EXTENSIONS | PDF_EXTENSIONS | AUDIO_EXTENSIONS
RENAMING_INFO_FILENAME = "renaming_info.json"

# Audio IIIF Presentation 3 manifest emission. Defined locally rather than
# imported from text_preparation.importers.ina.classes.IIIF_ENDPOINT_URI to
# avoid pulling the INA importer's bs4/audio surface into this preprocessing
# script.
IIIF_AUDIO_BASE_URL = "https://impresso-project.ch/media/audio/"
MANIFEST_FILENAME = "manifest.json"

# Minimum plausible size for a real newspaper-page JP2. opj_compress occasionally
# exits 0 after writing only the JP2 box skeleton (~77 bytes, no codestream);
# anything well below a real page must be treated as a stub.
MIN_VALID_JP2_BYTES = 5_000

# Default opj_compress -r value. 1 = mathematically lossless; higher = smaller +
# lossier. 10 is the impresso standard (~1-2pp reOCR loss, zoom-grade quality).
DEFAULT_JP2_COMPRESSION_RATIO = 10


@dataclass
class Config:
    """Script configuration, loaded from a YAML file with optional CLI overrides."""

    # --- paths ---
    issues_json_path: str = ""
    source_base_dir: str = "/mnt/project_impresso/original"
    target_base_dir: str = "/mnt/impresso_images"

    # --- filtering ---
    aliases_include: list[str] = field(default_factory=list)
    aliases_exclude: list[str] = field(default_factory=list)

    # --- behaviour ---
    dry_run: bool = True
    sample: int = 0  # 0 = disabled; >0 = process first N issues end-to-end
    delete_source: bool = False
    overwrite: bool = False  # unit-level: force-reconvert a unit even if a valid target exists (orthogonal to resume)

    # --- image conversion ---
    # opj_compress -r value. 1 = lossless; 10 = impresso standard (smaller, ~1-2pp
    # reOCR loss). Already-compressed sources may yield a JP2 larger than the source.
    compression_ratio: int = DEFAULT_JP2_COMPRESSION_RATIO

    # --- performance ---
    workers: int = 1  # 1 = sequential; >1 = parallel page conversion

    # --- audio manifest ---
    # Required when any audio issue is in scope (validated at startup, not in
    # __post_init__, so pure-image runs need not set it).
    access_rights_path: str = ""

    # --- output / logging ---
    log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_file: str = ""
    report_dir: str = ""
    prior_report_dir: str = ""
    retry_failed_only: bool = False

    def __post_init__(self):
        if not self.issues_json_path:
            raise ValueError("issues_json_path is required in config")
        if not self.target_base_dir:
            raise ValueError("target_base_dir is required in config")
        if self.workers < 1:
            raise ValueError(f"workers must be >= 1, got {self.workers}")
        if self.compression_ratio < 1:
            raise ValueError(
                f"compression_ratio must be >= 1, got {self.compression_ratio}"
            )
        if self.sample < 0:
            raise ValueError(f"sample must be >= 0, got {self.sample}")
        valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        if self.log_level.upper() not in valid_levels:
            raise ValueError(
                f"log_level must be one of {valid_levels}, got '{self.log_level}'"
            )

    def log_summary(self):
        """Log the resolved configuration."""
        logger.info("Resolved configuration:")
        for k, v in asdict(self).items():
            logger.info("  %s: %s", k, v)


def load_config(config_path: str, **overrides) -> Config:
    """Load a YAML config file and apply CLI overrides.

    Any key passed as a CLI flag (e.g. --dry_run) takes
    precedence over the value in the config file.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # CLI overrides take precedence (drop None values — fire passes them for unset flags)
    for key, value in overrides.items():
        if value is not None:
            raw[key] = value

    return Config(**raw)


# ---------------------------------------------------------------------------
# Issue records
# ---------------------------------------------------------------------------


@dataclass
class IssueRecord:
    """A single newspaper issue to process, parsed from the input JSON."""

    alias: str
    date: date
    edition: str
    local_path: list[str]  # one or more paths relative to source_base_dir
    imgs_subdir: str  # subfolder within each local_path ("" = images directly in local_path)
    ext: str  # e.g. ".tif", ".jp2", ".pdf"

    @property
    def issue_id(self) -> str:
        """Canonical Impresso issue ID: {alias}-{YYYY}-{MM}-{DD}-{edition}."""
        return f"{self.alias}-{self.date:%Y-%m-%d}-{self.edition}"

    @property
    def media_type(self) -> str:
        """Media kind inferred from ``ext``: ``"audio"`` or ``"image"``.

        Audio extensions (``.mp3``, ``.wav``, …) route to the audio pipeline
        (record numbering, copy/transcode, duration metadata). Everything else
        (image + PDF extensions) routes to the image pipeline. Derived from the
        existing ``ext`` field — no separate config knob.
        """
        return "audio" if self.ext.lower() in AUDIO_EXTENSIONS else "image"


def load_issues_index(json_path: str) -> dict:
    """Load the raw hierarchical issues JSON.

    Returns the dict exactly as parsed (alias > year > month > [entries]),
    for in-place augmentation alongside the flattened ``load_issues()`` output.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_access_rights(path: str) -> dict:
    """Load the provider-specific access-rights JSON once for the run.

    Schema: ``rights[alias][year_range_label]`` -> entry with ``start_year``,
    ``end_year``, ``content_bitmaps: {explore, get_tr, get_img}``, and other
    metadata. Same alias can carry multiple year-range entries with different
    bitmaps; lookup is by ``(alias, year)`` via ``find_access_rights_entry``.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_access_rights_entry(rights: dict, alias: str, year: int) -> dict:
    """Look up the rights entry for ``(alias, year)``. Raises ``KeyError`` if absent.

    Walks ``rights[alias][<year-range>]`` and returns the entry whose
    ``start_year <= year <= end_year``. The year-range label string
    (``"1915-1915"``, ``"1938-1946"``, ...) is purely descriptive — the int
    fields ``start_year`` / ``end_year`` are the source of truth. Different
    year ranges for the same alias can carry different ``content_bitmaps``,
    so bitmaps surfaced into the manifest are time-period-dependent.

    Example (``RActuFR`` has both ``"1915-1915"`` and ``"1938-1946"``):
        find_access_rights_entry(rights, "RActuFR", 1942)
            -> returns the 1938-1946 entry (and its bitmaps).
        find_access_rights_entry(rights, "RActuFR", 1925)
            -> raises KeyError (no range covers 1925).

    Callers pass ``issue.date.year`` — same value for every record of a given
    issue, so a single lookup per issue feeds all canvases of its manifest.
    """
    alias_rights = rights.get(alias)
    if alias_rights is None:
        raise KeyError(f"no access-rights entry for alias {alias!r}")
    for _range_label, entry in alias_rights.items():
        if entry["start_year"] <= year <= entry["end_year"]:
            return entry
    raise KeyError(
        f"no access-rights year-range covers {alias} {year} "
        f"(have: {sorted(alias_rights.keys())})"
    )


def infer_provider(issues_json_path: str) -> str:
    """Extract ``{provider}`` from ``issues_to_ingest.{provider}.json``.

    Also works for ``issue_index.{provider}.json``. Falls back to the full stem
    if the filename doesn't contain a dot.
    """
    stem = Path(issues_json_path).stem
    return stem.rsplit(".", 1)[-1] if "." in stem else stem


def load_issues(
    json_path: str,
    aliases_include: list[str],
    aliases_exclude: list[str],
) -> list[IssueRecord]:
    """Parse the hierarchical issues JSON and return a flat list of IssueRecords.

    The JSON schema is: alias > year > month > [{day, edition, local_path, ...}].
    Each entry must include an ``ext`` field. ``local_path`` is a list of one or
    more directory paths relative to ``source_base_dir``.

    Args:
        json_path: Path to the issues_to_ingest.{provider}.json file.
        aliases_include: Only process these aliases (empty = all).
        aliases_exclude: Skip these aliases (applied after include).

    Returns:
        Flat list of IssueRecord objects.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Pre-compute include/exclude sets for O(1) lookups
    include_set = set(aliases_include) if aliases_include else None
    exclude_set = set(aliases_exclude) if aliases_exclude else set()

    issues: list[IssueRecord] = []

    for alias, years in data.items():
        # --- alias filtering ---
        if include_set is not None and alias not in include_set:
            continue
        if alias in exclude_set:
            continue

        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:
                    try:
                        local_path = entry["local_path"]
                        if not isinstance(local_path, list) or not local_path:
                            raise TypeError(
                                f"local_path must be a non-empty list, got {type(local_path).__name__}"
                            )
                        issues.append(
                            IssueRecord(
                                alias=alias,
                                date=date(int(year), int(month), int(entry["day"])),
                                edition=entry["edition"],
                                local_path=local_path,
                                imgs_subdir=entry.get("imgs_subdir", ""),
                                # Normalize to lowercase-with-leading-dot so the
                                # index's spelling doesn't matter: RTS encodes
                                # ".mp3", INA encodes "MP3". Every downstream
                                # comparison (media_type routing, suffix checks)
                                # assumes this canonical form.
                                ext="." + entry["ext"].lstrip(".").lower(),
                            )
                        )
                    except (KeyError, ValueError, TypeError) as e:
                        logger.error(
                            "Skipping malformed entry: alias=%s year=%s month=%s entry=%s — %s",
                            alias, year, month, entry, e,
                        )

    return issues


# ---------------------------------------------------------------------------
# Page file discovery
# ---------------------------------------------------------------------------

_TRAILING_DIGITS_RE = re.compile(r"(\d+)$")


@dataclass
class PageFile:
    """A single page image file discovered within an issue directory."""

    source_path: Path
    page_num: int
    source_format: str  # without dot: "tif", "jp2" — matches convert_to_jp2() convention

    @property
    def num(self) -> int:
        """Unit number, uniform accessor shared with AudioRecord (the main loop
        treats pages and audio records interchangeably via ``.num``)."""
        return self.page_num


@dataclass
class AudioRecord:
    """A single audio record file referenced by an issue.

    Audio differs from images: each ``local_path`` entry is the audio file
    itself (not a directory), and record numbers are assigned by the
    (already-sorted) order of ``local_path``, not parsed from the filename.
    """

    source_path: Path
    record_num: int
    source_format: str  # without dot: "mp3", "wav" — matches convert_audio() convention

    @property
    def num(self) -> int:
        """Unit number, uniform accessor shared with PageFile."""
        return self.record_num


def discover_pages(issue: IssueRecord, source_base_dir: str) -> list[PageFile]:
    """Discover page image files in an issue's directories and extract page numbers.

    Walks every directory in ``issue.local_path`` (each optionally suffixed with
    ``issue.imgs_subdir``), filters files by ``issue.ext``, and combines the
    results into a single page sequence. Page numbers are extracted from the
    trailing digits in each filename stem (e.g. ``00000001.tif`` → page 1,
    ``0002384_18420515_0001.jp2`` → page 1).

    Args:
        issue: The issue record describing where to find files.
        source_base_dir: Root of the source data tree.

    Returns:
        List of PageFile objects sorted by page number.

    Raises:
        FileNotFoundError: If any image directory doesn't exist, or no valid
            page files are found across all directories.
        NotImplementedError: If ``ext`` is a PDF extension (TODO).
        ValueError: If duplicate page numbers are detected (e.g. two source
            directories each contain a ``00000001.tif``).
    """
    # --- PDF: TODO stub ---
    if issue.ext.lower() in PDF_EXTENSIONS:
        raise NotImplementedError(
            f"PDF page discovery not yet implemented for {issue.issue_id}"
        )

    target_ext = issue.ext.lower()

    # --- collect matching files across every local_path ---
    matching_files: list[Path] = []
    missing_dirs: list[Path] = []

    for local_path in issue.local_path:
        img_dir = Path(source_base_dir) / local_path.lstrip("/")
        if issue.imgs_subdir:
            img_dir = img_dir / issue.imgs_subdir

        if not img_dir.is_dir():
            missing_dirs.append(img_dir)
            continue

        matching_files.extend(
            f for f in img_dir.iterdir()
            if f.is_file() and f.suffix.lower() == target_ext
        )

    if missing_dirs:
        raise FileNotFoundError(
            f"Image director{'y' if len(missing_dirs) == 1 else 'ies'} "
            f"missing for {issue.issue_id}: {[str(d) for d in missing_dirs]}"
        )

    matching_files.sort()

    if not matching_files:
        raise FileNotFoundError(
            f"No files with extension '{target_ext}' found for "
            f"{issue.issue_id} under {issue.local_path}"
        )

    # --- extract page numbers from trailing digits in stem ---
    pages: list[PageFile] = []
    skipped: list[str] = []

    for fpath in matching_files:
        m = _TRAILING_DIGITS_RE.search(fpath.stem)
        if m is None:
            skipped.append(fpath.name)
            continue
        pages.append(PageFile(
            source_path=fpath,
            page_num=int(m.group(1)),
            source_format=fpath.suffix.lstrip(".").lower(),
        ))

    if skipped:
        logger.warning(
            "%s: %d file(s) skipped — no trailing digits in stem: %s",
            issue.issue_id,
            len(skipped),
            skipped[:5],
        )

    if not pages:
        raise FileNotFoundError(
            f"No valid page files found in {img_dir} for {issue.issue_id} "
            f"(all {len(matching_files)} file(s) lacked trailing digits in stem)"
        )

    # --- check for duplicate page numbers ---
    seen: dict[int, Path] = {}
    duplicates: list[str] = []
    for pf in pages:
        if pf.page_num in seen:
            duplicates.append(
                f"page {pf.page_num}: {seen[pf.page_num].name} and {pf.source_path.name}"
            )
        else:
            seen[pf.page_num] = pf.source_path

    if duplicates:
        raise ValueError(
            f"Duplicate page numbers in {issue.issue_id}: {'; '.join(duplicates)}"
        )

    # --- sort by page number ---
    pages.sort(key=lambda pf: pf.page_num)

    # --- warn on page numbers exceeding 4-digit padding ---
    oversized = [pf for pf in pages if pf.page_num > 9999]
    if oversized:
        logger.warning(
            "%s: %d page(s) exceed 4-digit limit (max p9999) — "
            "filenames will violate Impresso naming convention: %s",
            issue.issue_id,
            len(oversized),
            [f"p{pf.page_num}" for pf in oversized[:5]],
        )

    logger.debug(
        "%s: discovered %d pages (p%d–p%d) across %d director%s",
        issue.issue_id,
        len(pages),
        pages[0].page_num,
        pages[-1].page_num,
        len(issue.local_path),
        "y" if len(issue.local_path) == 1 else "ies",
    )

    return pages


def discover_audio_records(
    issue: IssueRecord, source_base_dir: str
) -> list[AudioRecord]:
    """Resolve an audio issue's record files from its ``local_path`` list.

    Unlike ``discover_pages``, each ``local_path`` entry is the audio FILE
    itself (not a directory), so there is no globbing, no ``imgs_subdir`` join,
    and no trailing-digit parsing. Record numbers are assigned 1-based by the
    order of ``local_path`` — the list is already sorted upstream, so entry 0
    becomes ``r0001``, entry 1 ``r0002``, etc.

    Args:
        issue: The audio issue record describing where the files are.
        source_base_dir: Root of the source data tree.

    Returns:
        List of AudioRecord objects, in ``local_path`` order.

    Raises:
        FileNotFoundError: If any referenced audio file is missing.
    """
    records: list[AudioRecord] = []
    missing: list[str] = []

    for idx, local_path in enumerate(issue.local_path, start=1):
        fpath = Path(source_base_dir) / local_path.lstrip("/")
        if not fpath.is_file():
            missing.append(str(fpath))
            continue
        if fpath.suffix.lower() != issue.ext.lower():
            logger.warning(
                "%s record %d: file extension %s differs from declared ext %s (%s)",
                issue.issue_id, idx, fpath.suffix, issue.ext, fpath.name,
            )
        records.append(AudioRecord(
            source_path=fpath,
            record_num=idx,
            source_format=fpath.suffix.lstrip(".").lower(),
        ))

    if missing:
        raise FileNotFoundError(
            f"Audio file(s) missing for {issue.issue_id}: {missing}"
        )

    # local_path was non-empty (enforced by load_issues), so records is non-empty.
    if len(records) > 1:
        # CanonicalAudioRecord warns when a record number != 1; surface that the
        # multi-file issue will produce r0002+ which downstream may flag.
        logger.warning(
            "%s: %d audio records — record numbers r0002+ assigned; "
            "CanonicalAudioRecord warns on records numbered other than 1.",
            issue.issue_id, len(records),
        )

    oversized = [r for r in records if r.record_num > 9999]
    if oversized:
        logger.warning(
            "%s: %d record(s) exceed 4-digit limit (max r9999) — "
            "filenames will violate Impresso naming convention: %s",
            issue.issue_id,
            len(oversized),
            [f"r{r.record_num}" for r in oversized[:5]],
        )

    logger.debug(
        "%s: discovered %d audio record(s) (r%04d–r%04d)",
        issue.issue_id,
        len(records),
        records[0].record_num,
        records[-1].record_num,
    )

    return records


# ---------------------------------------------------------------------------
# Renaming & target path construction
# ---------------------------------------------------------------------------


def build_target_path(
    target_base_dir: str,
    issue: IssueRecord,
    num: int,
    *,
    unit_letter: str | None = None,
    ext: str | None = None,
) -> Path:
    """Build the target media path following Impresso conventions.

    Directory:  {target_base_dir}/{alias}/{YYYY}/{MM}/{DD}/{edition}/
    Filename:   {issue_id}-{unit_letter}{num:04d}{ext}

    For images the filename is ``{issue_id}-p{num:04d}.jp2``; for audio it is
    ``{issue_id}-r{num:04d}.mp3`` (matching the canonical audio record ID).
    When ``unit_letter`` / ``ext`` are not given they are derived from
    ``issue.media_type``, so existing image call sites keep working unchanged.

    Args:
        target_base_dir: Root of the writable output tree.
        issue: The issue being processed.
        num: Page number (images) or record number (audio). Zero-padded to 4
            digits.
        unit_letter: ``"p"`` for pages, ``"r"`` for records. Defaults by media
            type when ``None``.
        ext: Target extension including the dot (e.g. ``".jp2"``, ``".mp3"``).
            Defaults by media type when ``None`` — images become ``.jp2``,
            audio keeps its source extension.

    Returns:
        Full target path as a Path object.
    """
    is_audio = issue.media_type == "audio"
    if unit_letter is None:
        unit_letter = "r" if is_audio else "p"
    if ext is None:
        ext = f".{issue.ext.lstrip('.')}" if is_audio else ".jp2"

    target_dir = Path(target_base_dir) / issue.alias / f"{issue.date:%Y/%m/%d}" / issue.edition
    filename = f"{issue.issue_id}-{unit_letter}{num:04d}{ext}"
    return target_dir / filename


def write_renaming_info(
    issue: IssueRecord,
    pages: list[PageFile],
    page_results: dict[int, dict],
    target_base_dir: str,
    source_base_dir: str,
    dry_run: bool = False,
) -> dict[str, dict]:
    """Write per-issue metadata JSON documenting the copy/conversion process.

    Assembles a dict mapping page number (as string) to per-page metadata,
    then writes it as ``renaming_info.json`` in the issue's target directory.

    Args:
        issue: The issue record being processed.
        pages: Page files discovered by ``discover_pages()``.
        page_results: Mapping of page_num -> ``convert_to_jp2()`` return dict
            (must contain ``width`` and ``height`` keys).
        target_base_dir: Root of the writable image output tree.
        source_base_dir: Root of the source data tree.
        dry_run: If True, build and log the dict but do not write to disk.

    Returns:
        The assembled info dict (string keys -> per-page metadata).

    Raises:
        OSError: If writing the JSON file fails (logged before raising).
    """
    info_dict: dict[str, dict] = {}

    target_dir = build_target_path(target_base_dir, issue, pages[0].page_num).parent
    rel_img_dir = target_dir.relative_to(target_base_dir)
    rel_ocr_dirs = [p.lstrip("/") for p in issue.local_path]

    for page in pages:
        filename = f"{issue.issue_id}-p{page.page_num:04d}.jp2"
        result = page_results[page.page_num]

        info_dict[str(page.page_num)] = {
            "original_filename": page.source_path.name,
            "new_filename": filename,
            "issue_id": issue.issue_id,
            "img_dir_path": str(rel_img_dir),
            "ocr_dir_path": rel_ocr_dirs,
            "width": result["width"],
            "height": result["height"],
        }

    logger.debug(
        "%s: renaming_info with %d pages assembled",
        issue.issue_id,
        len(info_dict),
    )

    if dry_run:
        logger.info(
            "[DRY RUN] Would write %s for %s (%d pages)",
            RENAMING_INFO_FILENAME,
            issue.issue_id,
            len(info_dict),
        )
        return info_dict

    info_filepath = target_dir / RENAMING_INFO_FILENAME

    try:
        with open(info_filepath, "w", encoding="utf-8") as fout:
            json.dump(info_dict, fout)
        logger.info(
            "%s: wrote %s (%d pages)",
            issue.issue_id,
            info_filepath,
            len(info_dict),
        )
    except OSError:
        logger.error(
            "%s: failed to write %s",
            issue.issue_id,
            info_filepath,
            exc_info=True,
        )
        raise

    return info_dict


def write_renaming_info_audio(
    issue: IssueRecord,
    records: list[AudioRecord],
    record_results: dict[int, dict],
    target_base_dir: str,
    source_base_dir: str,
    dry_run: bool = False,
) -> dict[str, dict]:
    """Write per-issue ``renaming_info.json`` for an audio issue.

    Audio analogue of ``write_renaming_info``: keyed by record number, with
    ``duration`` (HH:MM:SS) in place of ``width``/``height``, ``record_dir_path``
    in place of ``img_dir_path``, and ``src_path`` (the relative ``local_path``
    file list) for traceability.

    Args:
        issue: The audio issue being processed.
        records: Records discovered by ``discover_audio_records()``.
        record_results: Mapping of record_num -> ``convert_audio()`` return dict
            (must contain a ``duration`` key).
        target_base_dir: Root of the writable output tree.
        source_base_dir: Root of the source data tree (unused; kept for a
            uniform signature with ``write_renaming_info``).
        dry_run: If True, build and log the dict but do not write to disk.

    Returns:
        The assembled info dict (string keys -> per-record metadata).

    Raises:
        OSError: If writing the JSON file fails (logged before raising).
    """
    info_dict: dict[str, dict] = {}

    target_dir = build_target_path(target_base_dir, issue, records[0].record_num).parent
    rel_record_dir = target_dir.relative_to(target_base_dir)
    rel_src_paths = [p.lstrip("/") for p in issue.local_path]

    for record in records:
        filename = build_target_path(target_base_dir, issue, record.record_num).name
        result = record_results[record.record_num]

        info_dict[str(record.record_num)] = {
            "original_filename": record.source_path.name,
            "new_filename": filename,
            "issue_id": issue.issue_id,
            "record_dir_path": str(rel_record_dir),
            "src_path": rel_src_paths,
            "duration": result["duration"],
        }

    logger.debug(
        "%s: audio renaming_info with %d record(s) assembled",
        issue.issue_id,
        len(info_dict),
    )

    if dry_run:
        logger.info(
            "[DRY RUN] Would write %s for %s (%d record(s))",
            RENAMING_INFO_FILENAME,
            issue.issue_id,
            len(info_dict),
        )
        return info_dict

    info_filepath = target_dir / RENAMING_INFO_FILENAME

    try:
        with open(info_filepath, "w", encoding="utf-8") as fout:
            json.dump(info_dict, fout)
        logger.info(
            "%s: wrote %s (%d record(s))",
            issue.issue_id,
            info_filepath,
            len(info_dict),
        )
    except OSError:
        logger.error(
            "%s: failed to write %s",
            issue.issue_id,
            info_filepath,
            exc_info=True,
        )
        raise

    return info_dict


# ---------------------------------------------------------------------------
# Audio IIIF Presentation 3 manifest
# ---------------------------------------------------------------------------


def write_audio_manifest(
    issue: IssueRecord,
    records: list[AudioRecord],
    record_results: dict[int, dict],
    url_provider: str,
    rights_entry: dict,
    target_base_dir: str,
    dry_run: bool = False,
) -> Path:
    """Write the IIIF Presentation 3 ``manifest.json`` beside the audio file(s).

    One manifest per issue, **one Canvas per issue** regardless of record
    count: an N-record issue is presented as a single continuous broadcast
    program whose Canvas timeline is the concatenation of all records, with
    one painting Annotation per record placed on its own time slice via
    ``target = {canvas_id}#t=start,end``. This matches IIIF Cookbook Recipe
    0064 (https://iiif.io/api/cookbook/recipe/0064-opera-one-canvas/) for
    the multi-record case and Recipe 0002
    (https://iiif.io/api/cookbook/recipe/0002-mvm-audio/) for the
    single-record case.

    Metadata locality (consistent across both cases):

    - ``issue_id``, ``ci_id``, and the three rights bitmaps →
      ``Manifest.metadata`` (issue-scope). These are the issue's own
      facts; since the issue maps 1:1 to the single Canvas, they are
      equivalently the Canvas's facts, but we surface them at the Manifest
      top level so consumers find them without descending into ``items``.
      Includes ``ci_id`` because every audio issue has exactly one content
      item (even when split across N audio files), mirroring the Swissinfo
      convention of one CI spanning multiple facsimile pages.
    - ``audio_id`` → ``Annotation.metadata`` (record-scope, one per mp3).
      This is the only field that genuinely varies per record, so it stays
      on its Annotation rather than moving to the Manifest top level.
    - ``Canvas.metadata`` is omitted — with one Canvas per issue, the
      issue-scope facts sit on the Manifest instead.

    Multi-record manifests additionally carry a ``structures`` array (one
    outer Range labelled with the ``issue_id``, with one child Range per
    record labelled with the record's ``audio_id`` and referencing the
    matching ``{canvas_id}#t=start,end`` fragment) for TOC navigation.
    Single-record manifests omit ``structures`` — there is only one
    segment, so a TOC is degenerate.

    URL conventions:

    - Canvas: ``{base}/canvas`` (always; no record-number suffix).
    - AnnotationPage: ``{base}/canvas/page``.
    - Annotation: ``{base}/canvas/page/annotation`` (single-record, matches
      the original Impresso reference template byte-for-byte) or
      ``{base}/canvas/page/annotation/{N}`` (multi-record).
    - Range: ``{base}/range/issue`` (outer) and
      ``{base}/range/record/{N}`` (child).

    ID derivation: ``audio_id = {issue_id}-r{N:04d}`` for record N, and
    ``ci_id = {issue_id}-i0001`` always (issue-level, one CI per issue
    regardless of record count). The input JSON carries no per-record
    ci_id list, and none is needed under the current one-CI-per-issue
    convention. If/when an issue is allowed to carry multiple CIs, the
    input data model would need to provide them explicitly and the
    derivation would need to move from a constant to a per-record lookup.

    Args:
        issue: Audio issue being processed.
        records: Records discovered by ``discover_audio_records()``.
        record_results: Mapping ``record_num -> convert_audio() result dict``;
            each result must include ``duration_seconds`` (float).
        url_provider: URL provider segment, e.g. ``"INA"`` or ``"RTS"``
            (typically ``infer_provider(issues_json_path).upper()``).
        rights_entry: Single access-rights entry (one of the year-range
            entries from the file) — supplies ``content_bitmaps``.
        target_base_dir: Root of the writable output tree for **this run's
            data**, i.e. the kind root joined with the provider segment
            (e.g. ``/mnt/project_impresso_rw/audios/INA``). ``main()``
            derives it from the YAML's ``target_base_dir`` (the bare kind
            root, e.g. ``/mnt/project_impresso_rw/audios``) plus the URL
            provider, so the manifest physical path mirrors the IIIF URL
            path. The augmented issue_index, written outside this
            function, still uses the YAML's kind root directly.
        dry_run: If True, log the would-be write and return the target path
            without touching disk.

    Returns:
        Path to the written (or would-be-written) ``manifest.json``.

    Raises:
        OSError: If writing the JSON file fails (logged before re-raise).
    """
    base = (
        f"{IIIF_AUDIO_BASE_URL}{url_provider}/{issue.alias}"
        f"/{issue.date:%Y/%m/%d}/{issue.edition}"
    )
    multi = len(records) > 1
    bitmaps = rights_entry["content_bitmaps"]
    canvas_id = f"{base}/canvas"
    page_id = f"{canvas_id}/page"

    # Per-record durations, rounded to 2 decimals so the cumulative offsets
    # we emit in `#t=start,end` are arithmetically consistent with each
    # body's own `duration` value (no drift between target endpoints and
    # body lengths). Build the (start, end) chain in the same loop.
    record_durations: list[float] = []
    record_ranges: list[tuple[float, float]] = []
    cursor = 0.0
    for record in records:
        dur = round(record_results[record.record_num]["duration_seconds"], 2)
        record_durations.append(dur)
        start = round(cursor, 2)
        end = round(cursor + dur, 2)
        record_ranges.append((start, end))
        cursor = end
    total_duration = round(cursor, 2)

    def _fmt_t(x: float) -> str:
        """W3C Media Fragments NPT formatting — integers render without
        ``.0``, fractional values keep their decimals. Matches the IIIF
        Cookbook 0064 style (`#t=0,3971.24`)."""
        return str(int(x)) if x == int(x) else str(x)

    # Label: keep the reference template's "-r0001" suffix in the single-
    # record case; multi-record uses the bare issue_id (the manifest then
    # represents the whole program, so the suffix would be misleading).
    if multi:
        label_id = issue.issue_id
    else:
        label_id = f"{issue.issue_id}-r{records[0].record_num:04d}"
    label = (
        f"Radio Broadcast Audio Record of {issue.alias} on "
        f"{issue.date:%d/%m/%Y} with canonical ID {label_id}"
    )

    # --- painting annotations (one per record) -----------------------------
    # `audio_id` is the only truly per-record field (one mp3 file ↔ one
    # audio_id). `ci_id` is issue-scope (one content item per issue, even
    # when split into N audio files for storage) and lives on the Canvas.
    annotations: list[dict] = []
    for idx, record in enumerate(records):
        dur = record_durations[idx]
        start, end = record_ranges[idx]
        audio_id = f"{issue.issue_id}-r{record.record_num:04d}"

        # Single-record: bare canvas target + unsuffixed annotation URL
        # (matches the Impresso reference template). Multi-record: time-
        # fragmented target + record-numbered annotation URL.
        if multi:
            annotation_id = f"{page_id}/annotation/{record.record_num}"
            target = f"{canvas_id}#t={_fmt_t(start)},{_fmt_t(end)}"
        else:
            annotation_id = f"{page_id}/annotation"
            target = canvas_id

        annotations.append({
            "id": annotation_id,
            "type": "Annotation",
            "motivation": "painting",
            "metadata": [
                {"label": {"en": ["audio_id"]}, "value": {"en": [audio_id]}},
            ],
            "body": {
                "id": f"{base}/{audio_id}.mp3",
                "type": "Sound",
                "format": "audio/mp3",
                "duration": dur,
            },
            "target": target,
        })

    # --- the single Canvas ------------------------------------------------
    # The Canvas is the issue's timeline (one per Manifest in our schema)
    # and carries no metadata of its own — issue-level facts live on the
    # Manifest top level below. `ci_id` is derived here for that block:
    # every audio issue has exactly one content item, even when split into
    # N audio files; the Impresso convention mirrors how Swissinfo radio
    # bulletins carry one CI across multiple facsimile pages. If a future
    # provider genuinely has N CIs per issue, the input data model would
    # need to carry per-record ci_ids and this derivation would change.
    ci_id = f"{issue.issue_id}-i0001"
    canvas = {
        "id": canvas_id,
        "type": "Canvas",
        "duration": total_duration,
        "items": [{
            "id": page_id,
            "type": "AnnotationPage",
            "items": annotations,
        }],
    }

    # --- manifest skeleton ------------------------------------------------
    # Issue-scope facts live at the Manifest top level. The issue *is* the
    # single Canvas in our schema (always 1:1), so these are equivalently
    # the Canvas's facts; we surface them on the Manifest rather than the
    # nested Canvas so consumers find them without descending into `items`.
    manifest: dict = {
        "@context": "http://iiif.io/api/presentation/3/context.json",
        "id": f"{base}/manifest.json",
        "type": "Manifest",
        "label": {"en": [label]},
        "metadata": [
            {"label": {"en": ["issue_id"]},
             "value": {"en": [issue.issue_id]}},
            {"label": {"en": ["ci_id"]},
             "value": {"en": [ci_id]}},
            {"label": {"en": ["explore_bitmap"]},
             "value": {"en": [bitmaps["explore"]]}},
            {"label": {"en": ["get_tr_bitmap"]},
             "value": {"en": [bitmaps["get_tr"]]}},
            {"label": {"en": ["get_img_bitmap"]},
             "value": {"en": [bitmaps["get_img"]]}},
        ],
        "items": [canvas],
    }

    # --- structures (TOC) — multi-record only -----------------------------
    if multi:
        child_ranges = []
        for idx, record in enumerate(records):
            start, end = record_ranges[idx]
            audio_id = f"{issue.issue_id}-r{record.record_num:04d}"
            child_ranges.append({
                "type": "Range",
                "id": f"{base}/range/record/{record.record_num}",
                "label": {"en": [audio_id]},
                "items": [
                    {"type": "Canvas",
                     "id": f"{canvas_id}#t={_fmt_t(start)},{_fmt_t(end)}"},
                ],
            })
        manifest["structures"] = [{
            "type": "Range",
            "id": f"{base}/range/issue",
            "label": {"en": [issue.issue_id]},
            "items": child_ranges,
        }]

    target_dir = build_target_path(
        target_base_dir, issue, records[0].record_num
    ).parent
    manifest_path = target_dir / MANIFEST_FILENAME

    n_records = len(records)
    if dry_run:
        logger.info(
            "[DRY RUN] Would write %s for %s (1 canvas, %d annotation%s)",
            manifest_path, issue.issue_id,
            n_records, "" if n_records == 1 else "s",
        )
        return manifest_path

    try:
        with open(manifest_path, "w", encoding="utf-8") as fout:
            json.dump(manifest, fout, ensure_ascii=False, indent=2)
        if os.path.getsize(manifest_path) == 0:
            raise OSError(f"Written manifest is zero bytes: {manifest_path}")
        logger.info(
            "%s: wrote %s (1 canvas, %d annotation%s)",
            issue.issue_id, manifest_path,
            n_records, "" if n_records == 1 else "s",
        )
    except OSError:
        logger.error(
            "%s: failed to write %s",
            issue.issue_id, manifest_path,
            exc_info=True,
        )
        raise

    return manifest_path


# ---------------------------------------------------------------------------
# Augmented provider-level issue index
# ---------------------------------------------------------------------------


def prune_index_to_scope(
    index: dict,
    aliases_include: list[str],
    aliases_exclude: list[str],
) -> None:
    """Remove aliases from the hierarchical index that aren't in the run's scope.

    Mirrors the alias filter used by ``load_issues()``. Mutates ``index`` in place.
    """
    include_set = set(aliases_include) if aliases_include else None
    exclude_set = set(aliases_exclude) if aliases_exclude else set()

    for alias in list(index.keys()):
        if include_set is not None and alias not in include_set:
            del index[alias]
            continue
        if alias in exclude_set:
            del index[alias]


def _find_index_entry(index: dict, issue: IssueRecord) -> dict | None:
    """Locate the issue's entry in the hierarchical index dict, or return None."""
    year_key = f"{issue.date.year:04d}"
    month_key = f"{issue.date.month:02d}"
    day_key = f"{issue.date.day:02d}"
    try:
        entries = index[issue.alias][year_key][month_key]
    except KeyError:
        return None
    for entry in entries:
        if entry.get("day") == day_key and entry.get("edition") == issue.edition:
            return entry
    return None


def augment_index_entry(
    index: dict,
    issue: IssueRecord,
    pages: list[PageFile],
    page_results: dict[int, dict],
    target_base_dir: str,
) -> None:
    """Augment the hierarchical index entry for ``issue`` with processing outcome.

    Adds: issue_id, num_pages, img_dir_path (relative to ``target_base_dir``),
    pages: [{page_num, original_filename, width, height}]. Mutates ``index`` in place.
    If the entry can't be located, logs a warning and returns.
    """
    entry = _find_index_entry(index, issue)
    if entry is None:
        logger.warning(
            "%s: no matching entry in issue index — augmentation skipped",
            issue.issue_id,
        )
        return

    target_dir = build_target_path(target_base_dir, issue, pages[0].page_num).parent
    try:
        rel_img_dir = target_dir.relative_to(Path(target_base_dir))
    except ValueError:
        rel_img_dir = target_dir

    entry["issue_id"] = issue.issue_id
    entry["num_pages"] = len(pages)
    entry["img_dir_path"] = str(rel_img_dir)
    entry["pages"] = [
        {
            "page_num": page.page_num,
            "original_filename": page.source_path.name,
            "width": page_results[page.page_num]["width"],
            "height": page_results[page.page_num]["height"],
        }
        for page in pages
    ]


def augment_index_entry_audio(
    index: dict,
    issue: IssueRecord,
    records: list[AudioRecord],
    record_results: dict[int, dict],
    target_base_dir: str,
) -> None:
    """Augment the hierarchical index entry for an audio ``issue``.

    Audio analogue of ``augment_index_entry``: adds ``issue_id``,
    ``num_records``, ``record_dir_path`` (relative to ``target_base_dir``), and
    ``records: [{record_num, original_filename, duration}]``. Mutates ``index``
    in place. If the entry can't be located, logs a warning and returns.
    """
    entry = _find_index_entry(index, issue)
    if entry is None:
        logger.warning(
            "%s: no matching entry in issue index — augmentation skipped",
            issue.issue_id,
        )
        return

    target_dir = build_target_path(target_base_dir, issue, records[0].record_num).parent
    try:
        rel_record_dir = target_dir.relative_to(Path(target_base_dir))
    except ValueError:
        rel_record_dir = target_dir

    entry["issue_id"] = issue.issue_id
    entry["num_records"] = len(records)
    entry["record_dir_path"] = str(rel_record_dir)
    entry["records"] = [
        {
            "record_num": record.record_num,
            "original_filename": record.source_path.name,
            "duration": record_results[record.record_num]["duration"],
        }
        for record in records
    ]


def remove_index_entry(index: dict, issue: IssueRecord) -> None:
    """Remove the issue's entry from the hierarchical index (used on failure).

    Prunes now-empty month/year/alias containers as well.
    """
    year_key = f"{issue.date.year:04d}"
    month_key = f"{issue.date.month:02d}"
    day_key = f"{issue.date.day:02d}"

    years = index.get(issue.alias)
    if not years:
        return
    months = years.get(year_key)
    if not months:
        return
    entries = months.get(month_key)
    if not entries:
        return

    months[month_key] = [
        e for e in entries
        if not (e.get("day") == day_key and e.get("edition") == issue.edition)
    ]
    if not months[month_key]:
        del months[month_key]
    if not months:
        del years[year_key]
    if not years:
        del index[issue.alias]


def populate_resumed_entry(
    index: dict,
    issue: IssueRecord,
    target_base_dir: str,
) -> None:
    """For an issue already successful in a prior run, populate its index entry
    by reading the existing ``renaming_info.json`` in its target directory.

    Drops the entry from the index if the file is missing or malformed.
    """
    entry = _find_index_entry(index, issue)
    if entry is None:
        return

    # Target dir is deterministic from issue, but page_num is unknown; use a
    # placeholder since build_target_path only affects the filename, not the dir.
    target_dir = build_target_path(target_base_dir, issue, 1).parent
    info_path = target_dir / RENAMING_INFO_FILENAME

    try:
        with open(info_path, "r", encoding="utf-8") as f:
            info = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(
            "%s: cannot populate resumed index entry from %s — %s",
            issue.issue_id, info_path, e,
        )
        remove_index_entry(index, issue)
        return

    try:
        rel_dir = target_dir.relative_to(Path(target_base_dir))
    except ValueError:
        rel_dir = target_dir

    if issue.media_type == "audio":
        records_list = []
        for rec_key in sorted(info.keys(), key=lambda k: int(k)):
            rec_meta = info[rec_key]
            records_list.append({
                "record_num": int(rec_key),
                "original_filename": rec_meta["original_filename"],
                "duration": rec_meta["duration"],
            })
        entry["issue_id"] = issue.issue_id
        entry["num_records"] = len(records_list)
        entry["record_dir_path"] = str(rel_dir)
        entry["records"] = records_list
    else:
        pages_list = []
        for page_key in sorted(info.keys(), key=lambda k: int(k)):
            page_meta = info[page_key]
            pages_list.append({
                "page_num": int(page_key),
                "original_filename": page_meta["original_filename"],
                "width": page_meta["width"],
                "height": page_meta["height"],
            })
        entry["issue_id"] = issue.issue_id
        entry["num_pages"] = len(pages_list)
        entry["img_dir_path"] = str(rel_dir)
        entry["pages"] = pages_list


def prune_unaugmented_entries(index: dict) -> int:
    """Remove any entry that wasn't augmented (no ``issue_id``) and collapse
    empty month/year/alias containers. Returns the number of entries removed.

    Covers failures, sample-truncated issues, and resume-path entries whose
    ``renaming_info.json`` was unreadable.
    """
    removed = 0
    for alias in list(index.keys()):
        years = index[alias]
        for year_key in list(years.keys()):
            months = years[year_key]
            for month_key in list(months.keys()):
                entries = months[month_key]
                kept = [e for e in entries if "issue_id" in e]
                removed += len(entries) - len(kept)
                if kept:
                    months[month_key] = kept
                else:
                    del months[month_key]
            if not months:
                del years[year_key]
        if not years:
            del index[alias]
    return removed


def count_index_issues(index: dict) -> int:
    """Count the total number of issue entries in the hierarchical index."""
    return sum(
        len(entries)
        for years in index.values()
        for months in years.values()
        for entries in months.values()
    )


def write_issue_index(
    index: dict,
    target_base_dir: str,
    provider: str,
    dry_run: bool = False,
    scope_suffix: str | None = None,
) -> Path:
    """Write the augmented issue index under ``{target_base_dir}/issue_index/``.

    Filename is ``issue_index.{provider}.json`` for full runs (no scope
    suffix), ``issue_index.{provider}.sample.json`` for sample runs, or
    ``issue_index.{provider}.partial.json`` when alias include/exclude
    filters are active. The non-canonical names ensure a partial or sample
    run can never clobber the canonical full-run index.

    Args:
        scope_suffix: ``None`` for a full-collection run (canonical filename),
            ``"sample"`` for sample mode, ``"partial"`` for filtered scope.
    """
    index_dir = Path(target_base_dir) / "issue_index"
    suffix = f".{scope_suffix}" if scope_suffix else ""
    filename = f"issue_index.{provider}{suffix}.json"
    index_path = index_dir / filename
    n = count_index_issues(index)

    if dry_run:
        logger.info(
            "[DRY RUN] Would write %s (%d issues)", index_path, n,
        )
        return index_path

    index_dir.mkdir(parents=True, exist_ok=True)
    with open(index_path, "w", encoding="utf-8") as fout:
        json.dump(index, fout, ensure_ascii=False, indent=2)
    logger.info("Wrote augmented issue index: %s (%d issues)", index_path, n)
    return index_path


# ---------------------------------------------------------------------------
# Processing report
# ---------------------------------------------------------------------------


class ReportWriter:
    """Split report writer with resume support.

    A directory ``{report_dir}`` holds two fixed-name files:

    - ``{report_dir}/success.txt`` — plain text, one ``issue_id`` per line.
    - ``{report_dir}/failed.jsonl`` — JSONL, one object per failed issue, with
      ``issue_id``, ``status="failed"``, ``num_pages``, ``pages_ok``,
      ``failed_pages``, ``errors``, ``timestamp``.

    Both files are truncated and rewritten at the start of each run via
    :meth:`prepare`: prior in-scope successes are carried forward, then this
    run's outcomes are appended live (flushed per write). Net: each
    ``report_dir`` is a self-contained snapshot of "what's been done so far
    for this scope", regardless of whether the prior report lives in the same
    directory or a different one.

    Resume logic (when a prior directory is provided):
    - ``retry_failed_only=False``: skip issues listed in the prior success
      file; retry everything else.
    - ``retry_failed_only=True``: *only* process issues present in the prior
      failed file.

    ``overwrite`` is orthogonal to all of the above — it is a unit-level
    force-reconvert flag (see the main loop's reuse gate), not a resume knob.
    ``overwrite=True`` + ``prior_report_dir`` thus resumes (skips prior
    successes) *and* recompresses every remaining issue rather than reusing
    the target files already on disk.
    """

    SUCCESS_FILENAME = "success.txt"
    FAILED_FILENAME = "failed.jsonl"

    def __init__(
        self,
        report_dir: str | None = None,
        prior_report_dir: str | None = None,
        retry_failed_only: bool = False,
        dry_run: bool = False,
        overwrite: bool = False,
    ):
        self._report_dir = report_dir
        self._prior_report_dir = prior_report_dir
        self.retry_failed_only = retry_failed_only
        self._dry_run = dry_run
        # Unit-level knob: when True, existing target files are force-reconverted
        # instead of reused (see the main loop's reuse gate). It does NOT affect
        # the issue-level resume decision — which issues run is governed solely
        # by prior_report_dir + retry_failed_only. Combine overwrite=True with a
        # prior_report_dir to resume a killed compress run and recompress the rest.
        self.overwrite = overwrite
        self._counts = {"success": 0, "failed": 0, "resumed": 0}
        self._prepared = False

        # --- load prior report (in-memory only) ---
        self._prior_success: set[str] = set()
        self._prior_failed: dict[str, dict] = {}
        if prior_report_dir:
            self._prior_success, self._prior_failed = self._load_prior_report(
                prior_report_dir
            )
            logger.info(
                "Loaded prior report from directory %s (%d success, %d failed)",
                prior_report_dir,
                len(self._prior_success),
                len(self._prior_failed),
            )

        # File handles opened lazily in prepare().
        self._success_fh = None
        self._failed_fh = None

    def prepare(self, scope_ids: set[str]) -> None:
        """Open the report files and carry-forward in-scope prior successes.

        Truncates ``success.txt`` and ``failed.jsonl`` in ``report_dir``,
        writes back the subset of prior successes that intersects ``scope_ids``,
        then leaves the handles open so run-time writes append.

        Must be called once after the run's scope is known (i.e. after
        ``load_issues``). In ``dry_run`` mode no files are touched.

        Args:
            scope_ids: issue IDs in this run's scope (after alias filtering,
                before resume filtering). Used to decide which prior
                successes to carry forward — out-of-scope priors are dropped
                so the report stays focused on the current scope.

        Raises:
            RuntimeError: if called twice; if ``retry_failed_only=True`` but
                no prior failures were loaded; or if ``report_dir`` already
                contains a non-empty report file and no ``prior_report_dir``
                was given (refuse-clobber guardrail).
        """
        if self._prepared:
            raise RuntimeError("ReportWriter.prepare() called twice")
        self._prepared = True

        if self.retry_failed_only and not self._prior_failed:
            raise RuntimeError(
                "retry_failed_only=True but no prior failures loaded — "
                "set prior_report_dir to a directory containing failed.jsonl"
            )

        if self._dry_run or not self._report_dir:
            return

        if not os.path.isdir(self._report_dir):
            os.makedirs(self._report_dir, exist_ok=True)
            logger.info("Created report directory: %s", self._report_dir)

        success_path = os.path.join(self._report_dir, self.SUCCESS_FILENAME)
        failed_path = os.path.join(self._report_dir, self.FAILED_FILENAME)

        # Refuse silent clobber of an existing non-empty report.
        if not self._prior_report_dir:
            for p in (success_path, failed_path):
                if os.path.isfile(p) and os.path.getsize(p) > 0:
                    raise RuntimeError(
                        f"report_dir {self._report_dir!r} contains an existing "
                        f"non-empty {os.path.basename(p)}. Pass "
                        f"prior_report_dir={self._report_dir!r} to resume, or "
                        f"remove the directory to start fresh."
                    )

        # Truncate, then carry-forward in-scope prior successes.
        self._success_fh = open(success_path, "w", encoding="utf-8")
        self._failed_fh = open(failed_path, "w", encoding="utf-8")

        # Carry forward in-scope prior successes: these issues are skipped by
        # should_process() (resume), so they never reach write_success() this
        # run and must be re-emitted here to keep the report complete. Reprocessed
        # issues are by definition absent from _prior_success, so no duplication.
        carried = 0
        for issue_id in self._prior_success & scope_ids:
            self._success_fh.write(issue_id + "\n")
            carried += 1
        if carried:
            self._success_fh.flush()
            logger.info(
                "Carried forward %d prior success entr%s into %s",
                carried, "y" if carried == 1 else "ies", success_path,
            )

        logger.info(
            "Report files: %s, %s (truncated)",
            success_path,
            failed_path,
        )

    # --- prior report loading ---

    @classmethod
    def _load_prior_report(
        cls, prior_report_dir: str
    ) -> tuple[set[str], dict[str, dict]]:
        """Read prior ``success.txt`` + ``failed.jsonl`` from a directory.

        Returns:
            (success_set, failed_dict) — both may be empty.
        """
        success_path = os.path.join(prior_report_dir, cls.SUCCESS_FILENAME)
        failed_path = os.path.join(prior_report_dir, cls.FAILED_FILENAME)

        success_set: set[str] = set()
        if os.path.isfile(success_path):
            with open(success_path, "r", encoding="utf-8") as f:
                for line in f:
                    issue_id = line.strip()
                    if issue_id:
                        success_set.add(issue_id)
        else:
            logger.info("Prior success file not found: %s", success_path)

        failed_dict: dict[str, dict] = {}
        if os.path.isfile(failed_path):
            with open(failed_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning(
                            "Skipping malformed line %d in prior report %s",
                            line_num,
                            failed_path,
                        )
                        continue
                    issue_id = entry.get("issue_id")
                    if issue_id:
                        failed_dict[issue_id] = entry
        else:
            logger.info("Prior failed file not found: %s", failed_path)

        return success_set, failed_dict

    @property
    def has_prior(self) -> bool:
        return bool(self._prior_success) or bool(self._prior_failed)

    def add_resumed(self, count: int) -> None:
        """Bulk-increment the resumed counter (for issues already done in a prior run)."""
        self._counts["resumed"] += count

    # --- resume decision ---

    def should_process(self, issue_id: str) -> bool:
        """Decide whether an issue should be processed in this run.

        Returns True if the issue should be processed, False if it should be
        skipped based on the prior report.

        ``overwrite`` is intentionally NOT consulted here: it is a *unit-level*
        knob (force-reconvert existing target files rather than reuse them),
        orthogonal to the *issue-level* resume decision. This lets
        ``overwrite=True`` + ``prior_report_dir`` mean "resume the prior run
        (skip its successes) and force-recompress every remaining issue" — the
        combination a killed compress run needs. Without a prior report,
        ``_prior_success`` is empty, so every issue is still processed.
        """
        if self.retry_failed_only:
            # Only process issues that explicitly failed before.
            return issue_id in self._prior_failed

        # Default: skip issues that already succeeded.
        return issue_id not in self._prior_success

    # --- write entries ---

    def write_success(self, issue_id: str) -> None:
        self._counts["success"] += 1
        if self._success_fh is not None:
            self._success_fh.write(issue_id + "\n")
            self._success_fh.flush()

    def write_failure(
        self,
        issue_id: str,
        num_pages: int,
        pages_ok: int,
        errors: list[dict],
    ) -> None:
        """Record a failed issue.

        Args:
            errors: list of ``{"page": <int>, "error": <str>}`` dicts.
        """
        self._counts["failed"] += 1
        entry = {
            "issue_id": issue_id,
            "status": "failed",
            "num_pages": num_pages,
            "pages_ok": pages_ok,
            "failed_pages": [e["page"] for e in errors],
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
        }
        if self._failed_fh is not None:
            self._failed_fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._failed_fh.flush()

    # --- context manager & cleanup ---

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.log_summary()
        self.close()

    def log_summary(self) -> None:
        total = sum(self._counts.values())
        logger.info(
            "Report summary: %d total — %d success, %d failed, %d resumed",
            total,
            self._counts["success"],
            self._counts["failed"],
            self._counts["resumed"],
        )

    def close(self) -> None:
        if self._success_fh is not None:
            self._success_fh.close()
            self._success_fh = None
        if self._failed_fh is not None:
            self._failed_fh.close()
            self._failed_fh = None


# ---------------------------------------------------------------------------
# Image conversion utilities
# ---------------------------------------------------------------------------


def _validate_jp2(target_path: str | Path) -> None:
    """Verify a JP2 file is a complete, decodable codestream.

    Raises OSError with a precise reason on missing/short/header-only files.
    Catches the silent failure mode where opj_compress exits 0 after writing
    only the JP2 box skeleton (no SOC marker), which Pillow accepts but
    real decoders reject.
    """
    if not os.path.exists(target_path):
        raise OSError(f"JP2 missing: {target_path}")
    size = os.path.getsize(target_path)
    if size < MIN_VALID_JP2_BYTES:
        raise OSError(
            f"JP2 implausibly small ({size} bytes < {MIN_VALID_JP2_BYTES}): "
            f"{target_path}"
        )
    result = subprocess.run(
        ["opj_dump", "-i", str(target_path)],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise OSError(
            f"JP2 failed opj_dump ({target_path}): {result.stderr.strip()}"
        )


def _run_opj_compress(
    source_path: str | Path,
    target_path: str | Path,
    compression_ratio: int = DEFAULT_JP2_COMPRESSION_RATIO,
) -> None:
    """Run opj_compress to produce a JP2.

    ``compression_ratio`` maps to opj_compress ``-r`` (1 = lossless; higher =
    smaller + lossier). Raises RuntimeError on non-zero exit and OSError on
    missing/short/invalid output (validated via opj_dump).
    """
    cmd = [
        "opj_compress",
        "-i", str(source_path),
        "-o", str(target_path),
        "-r", str(compression_ratio),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(
            f"opj_compress failed (exit {result.returncode}) for "
            f"{source_path}: {result.stderr.strip()}"
        )
    _validate_jp2(target_path)


def convert_image_to_jp2(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
    compression_ratio: int = DEFAULT_JP2_COMPRESSION_RATIO,
) -> tuple[int, int]:
    """Convert a TIF/PNG/JPG image to JP2 via opj_compress.

    ``compression_ratio`` maps to opj_compress ``-r`` (1 = lossless; higher =
    smaller + lossier). Returns (width, height) of the image.
    """
    with Image.open(source_path) as img:
        width, height = img.size

    if dry_run:
        logger.info(
            "[DRY RUN] Would convert %s -> %s (%dx%d) at -r %d",
            source_path, target_path, width, height, compression_ratio,
        )
        return width, height

    # opj_compress can't read JPEG — convert to a temp TIF first
    src = Path(source_path)
    if src.suffix.lower() in {".jpg", ".jpeg"}:
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with Image.open(source_path) as img:
                img.save(tmp_path, format="TIFF")
            _run_opj_compress(tmp_path, target_path, compression_ratio)
        finally:
            os.unlink(tmp_path)
    else:
        _run_opj_compress(source_path, target_path, compression_ratio)

    # Verify dimensions match after conversion
    with Image.open(target_path) as saved:
        tw, th = saved.size
    if (tw, th) != (width, height):
        raise ValueError(
            f"Dimension mismatch after conversion: "
            f"source ({width}x{height}) vs target ({tw}x{th})"
        )

    # Surface re-inflation: an already-heavily-compressed source (e.g. lossy
    # JPEG) can yield a JP2 larger than itself. Unavoidable for such sources, but
    # worth flagging rather than silently shipping a bigger file.
    src_size = os.path.getsize(source_path)
    out_size = os.path.getsize(target_path)
    if out_size > src_size:
        logger.warning(
            "JP2 larger than source (%.1fMB > %.1fMB, -r %d): %s — "
            "source is likely already heavily compressed",
            out_size / 1e6, src_size / 1e6, compression_ratio, target_path,
        )

    logger.info(
        "Converted %s -> %s (%dx%d)",
        source_path, target_path, width, height,
    )
    return width, height


def extract_pdf_page_to_jp2(
    pdf_path: str | Path,
    page_num: int,
    target_path: str | Path,
    expected_dimensions: tuple[int, int] | None = None,
    fallback_dpi: int = 300,
    dry_run: bool = False,
) -> tuple[int, int, str]:
    """Extract a single page from a PDF and save as JP2.

    Tries to extract the embedded raster image directly (approach a).
    Falls back to rasterizing at fallback_dpi (approach b) if that fails.

    Args:
        pdf_path: Path to the source PDF.
        page_num: 0-based page index.
        target_path: Where to write the JP2.
        expected_dimensions: Optional (width, height) from METS/ALTO for validation.
        fallback_dpi: DPI for rasterization fallback.
        dry_run: If True, extract/rasterize to get dimensions but skip writing.

    Returns:
        (width, height, method) where method is "embedded_raster" or "rasterized".
    """
    # TODO: implement once PDF page-discovery is in place
    raise NotImplementedError("PDF page extraction is not yet implemented")


def copy_jp2(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Copy a JP2 file and return its dimensions.

    Uses ``shutil.copyfile`` (bytes only, no metadata) rather than ``copy2``:
    the cluster CIFS mounts force ``file_mode``, so ``copy2``'s post-copy
    ``copystat`` fails with EPERM. Source mtime/mode are not consumed by
    any downstream artifact in this pipeline.
    """
    with Image.open(source_path) as img:
        width, height = img.size

    if dry_run:
        logger.info(
            "[DRY RUN] Would copy JP2 %s -> %s (%dx%d)",
            source_path, target_path, width, height,
        )
    else:
        shutil.copyfile(source_path, target_path)
        if os.path.getsize(target_path) == 0:
            raise OSError(f"Copied file is zero bytes: {target_path}")
        logger.info(
            "Copied JP2 %s -> %s (%dx%d)",
            source_path, target_path, width, height,
        )
    return width, height


def _read_audio_duration_seconds(path: str | Path) -> float:
    """Return an audio file's duration as a float (seconds).

    Uses ``mutagen.mp3.MP3`` — same call as
    ``INABroadcastAudioRecord._get_duration`` in the INA importer, and more
    precise than the generic ``mutagen.File`` for mp3 sources. The float is
    the canonical value: the HH:MM:SS string consumed by ``renaming_info.json``
    is derived from it, and the rounded float feeds the IIIF manifest's
    ``duration`` field.
    """
    info = MP3(str(path)).info
    if info is None:
        raise OSError(f"Could not read mp3 metadata from {path}")
    return info.length


def _read_audio_duration(path: str | Path) -> str:
    """Return an audio file's duration as an ``HH:MM:SS`` string.

    Format matches ``CanonicalAudioRecord``'s ``dur`` field (used by the INA
    importer). Derived from ``_read_audio_duration_seconds`` so seconds and
    HH:MM:SS share a single mutagen call when callers need both.
    """
    return strftime("%H:%M:%S", gmtime(_read_audio_duration_seconds(path)))


def copy_audio(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
) -> tuple[str, float]:
    """Copy an audio file unchanged and return its duration.

    Mirrors ``copy_jp2``: a bit-identical ``shutil.copyfile`` (bytes only,
    no metadata) with a zero-byte guard. No re-encoding — lossy formats
    stay untouched. ``copyfile`` rather than ``copy2`` because the cluster
    CIFS mounts force ``file_mode`` and reject ``copystat``'s ``chmod``
    with EPERM; source mtime/mode are not consumed downstream.

    Returns a ``(duration_hms, duration_seconds)`` tuple — both come from a
    single mutagen read of the source. ``duration_hms`` (HH:MM:SS) feeds
    ``renaming_info.json``; ``duration_seconds`` (float) feeds the IIIF
    manifest's ``Canvas.duration`` and ``body.duration``.
    """
    duration_seconds = _read_audio_duration_seconds(source_path)
    duration = strftime("%H:%M:%S", gmtime(duration_seconds))

    if dry_run:
        logger.info(
            "[DRY RUN] Would copy audio %s -> %s (%s)",
            source_path, target_path, duration,
        )
    else:
        shutil.copyfile(source_path, target_path)
        if os.path.getsize(target_path) == 0:
            raise OSError(f"Copied audio file is zero bytes: {target_path}")
        logger.info(
            "Copied audio %s -> %s (%s)", source_path, target_path, duration,
        )
    return duration, duration_seconds


def convert_audio(
    source_path: str | Path,
    target_path: str | Path,
    source_format: str,
    dry_run: bool = False,
) -> dict:
    """Prepare an audio record, dispatching on source format.

    Implemented: ``mp3 -> mp3`` (direct copy, like ``jp2 -> jp2``).

    Non-mp3 sources (wav/flac/m4a/ogg/aac) are NOT yet transcoded — this raises
    ``NotImplementedError``. The signature mirrors ``convert_to_jp2`` so the
    main loop can dispatch on a single converter callable.

    Returns:
        Dict with keys: duration, converted, method, source_format.
    """
    target_dir = os.path.dirname(target_path)
    if not dry_run and target_dir:
        os.makedirs(target_dir, exist_ok=True)

    fmt = source_format.lower().strip()

    if fmt == "mp3":
        duration, duration_seconds = copy_audio(source_path, target_path, dry_run)
        return {
            "duration": duration,
            "duration_seconds": duration_seconds,
            "converted": False,
            "method": "copy",
            "source_format": "mp3",
        }

    # PROPOSED SOLUTION for non-mp3 -> mp3 (not implemented yet):
    # transcode with FFmpeg via subprocess using the libmp3lame encoder at VBR
    # quality 2 (~190 kbps, transparent for archival), mirroring the
    # _run_opj_compress subprocess pattern used for images:
    #
    #     cmd = ["ffmpeg", "-y", "-i", str(source_path),
    #            "-codec:a", "libmp3lame", "-q:a", "2", str(target_path)]
    #     result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    #     if result.returncode != 0:
    #         raise RuntimeError(f"ffmpeg failed (exit {result.returncode}): "
    #                            f"{result.stderr.strip()}")
    #     if os.path.getsize(target_path) == 0:
    #         raise OSError(f"ffmpeg produced an empty file: {target_path}")
    #     duration = _read_audio_duration(target_path)
    #
    # ffmpeg would become a startup hard-requirement (shutil.which) only when an
    # in-scope issue needs transcoding, analogous to the opj_compress check.
    raise NotImplementedError(
        f"Audio transcoding {fmt!r} -> mp3 is not implemented "
        f"(source: {source_path}). Only mp3 -> mp3 copy is currently supported."
    )


def convert_to_jp2(
    source_path: str | Path,
    target_path: str | Path,
    source_format: str,
    page_num: int | None = None,
    expected_dimensions: tuple[int, int] | None = None,
    fallback_dpi: int = 300,
    dry_run: bool = False,
    compression_ratio: int = DEFAULT_JP2_COMPRESSION_RATIO,
) -> dict:
    """Convert a source image to JP2, dispatching to the appropriate handler.

    Args:
        source_path: Path to the source file.
        target_path: Where to write the JP2.
        source_format: One of "jp2", "tif", "tiff", "png", "jpg", "jpeg", "pdf".
        page_num: 0-based page index (required for PDF).
        expected_dimensions: Optional (width, height) from METS/ALTO (PDF only).
        fallback_dpi: DPI for PDF rasterization fallback.
        dry_run: If True, read dimensions but don't write files.
        compression_ratio: opj_compress -r value (image conversion only; the
            jp2-copy and pdf branches ignore it). 1 = lossless.

    Returns:
        Dict with keys: width, height, converted, method, source_format.
    """
    # Create target directory
    target_dir = os.path.dirname(target_path)
    if not dry_run and target_dir:
        os.makedirs(target_dir, exist_ok=True)

    fmt = source_format.lower().strip()

    if fmt in {"tif", "tiff", "png", "jpg", "jpeg"}:
        width, height = convert_image_to_jp2(
            source_path, target_path, dry_run, compression_ratio
        )
        return {
            "width": width,
            "height": height,
            "converted": True,
            "method": "opj_compress",
            "source_format": fmt,
        }

    if fmt == "pdf":
        if page_num is None:
            raise ValueError("page_num is required for PDF source format")
        width, height, method = extract_pdf_page_to_jp2(
            source_path, page_num, target_path,
            expected_dimensions=expected_dimensions,
            fallback_dpi=fallback_dpi,
            dry_run=dry_run,
        )
        return {
            "width": width,
            "height": height,
            "converted": True,
            "method": method,
            "source_format": "pdf",
        }

    if fmt == "jp2":
        width, height = copy_jp2(source_path, target_path, dry_run)
        return {
            "width": width,
            "height": height,
            "converted": False,
            "method": "copy",
            "source_format": "jp2",
        }

    raise ValueError(f"Unsupported source format: '{source_format}'")


def _try_reuse_existing_unit(
    issue: IssueRecord,
    unit,
    target_path: Path,
) -> dict | None:
    """Return a reuse result dict if an existing target can be trusted, else None.

    Lets retries skip already-produced units. Dispatches on media type:

    - **image**: the target must pass ``_validate_jp2`` (size + opj_dump) and
      open via Pillow — header-only JP2 stubs are rejected so we never reuse
      garbage. Returns ``width``/``height``.
    - **audio**: a non-zero-byte target whose duration mutagen can read is
      trusted (an mp3 copy has no codestream to validate). Returns ``duration``.

    Any stat/validation error returns ``None`` so the caller reconverts; the
    convert step then surfaces a genuinely broken mount.
    """
    try:
        target_size = target_path.stat().st_size
    except FileNotFoundError:
        return None
    except OSError as e:
        logger.warning(
            "%s unit %d: cannot stat existing target (%s) — will reconvert: %s",
            issue.issue_id, unit.num, target_path, e,
        )
        return None

    if target_size <= 0:
        return None

    if issue.media_type == "audio":
        try:
            duration_seconds = _read_audio_duration_seconds(target_path)
        except Exception as e:
            logger.warning(
                "%s record %d: existing target invalid, reconverting: %s",
                issue.issue_id, unit.num, e,
            )
            return None
        duration = strftime("%H:%M:%S", gmtime(duration_seconds))
        logger.debug(
            "%s record %d: reusing existing %s (%s)",
            issue.issue_id, unit.num, target_path, duration,
        )
        return {
            "duration": duration,
            "duration_seconds": duration_seconds,
            "converted": False,
            "method": "reused",
            "source_format": unit.source_format,
        }

    try:
        _validate_jp2(target_path)
        with Image.open(target_path) as img:
            w, h = img.size
    except Exception as e:
        logger.warning(
            "%s page %d: existing target invalid, reconverting: %s",
            issue.issue_id, unit.num, e,
        )
        return None
    logger.debug(
        "%s page %d: reusing existing %s (%dx%d)",
        issue.issue_id, unit.num, target_path, w, h,
    )
    return {
        "width": w,
        "height": h,
        "converted": False,
        "method": "reused",
        "source_format": unit.source_format,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(
    config: str = "",
    dry_run: bool = None,
    workers: int = None,
    log_level: str = None,
    sample: int = None,
    overwrite: bool = None,
):
    """Prepare collection media (images or audio) for the Impresso server.

    Args:
        config: Path to the YAML configuration file (required).
        dry_run: Override dry_run from config. When True, no files are written.
        workers: Override workers from config. Number of threads for parallel
            page/record conversion (1 = sequential).
        log_level: Override log_level from config. One of DEBUG, INFO, WARNING,
            ERROR, CRITICAL.
        sample: Override sample from config. Process only the first N issues
            end-to-end (overrides dry_run to False).
        overwrite: Override overwrite from config. When True, existing target
            JP2s are reconverted instead of reused.
    """
    if not config:
        print("Error: --config is required. Pass a path to a YAML config file.")
        print("Example: impresso-structure-media --config config.yaml")
        sys.exit(1)

    # --- load config with CLI overrides ---
    cfg = load_config(
        config,
        dry_run=dry_run,
        workers=workers,
        log_level=log_level,
        sample=sample,
        overwrite=overwrite,
    )

    # --- logging ---
    if cfg.log_file:
        log_dir = os.path.dirname(cfg.log_file)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    level = getattr(logging, cfg.log_level.upper())
    init_logger(logger, level, cfg.log_file or None)

    # When logging to a file, also show ERROR+ on the terminal.
    if cfg.log_file:
        stderr_handler = logging.StreamHandler()
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(
            logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
        )
        logger.addHandler(stderr_handler)

    # Visible startup banner — printed unconditionally to stdout so the
    # operator immediately knows where this run's log and report end up,
    # even when --log_file routes the structured log to a file (in which
    # case nothing else lands on stdout until an ERROR fires).
    print(f"Log file:   {cfg.log_file or 'stdout'}", flush=True)
    print(f"Report dir: {cfg.report_dir or '(none — no report written)'}", flush=True)
    print(flush=True)

    cfg.log_summary()

    # --- sample mode: override dry_run ---
    if cfg.sample > 0:
        if cfg.dry_run:
            logger.info(
                "Sample mode (N=%d): overriding dry_run → False.",
                cfg.sample,
            )
            cfg.dry_run = False

    if cfg.dry_run:
        logger.info("DRY RUN — no files will be written or modified.")

    if cfg.overwrite:
        logger.info(
            "OVERWRITE — existing target files are force-reconverted instead of "
            "reused. Which issues run is still governed by the prior report; "
            "pair with prior_report_dir to resume a run and recompress the rest."
        )

    # --- validate paths ---
    if not os.path.isfile(cfg.issues_json_path):
        logger.error("Issues JSON not found: %s", cfg.issues_json_path)
        sys.exit(1)

    if not cfg.dry_run and not os.path.isdir(cfg.target_base_dir):
        logger.error("Target base dir does not exist: %s", cfg.target_base_dir)
        sys.exit(1)

    # --- initialize report ---
    with ReportWriter(
        report_dir=cfg.report_dir or None,
        prior_report_dir=cfg.prior_report_dir or None,
        retry_failed_only=cfg.retry_failed_only,
        dry_run=cfg.dry_run,
        overwrite=cfg.overwrite,
    ) as report:

        # --- step 2: load issue list + raw hierarchical index ---
        issues = load_issues(
            cfg.issues_json_path,
            cfg.aliases_include,
            cfg.aliases_exclude,
        )
        logger.info("Loaded %d issues from %s", len(issues), cfg.issues_json_path)

        # --- check opj_compress + opj_dump (only when images are in scope) ---
        # A pure-audio run (mp3 copy) needs neither encoder; requiring them
        # would needlessly block audio collections on hosts without OpenJPEG.
        needs_opj = any(i.media_type == "image" for i in issues)
        if needs_opj:
            opj_path = shutil.which("opj_compress")
            opj_dump_path = shutil.which("opj_dump")
            if opj_path is None or opj_dump_path is None:
                logger.error(
                    "opj_compress and/or opj_dump not found on PATH. "
                    "Install OpenJPEG: brew install openjpeg (macOS) "
                    "/ apt install libopenjp2-tools (Debian/Ubuntu)"
                )
                sys.exit(1)
            logger.info(
                "Using opj_compress: %s, opj_dump: %s (workers: %d)",
                opj_path, opj_dump_path, cfg.workers,
            )
        else:
            logger.info(
                "Audio-only run — skipping opj_compress/opj_dump check (workers: %d).",
                cfg.workers,
            )

        # --- load access-rights once (only when audio is in scope) ---
        # The file is read once at startup into a local dict; every per-issue
        # rights lookup is a pure in-memory walk — no per-issue file I/O.
        needs_rights = any(i.media_type == "audio" for i in issues)
        url_provider: str | None = None
        access_rights: dict | None = None
        # Audio data dirs nest under {target_base_dir}/{PROVIDER}/... so the
        # on-disk layout mirrors the IIIF URL (.../media/audio/{PROVIDER}/{alias}/...).
        # The augmented issue_index still lands at cfg.target_base_dir directly
        # (= the kind root), so all providers' indices co-locate there alongside
        # the image-side index — same convention for both media types.
        audio_target_base_dir: str | None = None
        if needs_rights:
            if not cfg.access_rights_path or not os.path.isfile(cfg.access_rights_path):
                logger.error(
                    "access_rights_path is required for audio issues "
                    "(missing or unset): %r", cfg.access_rights_path,
                )
                sys.exit(1)
            access_rights = load_access_rights(cfg.access_rights_path)
            url_provider = infer_provider(cfg.issues_json_path).upper()
            audio_target_base_dir = os.path.join(cfg.target_base_dir, url_provider)
            logger.info(
                "Loaded access-rights from %s once: %d aliases, %d total "
                "year-range entries (audio URL provider: %s, data dir: %s)",
                cfg.access_rights_path,
                len(access_rights),
                sum(len(v) for v in access_rights.values()),
                url_provider,
                audio_target_base_dir,
            )

        # Open report files (truncate + carry-forward in-scope prior successes).
        # Surfaces refuse-clobber and retry-only-no-prior errors before any work.
        report.prepare({i.issue_id for i in issues})

        augmented_index = load_issues_index(cfg.issues_json_path)
        prune_index_to_scope(
            augmented_index, cfg.aliases_include, cfg.aliases_exclude,
        )
        provider = infer_provider(cfg.issues_json_path)

        # --- validate source path ---
        if not os.path.isdir(cfg.source_base_dir):
            logger.error("Source base dir does not exist: %s", cfg.source_base_dir)
            sys.exit(1)

        if cfg.delete_source:
            logger.warning(
                "delete_source=True but source deletion is not yet implemented. Ignoring."
            )

        # --- resume filter ---
        already_done = 0
        resumed_issues: list[IssueRecord] = []
        if report.has_prior:
            issues_to_process = []
            for i in issues:
                if report.should_process(i.issue_id):
                    issues_to_process.append(i)
                else:
                    resumed_issues.append(i)
            already_done = len(resumed_issues)
            if already_done:
                logger.info(
                    "Resuming: %d issues already processed, %d to process",
                    already_done, len(issues_to_process),
                )
                report.add_resumed(already_done)
        else:
            issues_to_process = issues

        # --- sample mode: take first N issues ---
        if cfg.sample > 0:
            if cfg.sample >= len(issues_to_process):
                logger.warning(
                    "Sample size (%d) >= available issues (%d) — processing all.",
                    cfg.sample, len(issues_to_process),
                )
            else:
                issues_to_process = issues_to_process[:cfg.sample]
                logger.info(
                    "Sample mode: processing first %d issue(s).",
                    len(issues_to_process),
                )

        # --- process issues (steps 3–6) ---
        desc = "Processing (SAMPLE)" if cfg.sample > 0 else "Processing"
        with ThreadPoolExecutor(max_workers=cfg.workers) as executor:
            pbar = tqdm(issues_to_process, desc=desc, unit="issue",
                        total=len(issues_to_process) + already_done,
                        initial=already_done)
            for issue in pbar:

                # --- discover units: pages (image) or records (audio) (step 3) ---
                try:
                    if issue.media_type == "audio":
                        units = discover_audio_records(issue, cfg.source_base_dir)
                    else:
                        units = discover_pages(issue, cfg.source_base_dir)
                except (FileNotFoundError, NotImplementedError, ValueError) as e:
                    logger.error("%s: %s", issue.issue_id, e)
                    report.write_failure(
                        issue.issue_id, 0, 0, [{"page": 0, "error": str(e)}]
                    )
                    continue

                # Per-issue data root. Audio gets the {PROVIDER} segment so the
                # on-disk path mirrors the IIIF URL; image stays at the kind
                # root. The augmented issue_index (written outside this loop)
                # always uses cfg.target_base_dir.
                data_dir = (
                    audio_target_base_dir if issue.media_type == "audio"
                    else cfg.target_base_dir
                )

                # --- convert units in parallel (steps 4+5) ---
                unit_results: dict[int, dict] = {}
                errors: list[dict] = []
                convert_fn = (
                    convert_audio if issue.media_type == "audio"
                    else partial(convert_to_jp2, compression_ratio=cfg.compression_ratio)
                )

                future_to_unit = {}
                for unit in units:
                    target_path = build_target_path(
                        data_dir, issue, unit.num
                    )

                    # Reuse an existing target unless overwrite is set.
                    # Lets retries skip already-produced units cheaply.
                    if not cfg.overwrite and not cfg.dry_run:
                        reused = _try_reuse_existing_unit(issue, unit, target_path)
                        if reused is not None:
                            unit_results[unit.num] = reused
                            continue

                    future = executor.submit(
                        convert_fn,
                        unit.source_path,
                        target_path,
                        unit.source_format,
                        dry_run=cfg.dry_run,
                    )
                    future_to_unit[future] = unit

                for future in as_completed(future_to_unit):
                    unit = future_to_unit[future]
                    try:
                        unit_results[unit.num] = future.result()
                    except Exception as e:
                        logger.error(
                            "%s unit %d: %s", issue.issue_id, unit.num, e
                        )
                        errors.append({"page": unit.num, "error": str(e)})

                if errors:
                    # Successfully-produced units stay on disk so retries can
                    # reuse them. renaming_info.json is skipped (it requires
                    # full coverage of all units).
                    report.write_failure(
                        issue.issue_id, len(units), len(unit_results), errors
                    )
                    continue

                # --- audio access-rights lookup (step 9b) ---
                # In-memory dict walk against the once-loaded access-rights.
                # Missing alias / no covering year-range is a hard fail: no
                # renaming_info.json or manifest.json is written; the mp3
                # already on disk lets a retry converge after the rights file
                # is updated.
                rights_entry: dict | None = None
                if issue.media_type == "audio":
                    try:
                        rights_entry = find_access_rights_entry(
                            access_rights, issue.alias, issue.date.year
                        )
                    except KeyError as e:
                        logger.error("%s: %s", issue.issue_id, e)
                        report.write_failure(
                            issue.issue_id, len(units), len(unit_results),
                            [{"page": 0,
                              "error": f"access-rights lookup failed: {e}"}],
                        )
                        continue

                # --- write metadata (step 6) ---
                write_info = (
                    write_renaming_info_audio
                    if issue.media_type == "audio"
                    else write_renaming_info
                )
                try:
                    write_info(
                        issue,
                        units,
                        unit_results,
                        data_dir,
                        cfg.source_base_dir,
                        dry_run=cfg.dry_run,
                    )
                except OSError as e:
                    report.write_failure(
                        issue.issue_id,
                        len(units),
                        len(unit_results),
                        [{"page": 0, "error": f"Failed to write renaming_info.json: {e}"}],
                    )
                    continue

                # --- write IIIF manifest (step 9b, audio only) ---
                if issue.media_type == "audio":
                    try:
                        write_audio_manifest(
                            issue, units, unit_results,
                            url_provider, rights_entry,
                            data_dir, dry_run=cfg.dry_run,
                        )
                    except OSError as e:
                        report.write_failure(
                            issue.issue_id, len(units), len(unit_results),
                            [{"page": 0,
                              "error": f"Failed to write manifest.json: {e}"}],
                        )
                        continue

                report.write_success(issue.issue_id)
                if issue.media_type == "audio":
                    augment_index_entry_audio(
                        augmented_index, issue, units, unit_results, data_dir,
                    )
                else:
                    augment_index_entry(
                        augmented_index, issue, units, unit_results, data_dir,
                    )

        # --- step 6b: populate resumed entries + write augmented index ---
        for issue in resumed_issues:
            resumed_data_dir = (
                audio_target_base_dir if issue.media_type == "audio"
                else cfg.target_base_dir
            )
            populate_resumed_entry(augmented_index, issue, resumed_data_dir)

        dropped = prune_unaugmented_entries(augmented_index)
        if dropped:
            logger.info(
                "Pruned %d entr%s from augmented index (failed / not processed)",
                dropped, "y" if dropped == 1 else "ies",
            )
        # Pick the index filename suffix based on scope. Sample mode wins over
        # alias filters (more specific signal); only a fully unfiltered run
        # writes the canonical issue_index.{provider}.json.
        if cfg.sample > 0:
            scope_suffix = "sample"
        elif cfg.aliases_include or cfg.aliases_exclude:
            scope_suffix = "partial"
            logger.info(
                "Partial scope (alias filters active) — writing index to "
                "issue_index.%s.partial.json to avoid clobbering the "
                "canonical full-run index.",
                provider,
            )
        else:
            scope_suffix = None

        write_issue_index(
            augmented_index,
            cfg.target_base_dir,
            provider,
            dry_run=cfg.dry_run,
            scope_suffix=scope_suffix,
        )


def cli():
    """Entry point for the console_scripts wrapper."""
    fire.Fire(main)


if __name__ == "__main__":
    cli()
