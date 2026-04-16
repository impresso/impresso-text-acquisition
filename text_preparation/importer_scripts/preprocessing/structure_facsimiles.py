"""Prepare page facsimile images for the Impresso image server.

Copies, converts (to JP2), renames, and reorganizes page facsimile images
from any collection into the Impresso directory structure.

Usage:
    impresso-structure-facsimiles --config config.yaml
    impresso-structure-facsimiles --config config.yaml --dry_run
"""

import io
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
from pathlib import Path

import fire
import pymupdf
import yaml
from pdf2image import convert_from_path
from PIL import Image
from tqdm import tqdm

from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

IMAGE_EXTENSIONS = {".jp2", ".tif", ".tiff", ".png", ".jpg", ".jpeg"}
PDF_EXTENSIONS = {".pdf"}
ALL_SOURCE_EXTENSIONS = IMAGE_EXTENSIONS | PDF_EXTENSIONS
RENAMING_INFO_FILENAME = "renaming_info.json"


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

    # --- performance ---
    workers: int = 1  # 1 = sequential; >1 = parallel page conversion

    # --- output / logging ---
    log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_file: str = ""
    report_file: str = ""
    prior_report_file: str = ""
    retry_failed_only: bool = False

    def __post_init__(self):
        if not self.issues_json_path:
            raise ValueError("issues_json_path is required in config")
        if not self.target_base_dir:
            raise ValueError("target_base_dir is required in config")
        if self.workers < 1:
            raise ValueError(f"workers must be >= 1, got {self.workers}")
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
    local_path: str  # relative to source_base_dir
    imgs_subdir: str  # subfolder within local_path ("" = images directly in local_path)
    imgs_ext: str  # e.g. ".tif", ".jp2", ".pdf" ("" = auto-detect)

    @property
    def issue_id(self) -> str:
        """Canonical Impresso issue ID: {alias}-{YYYY}-{MM}-{DD}-{edition}."""
        return f"{self.alias}-{self.date:%Y-%m-%d}-{self.edition}"


def load_issues(
    json_path: str,
    aliases_include: list[str],
    aliases_exclude: list[str],
) -> list[IssueRecord]:
    """Parse the hierarchical issues JSON and return a flat list of IssueRecords.

    The JSON schema is: alias > year > month > [{day, edition, local_path, ...}].
    Each entry must include an ``imgs_ext`` field.

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
                        issues.append(
                            IssueRecord(
                                alias=alias,
                                date=date(int(year), int(month), int(entry["day"])),
                                edition=entry["edition"],
                                local_path=entry["local_path"],
                                imgs_subdir=entry.get("imgs_subdir", ""),
                                imgs_ext=entry["imgs_ext"],
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


def discover_pages(issue: IssueRecord, source_base_dir: str) -> list[PageFile]:
    """Discover page image files in an issue directory and extract page numbers.

    Resolves the image directory from the issue record, filters files by
    extension, and extracts page numbers from the trailing digits in each
    filename stem (e.g. ``00000001.tif`` → page 1,
    ``0002384_18420515_0001.jp2`` → page 1).

    Args:
        issue: The issue record describing where to find files.
        source_base_dir: Root of the source data tree.

    Returns:
        List of PageFile objects sorted by page number.

    Raises:
        FileNotFoundError: If the image directory doesn't exist or contains
            no valid page files.
        NotImplementedError: If ``imgs_ext`` is a PDF extension (TODO).
        ValueError: If duplicate page numbers are detected.
    """
    # --- resolve image directory ---
    img_dir = Path(source_base_dir) / issue.local_path.lstrip("/")
    if issue.imgs_subdir:
        img_dir = img_dir / issue.imgs_subdir

    if not img_dir.is_dir():
        raise FileNotFoundError(
            f"Image directory does not exist for {issue.issue_id}: {img_dir}"
        )

    # --- PDF: TODO stub ---
    if issue.imgs_ext.lower() in PDF_EXTENSIONS:
        raise NotImplementedError(
            f"PDF page discovery not yet implemented for {issue.issue_id} "
            f"(source: {img_dir})"
        )

    # --- filter files by extension ---
    target_ext = issue.imgs_ext.lower()
    matching_files = sorted(
        f for f in img_dir.iterdir()
        if f.is_file() and f.suffix.lower() == target_ext
    )

    if not matching_files:
        raise FileNotFoundError(
            f"No files with extension '{target_ext}' found in {img_dir} "
            f"for {issue.issue_id}"
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
        "%s: discovered %d pages (p%d–p%d) in %s",
        issue.issue_id,
        len(pages),
        pages[0].page_num,
        pages[-1].page_num,
        img_dir,
    )

    return pages


# ---------------------------------------------------------------------------
# Renaming & target path construction
# ---------------------------------------------------------------------------


def build_target_path(
    target_base_dir: str,
    issue: IssueRecord,
    page_num: int,
) -> Path:
    """Build the target JP2 path following Impresso conventions.

    Directory:  {target_base_dir}/{alias}/{YYYY}/{MM}/{DD}/{edition}/
    Filename:   {issue_id}-p{page_num:04d}.jp2

    Args:
        target_base_dir: Root of the writable image output tree.
        issue: The issue being processed.
        page_num: Page number (preserved from source filename, or 1-based
            sequential for PDF extraction).  Zero-padded to 4 digits.

    Returns:
        Full target path as a Path object.
    """
    target_dir = Path(target_base_dir) / issue.alias / f"{issue.date:%Y/%m/%d}" / issue.edition
    filename = f"{issue.issue_id}-p{page_num:04d}.jp2"
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

    ocr_dir_path = str(Path(source_base_dir) / issue.local_path.lstrip("/"))
    target_dir = build_target_path(target_base_dir, issue, pages[0].page_num).parent

    for page in pages:
        filename = f"{issue.issue_id}-p{page.page_num:04d}.jp2"
        result = page_results[page.page_num]

        info_dict[str(page.page_num)] = {
            "original_filename": page.source_path.name,
            "new_filename": filename,
            "issue_id": issue.issue_id,
            "img_dir_path": str(target_dir),
            "ocr_dir_path": ocr_dir_path,
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


# ---------------------------------------------------------------------------
# Processing report
# ---------------------------------------------------------------------------


class ReportWriter:
    """JSONL report writer with resume support.

    Each line in the report file is a self-contained JSON object recording the
    outcome of processing one issue.  The file is opened in append mode and
    flushed after every entry, so partial progress survives crashes.

    Resume logic (when a prior report is provided):
    - ``retry_failed_only=False``: skip issues that were ``success`` in the
      prior report; retry ``failed`` and ``skipped`` issues.
    - ``retry_failed_only=True``: *only* process issues that were ``failed``
      in the prior report — ignore new and previously skipped issues.
    """

    def __init__(
        self,
        report_path: str | None = None,
        prior_report_path: str | None = None,
        retry_failed_only: bool = False,
    ):
        self.retry_failed_only = retry_failed_only
        self._counts = {"success": 0, "failed": 0, "skipped": 0}

        # --- load prior report ---
        self._prior: dict[str, dict] = {}
        if prior_report_path:
            self._prior = self._load_prior_report(prior_report_path)
            logger.info(
                "Loaded prior report: %s (%d entries)",
                prior_report_path,
                len(self._prior),
            )

        # --- open report file for appending ---
        self._fh = None
        if report_path:
            report_dir = os.path.dirname(report_path)
            if report_dir and not os.path.isdir(report_dir):
                os.makedirs(report_dir, exist_ok=True)
                logger.info("Created report directory: %s", report_dir)
            self._fh = open(report_path, "a", encoding="utf-8")
            logger.info("Report file: %s (append mode)", report_path)

    # --- prior report loading ---

    @staticmethod
    def _load_prior_report(path: str) -> dict[str, dict]:
        """Read a JSONL report and return a dict keyed by issue_id.

        If an issue_id appears multiple times (e.g. from a retry), the last
        entry wins.
        """
        prior: dict[str, dict] = {}
        if not os.path.isfile(path):
            logger.info("Prior report not found, starting fresh: %s", path)
            return prior
        with open(path, "r", encoding="utf-8") as f:
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
                        path,
                    )
                    continue
                issue_id = entry.get("issue_id")
                if issue_id:
                    prior[issue_id] = entry
        return prior

    @property
    def has_prior(self) -> bool:
        return bool(self._prior)

    def add_skipped(self, count: int) -> None:
        """Bulk-increment the skipped counter (for pre-filtered issues)."""
        self._counts["skipped"] += count

    # --- resume decision ---

    def should_process(self, issue_id: str) -> bool:
        """Decide whether an issue should be processed in this run.

        Returns True if the issue should be processed, False if it should be
        skipped based on the prior report.
        """
        if not self._prior:
            # No prior report — process everything (unless retry_failed_only,
            # in which case there is nothing to retry).
            return not self.retry_failed_only

        prior_entry = self._prior.get(issue_id)

        if self.retry_failed_only:
            # Only process issues that explicitly failed before.
            return prior_entry is not None and prior_entry.get("status") == "failed"

        # Default resume: skip successes, retry everything else.
        return prior_entry is None or prior_entry.get("status") != "success"

    # --- write entries ---

    def write_success(self, issue_id: str, num_pages: int) -> None:
        self._write_entry({
            "issue_id": issue_id,
            "status": "success",
            "num_pages": num_pages,
            "timestamp": datetime.now().isoformat(),
        })

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
        self._write_entry({
            "issue_id": issue_id,
            "status": "failed",
            "num_pages": num_pages,
            "pages_ok": pages_ok,
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
        })

    def write_skip(self, issue_id: str, reason: str) -> None:
        self._write_entry({
            "issue_id": issue_id,
            "status": "skipped",
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        })

    def _write_entry(self, entry: dict) -> None:
        status = entry.get("status", "unknown")
        if status in self._counts:
            self._counts[status] += 1

        if self._fh is not None:
            issue_id = entry.get("issue_id")
            prior = self._prior.get(issue_id) if issue_id else None
            if prior and status == "skipped":
                return
            self._fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._fh.flush()

    # --- context manager & cleanup ---

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.log_summary()
        self.close()

    def log_summary(self) -> None:
        total = sum(self._counts.values())
        logger.info(
            "Report summary: %d total — %d success, %d failed, %d skipped",
            total,
            self._counts["success"],
            self._counts["failed"],
            self._counts["skipped"],
        )

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


# ---------------------------------------------------------------------------
# Image conversion utilities
# ---------------------------------------------------------------------------


def _run_opj_compress(
    source_path: str | Path,
    target_path: str | Path,
) -> None:
    """Run opj_compress for lossless JP2 conversion.

    Raises RuntimeError on non-zero exit and OSError on missing/empty output.
    """
    cmd = [
        "opj_compress",
        "-i", str(source_path),
        "-o", str(target_path),
        "-r", "1",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(
            f"opj_compress failed (exit {result.returncode}) for "
            f"{source_path}: {result.stderr.strip()}"
        )
    if not os.path.exists(target_path) or os.path.getsize(target_path) == 0:
        raise OSError(f"opj_compress produced no output: {target_path}")


def convert_image_to_jp2(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Convert a TIF/PNG/JPG image to lossless JP2 via opj_compress.

    Returns (width, height) of the image.
    """
    with Image.open(source_path) as img:
        width, height = img.size

    if dry_run:
        logger.info(
            "[DRY RUN] Would convert %s -> %s (%dx%d)",
            source_path, target_path, width, height,
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
            _run_opj_compress(tmp_path, target_path)
        finally:
            os.unlink(tmp_path)
    else:
        _run_opj_compress(source_path, target_path)

    # Verify dimensions match after conversion
    with Image.open(target_path) as saved:
        tw, th = saved.size
    if (tw, th) != (width, height):
        raise ValueError(
            f"Dimension mismatch after conversion: "
            f"source ({width}x{height}) vs target ({tw}x{th})"
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
    img = None
    method = None

    # --- Approach (a): extract embedded raster ---
    try:
        doc = pymupdf.open(pdf_path)
        try:
            page = doc.load_page(page_num)
            image_list = page.get_images(full=True)
            if len(image_list) != 1:
                raise ValueError(
                    f"Expected 1 embedded image on page {page_num}, "
                    f"found {len(image_list)}"
                )
            xref = image_list[0][0]
            base_image = doc.extract_image(xref)
            img = Image.open(io.BytesIO(base_image["image"]))
            method = "embedded_raster"
            logger.debug(
                "Extracted embedded raster from %s page %d (%dx%d)",
                pdf_path, page_num, img.size[0], img.size[1],
            )
        finally:
            doc.close()
    except Exception as e:
        # --- Approach (b): rasterize fallback ---
        logger.info(
            "Falling back to rasterization for %s page %d (reason: %s)",
            pdf_path, page_num, e,
        )
        images = convert_from_path(
            pdf_path,
            first_page=page_num + 1,
            last_page=page_num + 1,
            dpi=fallback_dpi,
        )
        img = images[0]
        method = "rasterized"

    try:
        width, height = img.size

        # --- Optional validation against expected dimensions ---
        if expected_dimensions is not None:
            exp_w, exp_h = expected_dimensions
            if (width, height) != (exp_w, exp_h):
                logger.warning(
                    "Dimension mismatch for %s page %d: "
                    "extracted (%dx%d) vs expected (%dx%d), method=%s",
                    pdf_path, page_num, width, height, exp_w, exp_h, method,
                )

        # --- Save via temp TIF + opj_compress ---
        if dry_run:
            logger.info(
                "[DRY RUN] Would save %s page %d -> %s (%dx%d, %s)",
                pdf_path, page_num, target_path, width, height, method,
            )
        else:
            with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                img.save(tmp_path, format="TIFF")
                _run_opj_compress(tmp_path, target_path)
            finally:
                os.unlink(tmp_path)
            logger.info(
                "Saved %s page %d -> %s (%dx%d, %s)",
                pdf_path, page_num, target_path, width, height, method,
            )
    finally:
        if img is not None:
            img.close()

    return width, height, method


def copy_jp2(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Copy a JP2 file and return its dimensions.

    Uses shutil.copy2 for a bit-identical copy (no dimension verification needed).
    """
    with Image.open(source_path) as img:
        width, height = img.size

    if dry_run:
        logger.info(
            "[DRY RUN] Would copy JP2 %s -> %s (%dx%d)",
            source_path, target_path, width, height,
        )
    else:
        shutil.copy2(source_path, target_path)
        if os.path.getsize(target_path) == 0:
            raise OSError(f"Copied file is zero bytes: {target_path}")
        logger.info(
            "Copied JP2 %s -> %s (%dx%d)",
            source_path, target_path, width, height,
        )
    return width, height


def convert_to_jp2(
    source_path: str | Path,
    target_path: str | Path,
    source_format: str,
    page_num: int | None = None,
    expected_dimensions: tuple[int, int] | None = None,
    fallback_dpi: int = 300,
    dry_run: bool = False,
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

    Returns:
        Dict with keys: width, height, converted, method, source_format.
    """
    # Create target directory
    target_dir = os.path.dirname(target_path)
    if not dry_run and target_dir:
        os.makedirs(target_dir, exist_ok=True)

    fmt = source_format.lower().strip()

    if fmt in {"tif", "tiff", "png", "jpg", "jpeg"}:
        width, height = convert_image_to_jp2(source_path, target_path, dry_run)
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(
    config: str = "",
    dry_run: bool = None,
    workers: int = None,
    log_level: str = None,
    sample: int = None,
):
    """Prepare page facsimile images for the Impresso image server.

    Args:
        config: Path to the YAML configuration file (required).
        dry_run: Override dry_run from config. When True, no files are written.
        workers: Override workers from config. Number of threads for parallel
            page conversion (1 = sequential).
        log_level: Override log_level from config. One of DEBUG, INFO, WARNING,
            ERROR, CRITICAL.
        sample: Override sample from config. Process only the first N issues
            end-to-end (overrides dry_run to False).
    """
    if not config:
        print("Error: --config is required. Pass a path to a YAML config file.")
        print("Example: impresso-structure-facsimiles --config config.yaml")
        sys.exit(1)

    # --- load config with CLI overrides ---
    cfg = load_config(
        config,
        dry_run=dry_run,
        workers=workers,
        log_level=log_level,
        sample=sample,
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

    # --- check opj_compress ---
    opj_path = shutil.which("opj_compress")
    if opj_path is None:
        logger.error(
            "opj_compress not found on PATH. "
            "Install OpenJPEG: brew install openjpeg (macOS) "
            "/ apt install libopenjp2-tools (Debian/Ubuntu)"
        )
        sys.exit(1)
    logger.info("Using opj_compress: %s (workers: %d)", opj_path, cfg.workers)

    # --- validate paths ---
    if not os.path.isfile(cfg.issues_json_path):
        logger.error("Issues JSON not found: %s", cfg.issues_json_path)
        sys.exit(1)

    if not cfg.dry_run and not os.path.isdir(cfg.target_base_dir):
        logger.error("Target base dir does not exist: %s", cfg.target_base_dir)
        sys.exit(1)

    # --- initialize report ---
    with ReportWriter(
        report_path=cfg.report_file or None,
        prior_report_path=cfg.prior_report_file or None,
        retry_failed_only=cfg.retry_failed_only,
    ) as report:

        # --- step 2: load issue list ---
        issues = load_issues(
            cfg.issues_json_path,
            cfg.aliases_include,
            cfg.aliases_exclude,
        )
        logger.info("Loaded %d issues from %s", len(issues), cfg.issues_json_path)

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
        if report.has_prior:
            issues_to_process = [i for i in issues if report.should_process(i.issue_id)]
            already_done = len(issues) - len(issues_to_process)
            if already_done:
                logger.info(
                    "Resuming: %d issues already processed, %d to process",
                    already_done, len(issues_to_process),
                )
                report.add_skipped(already_done)
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

                # --- discover pages (step 3) ---
                try:
                    pages = discover_pages(issue, cfg.source_base_dir)
                except (FileNotFoundError, NotImplementedError, ValueError) as e:
                    logger.error("%s: %s", issue.issue_id, e)
                    report.write_failure(
                        issue.issue_id, 0, 0, [{"page": 0, "error": str(e)}]
                    )
                    continue

                # --- convert pages in parallel (steps 4+5) ---
                page_results: dict[int, dict] = {}
                errors: list[dict] = []

                future_to_page = {}
                for page in pages:
                    target_path = build_target_path(
                        cfg.target_base_dir, issue, page.page_num
                    )
                    future = executor.submit(
                        convert_to_jp2,
                        page.source_path,
                        target_path,
                        page.source_format,
                        dry_run=cfg.dry_run,
                    )
                    future_to_page[future] = page

                for future in as_completed(future_to_page):
                    page = future_to_page[future]
                    try:
                        page_results[page.page_num] = future.result()
                    except Exception as e:
                        logger.error(
                            "%s page %d: %s", issue.issue_id, page.page_num, e
                        )
                        errors.append({"page": page.page_num, "error": str(e)})

                if errors:
                    # Clean up orphan JP2s from successfully-converted pages.
                    # The issue is marked failed, so renaming_info.json won't be
                    # written — leaving JP2 files without metadata is inconsistent.
                    # They will be re-created on retry.
                    if not cfg.dry_run:
                        for pg_num in page_results:
                            orphan = build_target_path(cfg.target_base_dir, issue, pg_num)
                            try:
                                orphan.unlink(missing_ok=True)
                            except OSError as cleanup_err:
                                logger.warning(
                                    "%s: could not remove orphan %s: %s",
                                    issue.issue_id, orphan, cleanup_err,
                                )
                    report.write_failure(
                        issue.issue_id, len(pages), len(page_results), errors
                    )
                    continue

                # --- write metadata (step 6) ---
                try:
                    write_renaming_info(
                        issue,
                        pages,
                        page_results,
                        cfg.target_base_dir,
                        cfg.source_base_dir,
                        dry_run=cfg.dry_run,
                    )
                except OSError as e:
                    report.write_failure(
                        issue.issue_id,
                        len(pages),
                        len(page_results),
                        [{"page": 0, "error": f"Failed to write renaming_info.json: {e}"}],
                    )
                    continue

                report.write_success(issue.issue_id, len(pages))


def cli():
    """Entry point for the console_scripts wrapper."""
    fire.Fire(main)


if __name__ == "__main__":
    cli()
