"""Prepare page facsimile images for the Impresso image server.

Copies, converts (to JP2), renames, and reorganizes page facsimile images
from any collection into the Impresso directory structure.

Usage:
    python structure_facsimiles.py --config config.yaml
    python structure_facsimiles.py --config config.yaml --dry_run
    python structure_facsimiles.py --config config.yaml --chunk_size 50 --chunk_idx 0
"""

import io
import json
import logging
import os
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime
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
    delete_source: bool = False
    source_format: str = "auto"  # jp2, tif, pdf, png, jpg, auto

    # --- output / logging ---
    log_file: str = ""
    report_file: str = ""
    prior_report_file: str = ""
    retry_failed_only: bool = False

    def __post_init__(self):
        if not self.issues_json_path:
            raise ValueError("issues_json_path is required in config")
        if not self.target_base_dir:
            raise ValueError("target_base_dir is required in config")

        # normalise source_format
        self.source_format = self.source_format.lower().strip()
        valid_formats = {"jp2", "tif", "tiff", "png", "jpg", "jpeg", "pdf", "auto"}
        if self.source_format not in valid_formats:
            raise ValueError(
                f"source_format must be one of {valid_formats}, got '{self.source_format}'"
            )

    def log_summary(self):
        """Log the resolved configuration."""
        logger.info("Resolved configuration:")
        for k, v in asdict(self).items():
            logger.info("  %s: %s", k, v)


def load_config(config_path: str, **overrides) -> Config:
    """Load a YAML config file and apply CLI overrides.

    Any key passed as a CLI flag (e.g. --dry_run, --chunk_size) takes
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
            self._fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._fh.flush()

    # --- summary & cleanup ---

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



def convert_image_to_jp2(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Convert a TIF/PNG/JPG image to JP2 (lossless).

    Returns (width, height) of the image.
    """
    with Image.open(str(source_path)) as img:
        width, height = img.size
        if dry_run:
            logger.info(
                "[DRY RUN] Would convert %s -> %s (%dx%d)",
                source_path, target_path, width, height,
            )
        else:
            img.save(str(target_path), format="JPEG2000")
            # Verify dimensions match after conversion
            with Image.open(str(target_path)) as saved:
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
        doc = pymupdf.open(str(pdf_path))
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
            str(pdf_path),
            first_page=page_num + 1,
            last_page=page_num + 1,
            dpi=fallback_dpi,
        )
        img = images[0]
        method = "rasterized"

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

    # --- Save ---
    if dry_run:
        logger.info(
            "[DRY RUN] Would save %s page %d -> %s (%dx%d, %s)",
            pdf_path, page_num, target_path, width, height, method,
        )
    else:
        img.save(str(target_path), format="JPEG2000")
        logger.info(
            "Saved %s page %d -> %s (%dx%d, %s)",
            pdf_path, page_num, target_path, width, height, method,
        )

    return width, height, method


def copy_jp2(
    source_path: str | Path,
    target_path: str | Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Copy a JP2 file and return its dimensions.

    Uses shutil.copy2 for a bit-identical copy (no dimension verification needed).
    """
    with Image.open(str(source_path)) as img:
        width, height = img.size

    if dry_run:
        logger.info(
            "[DRY RUN] Would copy JP2 %s -> %s (%dx%d)",
            source_path, target_path, width, height,
        )
    else:
        shutil.copy2(str(source_path), str(target_path))
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
    target_dir = os.path.dirname(str(target_path))
    if not dry_run and target_dir:
        os.makedirs(target_dir, exist_ok=True)

    fmt = source_format.lower().strip()

    if fmt in {"tif", "tiff", "png", "jpg", "jpeg"}:
        width, height = convert_image_to_jp2(source_path, target_path, dry_run)
        return {
            "width": width,
            "height": height,
            "converted": True,
            "method": "pillow",
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
    verbose: bool = False,
):
    """Prepare page facsimile images for the Impresso image server.

    Args:
        config: Path to the YAML configuration file (required).
        dry_run: Override dry_run from config. When True, no files are written.
        verbose: If True, set logging to DEBUG; otherwise INFO.
    """
    if not config:
        print("Error: --config is required. Pass a path to a YAML config file.")
        print("Example: python structure_facsimiles.py --config config.yaml")
        return

    # --- load config with CLI overrides ---
    cfg = load_config(
        config,
        dry_run=dry_run
    )

    # --- logging ---
    log_level = logging.DEBUG if verbose else logging.INFO
    init_logger(logger, log_level, cfg.log_file or None)

    cfg.log_summary()

    if cfg.dry_run:
        logger.info("DRY RUN — no files will be written or modified.")

    # --- validate paths ---
    if not os.path.isfile(cfg.issues_json_path):
        logger.error("Issues JSON not found: %s", cfg.issues_json_path)
        return

    if not cfg.dry_run and not os.path.isdir(cfg.target_base_dir):
        logger.error("Target base dir does not exist: %s", cfg.target_base_dir)
        return

    # --- initialize report ---
    report = ReportWriter(
        report_path=cfg.report_file or None,
        prior_report_path=cfg.prior_report_file or None,
        retry_failed_only=cfg.retry_failed_only,
    )

    # --- placeholder: steps 2–6, 8 will plug in here ---
    # The processing loop will use:
    #   report.should_process(issue_id) — to decide whether to process or skip
    #   report.write_success(issue_id, num_pages) — after successful processing
    #   report.write_failure(issue_id, num_pages, pages_ok, errors) — on failure
    #   report.write_skip(issue_id, reason) — when skipping
    logger.info("Configuration OK. Ready to process.")
    logger.info("(Steps 2-6, 8 not yet implemented)")

    report.log_summary()
    report.close()


if __name__ == "__main__":
    fire.Fire(main)
