"""Tests for image conversion utilities in structure_facsimiles.py."""

import filecmp
import io
import json
import logging
import os
import shutil
from datetime import date
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pymupdf
import pytest
from PIL import Image

from text_preparation.importer_scripts.preprocessing.structure_facsimiles import (
    RENAMING_INFO_FILENAME,
    IssueRecord,
    PageFile,
    build_target_path,
    convert_image_to_jp2,
    convert_to_jp2,
    copy_jp2,
    extract_pdf_page_to_jp2,
    write_renaming_info,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IMG_WIDTH = 100
IMG_HEIGHT = 80
IMG_COLOR = (200, 100, 50)

requires_poppler = pytest.mark.skipif(
    shutil.which("pdftoppm") is None,
    reason="poppler (pdftoppm) not installed",
)

requires_opj_compress = pytest.mark.skipif(
    shutil.which("opj_compress") is None,
    reason="opj_compress not installed (brew install openjpeg)" \
    "apt install libopenjp2-tools (Debian/Ubuntu)",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_image():
    """Create a fresh PIL Image (not saved to disk)."""
    return Image.new("RGB", (IMG_WIDTH, IMG_HEIGHT), color=IMG_COLOR)


@pytest.fixture
def noisy_image():
    """Create a PIL Image with random pixel data (worst case for lossless)."""
    rng = np.random.RandomState(42)
    arr = rng.randint(0, 256, (IMG_HEIGHT, IMG_WIDTH, 3), dtype=np.uint8)
    return Image.fromarray(arr)


@pytest.fixture
def tif_image(tmp_path, sample_image):
    path = tmp_path / "source.tif"
    sample_image.save(str(path), format="TIFF")
    return path


@pytest.fixture
def png_image(tmp_path, sample_image):
    path = tmp_path / "source.png"
    sample_image.save(str(path), format="PNG")
    return path


@pytest.fixture
def jpg_image(tmp_path, sample_image):
    path = tmp_path / "source.jpg"
    sample_image.save(str(path), format="JPEG")
    return path


@pytest.fixture
def jp2_image(tmp_path, sample_image):
    path = tmp_path / "source.jp2"
    sample_image.save(str(path), format="JPEG2000")
    return path


@pytest.fixture
def pdf_with_image(tmp_path, sample_image):
    """Create a single-page PDF with one embedded raster image."""
    pdf_path = tmp_path / "source.pdf"
    buf = io.BytesIO()
    sample_image.save(buf, format="PNG")
    png_bytes = buf.getvalue()

    doc = pymupdf.open()
    page = doc.new_page(width=IMG_WIDTH, height=IMG_HEIGHT)
    rect = pymupdf.Rect(0, 0, IMG_WIDTH, IMG_HEIGHT)
    page.insert_image(rect, stream=png_bytes)
    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


@pytest.fixture
def pdf_no_images(tmp_path):
    """Create a single-page PDF with text only (no embedded images)."""
    pdf_path = tmp_path / "text_only.pdf"
    doc = pymupdf.open()
    page = doc.new_page(width=200, height=100)
    page.insert_text((10, 50), "Hello world")
    doc.save(str(pdf_path))
    doc.close()
    return pdf_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assert_valid_jp2(path: Path, expected_size: tuple[int, int] | None = None):
    """Open the file and verify it is a valid JP2 with expected dimensions."""
    assert path.exists(), f"Expected output file {path} to exist"
    with Image.open(str(path)) as img:
        assert img.format == "JPEG2000"
        if expected_size is not None:
            assert img.size == expected_size


# ---------------------------------------------------------------------------
# Tests: convert_image_to_jp2
# ---------------------------------------------------------------------------


@requires_opj_compress
class TestConvertImageToJp2:

    def test_convert_tif_to_jp2(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_image_to_jp2(tif_image, target)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        _assert_valid_jp2(target, (IMG_WIDTH, IMG_HEIGHT))

    def test_convert_png_to_jp2(self, png_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_image_to_jp2(png_image, target)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        _assert_valid_jp2(target, (IMG_WIDTH, IMG_HEIGHT))

    def test_convert_jpg_to_jp2(self, jpg_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_image_to_jp2(jpg_image, target)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        _assert_valid_jp2(target, (IMG_WIDTH, IMG_HEIGHT))

    def test_dry_run(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_image_to_jp2(tif_image, target, dry_run=True)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        assert not target.exists()

    def test_accepts_path_objects(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        assert isinstance(tif_image, Path)
        assert isinstance(target, Path)
        result = convert_image_to_jp2(tif_image, target)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        _assert_valid_jp2(target)

    def test_nonexistent_source(self, tmp_path):
        source = tmp_path / "does_not_exist.tif"
        target = tmp_path / "output.jp2"
        with pytest.raises(FileNotFoundError):
            convert_image_to_jp2(source, target)

    def test_lossless_tif(self, noisy_image, tmp_path):
        """Verify opj_compress produces pixel-identical output for TIF."""
        source = tmp_path / "noisy.tif"
        noisy_image.save(str(source), format="TIFF")
        target = tmp_path / "noisy.jp2"
        convert_image_to_jp2(source, target)
        src_pixels = np.array(noisy_image)
        with Image.open(str(target)) as dst:
            dst_pixels = np.array(dst)
        assert src_pixels.shape == dst_pixels.shape
        assert np.array_equal(src_pixels, dst_pixels), (
            f"Pixel mismatch — max diff: "
            f"{np.abs(src_pixels.astype(int) - dst_pixels.astype(int)).max()}"
        )

    def test_lossless_png(self, noisy_image, tmp_path):
        """Verify opj_compress produces pixel-identical output for PNG."""
        source = tmp_path / "noisy.png"
        noisy_image.save(str(source), format="PNG")
        target = tmp_path / "noisy.jp2"
        convert_image_to_jp2(source, target)
        src_pixels = np.array(noisy_image)
        with Image.open(str(target)) as dst:
            dst_pixels = np.array(dst)
        assert np.array_equal(src_pixels, dst_pixels), (
            f"Pixel mismatch — max diff: "
            f"{np.abs(src_pixels.astype(int) - dst_pixels.astype(int)).max()}"
        )

    def test_lossless_jpg(self, noisy_image, tmp_path):
        """Verify opj_compress produces pixel-identical output for JPG.

        Note: JPEG is lossy, so we compare the decoded JPEG pixels (not the
        original) against the JP2 output — the TIF→JP2 step must be lossless.
        """
        source = tmp_path / "noisy.jpg"
        noisy_image.save(str(source), format="JPEG", quality=95)
        # Re-read the JPEG to get the lossy-decoded pixels as ground truth
        with Image.open(str(source)) as jpg:
            jpg_pixels = np.array(jpg)
        target = tmp_path / "noisy.jp2"
        convert_image_to_jp2(source, target)
        with Image.open(str(target)) as dst:
            dst_pixels = np.array(dst)
        assert jpg_pixels.shape == dst_pixels.shape
        assert np.array_equal(jpg_pixels, dst_pixels), (
            f"Pixel mismatch — max diff: "
            f"{np.abs(jpg_pixels.astype(int) - dst_pixels.astype(int)).max()}"
        )


# ---------------------------------------------------------------------------
# Tests: extract_pdf_page_to_jp2
# ---------------------------------------------------------------------------


@requires_opj_compress
class TestExtractPdfPageToJp2:

    def test_embedded_raster(self, pdf_with_image, tmp_path):
        target = tmp_path / "output.jp2"
        w, h, method = extract_pdf_page_to_jp2(pdf_with_image, 0, target)
        assert method == "embedded_raster"
        assert w > 0 and h > 0
        _assert_valid_jp2(target, (w, h))

    @requires_poppler
    def test_rasterize_fallback(self, pdf_no_images, tmp_path):
        target = tmp_path / "output.jp2"
        w, h, method = extract_pdf_page_to_jp2(pdf_no_images, 0, target)
        assert method == "rasterized"
        assert w > 0 and h > 0
        _assert_valid_jp2(target, (w, h))

    def test_dry_run(self, pdf_with_image, tmp_path):
        target = tmp_path / "output.jp2"
        w, h, method = extract_pdf_page_to_jp2(
            pdf_with_image, 0, target, dry_run=True
        )
        assert w > 0 and h > 0
        assert method in {"embedded_raster", "rasterized"}
        assert not target.exists()

    def test_expected_dimensions_match(self, pdf_with_image, tmp_path, caplog):
        target = tmp_path / "output.jp2"
        # First extract to learn actual dimensions
        w, h, _ = extract_pdf_page_to_jp2(pdf_with_image, 0, target)
        target2 = tmp_path / "output2.jp2"
        with caplog.at_level(logging.WARNING):
            extract_pdf_page_to_jp2(
                pdf_with_image, 0, target2, expected_dimensions=(w, h)
            )
        assert "Dimension mismatch" not in caplog.text

    def test_expected_dimensions_mismatch(self, pdf_with_image, tmp_path, caplog):
        target = tmp_path / "output.jp2"
        with caplog.at_level(logging.WARNING):
            extract_pdf_page_to_jp2(
                pdf_with_image, 0, target, expected_dimensions=(999, 999)
            )
        assert "Dimension mismatch" in caplog.text

    @requires_poppler
    def test_custom_fallback_dpi(self, pdf_no_images, tmp_path):
        target_300 = tmp_path / "out_300.jp2"
        w_300, h_300, _ = extract_pdf_page_to_jp2(
            pdf_no_images, 0, target_300, fallback_dpi=300
        )
        target_150 = tmp_path / "out_150.jp2"
        w_150, h_150, _ = extract_pdf_page_to_jp2(
            pdf_no_images, 0, target_150, fallback_dpi=150
        )
        # 150 DPI should produce roughly half the dimensions of 300 DPI
        assert w_150 < w_300
        assert h_150 < h_300

    def test_page_num_zero_indexed(self, pdf_with_image, tmp_path):
        target = tmp_path / "output.jp2"
        w, h, method = extract_pdf_page_to_jp2(pdf_with_image, 0, target)
        assert w > 0 and h > 0
        _assert_valid_jp2(target)


# ---------------------------------------------------------------------------
# Tests: copy_jp2
# ---------------------------------------------------------------------------


class TestCopyJp2:

    def test_basic_copy(self, jp2_image, tmp_path):
        target = tmp_path / "copied.jp2"
        result = copy_jp2(jp2_image, target)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        assert target.exists()
        assert filecmp.cmp(str(jp2_image), str(target), shallow=False)

    def test_dry_run(self, jp2_image, tmp_path):
        target = tmp_path / "copied.jp2"
        result = copy_jp2(jp2_image, target, dry_run=True)
        assert result == (IMG_WIDTH, IMG_HEIGHT)
        assert not target.exists()

    def test_preserves_metadata(self, jp2_image, tmp_path):
        # Set a known mtime on source
        os.utime(str(jp2_image), (1000000, 1000000))
        target = tmp_path / "copied.jp2"
        copy_jp2(jp2_image, target)
        source_stat = os.stat(str(jp2_image))
        target_stat = os.stat(str(target))
        assert abs(source_stat.st_mtime - target_stat.st_mtime) < 1.0

    def test_nonexistent_source(self, tmp_path):
        source = tmp_path / "does_not_exist.jp2"
        target = tmp_path / "copied.jp2"
        with pytest.raises(FileNotFoundError):
            copy_jp2(source, target)


# ---------------------------------------------------------------------------
# Tests: convert_to_jp2 (dispatcher)
# ---------------------------------------------------------------------------


@requires_opj_compress
class TestConvertToJp2:

    def test_tif(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(tif_image, target, source_format="tif")
        assert result["converted"] is True
        assert result["method"] == "opj_compress"
        assert result["source_format"] == "tif"
        assert result["width"] == IMG_WIDTH
        assert result["height"] == IMG_HEIGHT
        _assert_valid_jp2(target, (IMG_WIDTH, IMG_HEIGHT))

    def test_tiff(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(tif_image, target, source_format="tiff")
        assert result["converted"] is True
        assert result["method"] == "opj_compress"
        assert result["source_format"] == "tiff"

    def test_png(self, png_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(png_image, target, source_format="png")
        assert result["converted"] is True
        assert result["method"] == "opj_compress"
        assert result["source_format"] == "png"
        _assert_valid_jp2(target, (IMG_WIDTH, IMG_HEIGHT))

    def test_jpg(self, jpg_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(jpg_image, target, source_format="jpg")
        assert result["converted"] is True
        assert result["method"] == "opj_compress"
        assert result["source_format"] == "jpg"
        _assert_valid_jp2(target, (IMG_WIDTH, IMG_HEIGHT))

    def test_jpeg(self, jpg_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(jpg_image, target, source_format="jpeg")
        assert result["converted"] is True
        assert result["source_format"] == "jpeg"

    def test_pdf(self, pdf_with_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(
            pdf_with_image, target, source_format="pdf", page_num=0
        )
        assert result["converted"] is True
        assert result["method"] in {"embedded_raster", "rasterized"}
        assert result["source_format"] == "pdf"
        _assert_valid_jp2(target)

    def test_jp2(self, jp2_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(jp2_image, target, source_format="jp2")
        assert result["converted"] is False
        assert result["method"] == "copy"
        assert result["source_format"] == "jp2"
        assert result["width"] == IMG_WIDTH
        assert result["height"] == IMG_HEIGHT
        assert filecmp.cmp(str(jp2_image), str(target), shallow=False)

    def test_pdf_missing_page_num(self, pdf_with_image, tmp_path):
        target = tmp_path / "output.jp2"
        with pytest.raises(ValueError, match="page_num is required"):
            convert_to_jp2(pdf_with_image, target, source_format="pdf")

    def test_unsupported_format(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        with pytest.raises(ValueError, match="Unsupported source format"):
            convert_to_jp2(tif_image, target, source_format="bmp")

    def test_format_case_insensitive(self, tif_image, tmp_path):
        for fmt in ("TIF", "Tif", "TiF"):
            target = tmp_path / f"output_{fmt}.jp2"
            result = convert_to_jp2(tif_image, target, source_format=fmt)
            assert result["source_format"] == "tif"
            _assert_valid_jp2(target)

    def test_format_whitespace_stripped(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(tif_image, target, source_format=" tif ")
        assert result["source_format"] == "tif"
        _assert_valid_jp2(target)

    def test_creates_target_directory(self, tif_image, tmp_path):
        target = tmp_path / "subdir" / "deep" / "output.jp2"
        result = convert_to_jp2(tif_image, target, source_format="tif")
        assert result["converted"] is True
        assert target.exists()

    def test_dry_run_no_directory_created(self, tif_image, tmp_path):
        subdir = tmp_path / "newdir"
        target = subdir / "output.jp2"
        result = convert_to_jp2(
            tif_image, target, source_format="tif", dry_run=True
        )
        assert result["width"] == IMG_WIDTH
        assert not subdir.exists()

    def test_return_dict_keys(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(tif_image, target, source_format="tif")
        assert set(result.keys()) == {
            "width", "height", "converted", "method", "source_format"
        }

    def test_dry_run_returns_correct_values(self, tif_image, tmp_path):
        target = tmp_path / "output.jp2"
        result = convert_to_jp2(
            tif_image, target, source_format="tif", dry_run=True
        )
        assert result["width"] == IMG_WIDTH
        assert result["height"] == IMG_HEIGHT
        assert result["converted"] is True
        assert result["method"] == "opj_compress"
        assert result["source_format"] == "tif"
        assert not target.exists()


# ---------------------------------------------------------------------------
# Tests: write_renaming_info
# ---------------------------------------------------------------------------


class TestWriteRenamingInfo:

    @pytest.fixture
    def sample_issue(self):
        return IssueRecord(
            alias="TEST",
            date=date(1900, 1, 1),
            edition="a",
            local_path="/PROV/TEST/1900/01/01",
            imgs_subdir="images",
            imgs_ext=".tif",
        )

    @pytest.fixture
    def sample_pages(self, tmp_path):
        """Create 3 mock PageFile objects with real source paths."""
        pages = []
        for i in (1, 2, 3):
            src = tmp_path / "source" / f"0000000{i}.tif"
            src.parent.mkdir(parents=True, exist_ok=True)
            src.touch()
            pages.append(PageFile(source_path=src, page_num=i, source_format="tif"))
        return pages

    @pytest.fixture
    def sample_page_results(self):
        return {
            1: {"width": 100, "height": 80, "converted": True, "method": "pillow", "source_format": "tif"},
            2: {"width": 200, "height": 160, "converted": True, "method": "pillow", "source_format": "tif"},
            3: {"width": 300, "height": 240, "converted": True, "method": "pillow", "source_format": "tif"},
        }

    def test_returns_correct_structure(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        result = write_renaming_info(
            sample_issue, sample_pages, sample_page_results,
            target_base_dir=str(tmp_path / "target"),
            source_base_dir="/mnt/original",
            dry_run=True,
        )
        expected_fields = {
            "original_filename", "new_filename", "issue_id",
            "img_dir_path", "ocr_dir_path", "width", "height",
        }
        assert set(result.keys()) == {"1", "2", "3"}
        for key, entry in result.items():
            assert set(entry.keys()) == expected_fields

        # Spot-check page 1
        assert result["1"]["original_filename"] == "00000001.tif"
        assert result["1"]["new_filename"] == "TEST-1900-01-01-a-p0001.jp2"
        assert result["1"]["issue_id"] == "TEST-1900-01-01-a"
        assert result["1"]["width"] == 100
        assert result["1"]["height"] == 80

    def test_writes_json_file(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        target_base = tmp_path / "target"
        # Create the target directory (normally done by convert_to_jp2)
        target_dir = build_target_path(
            str(target_base), sample_issue, 1
        ).parent
        target_dir.mkdir(parents=True)

        result = write_renaming_info(
            sample_issue, sample_pages, sample_page_results,
            target_base_dir=str(target_base),
            source_base_dir="/mnt/original",
            dry_run=False,
        )

        info_path = target_dir / RENAMING_INFO_FILENAME
        assert info_path.exists()
        with open(info_path, "r", encoding="utf-8") as f:
            on_disk = json.load(f)
        assert on_disk == result

    def test_dry_run_no_file(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        target_base = tmp_path / "target"
        result = write_renaming_info(
            sample_issue, sample_pages, sample_page_results,
            target_base_dir=str(target_base),
            source_base_dir="/mnt/original",
            dry_run=True,
        )
        # Dict is returned but no file created
        assert len(result) == 3
        target_dir = build_target_path(
            str(target_base), sample_issue, 1
        ).parent
        assert not (target_dir / RENAMING_INFO_FILENAME).exists()

    def test_multiple_pages_ordering(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        result = write_renaming_info(
            sample_issue, sample_pages, sample_page_results,
            target_base_dir=str(tmp_path / "target"),
            source_base_dir="/mnt/original",
            dry_run=True,
        )
        assert "1" in result
        assert "2" in result
        assert "3" in result
        assert result["2"]["width"] == 200
        assert result["3"]["height"] == 240

    def test_ocr_dir_path_no_imgs_subdir(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        # sample_issue has imgs_subdir="images" — ocr_dir_path should NOT include it
        result = write_renaming_info(
            sample_issue, sample_pages, sample_page_results,
            target_base_dir=str(tmp_path / "target"),
            source_base_dir="/mnt/original",
            dry_run=True,
        )
        expected_ocr = "/mnt/original/PROV/TEST/1900/01/01"
        assert result["1"]["ocr_dir_path"] == expected_ocr

    def test_write_failure_raises(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        target_base = tmp_path / "target"
        target_dir = build_target_path(
            str(target_base), sample_issue, 1
        ).parent
        target_dir.mkdir(parents=True)

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                write_renaming_info(
                    sample_issue, sample_pages, sample_page_results,
                    target_base_dir=str(target_base),
                    source_base_dir="/mnt/original",
                    dry_run=False,
                )

    def test_no_original_nlp_field(
        self, sample_issue, sample_pages, sample_page_results, tmp_path
    ):
        result = write_renaming_info(
            sample_issue, sample_pages, sample_page_results,
            target_base_dir=str(tmp_path / "target"),
            source_base_dir="/mnt/original",
            dry_run=True,
        )
        for entry in result.values():
            assert "original_nlp" not in entry
