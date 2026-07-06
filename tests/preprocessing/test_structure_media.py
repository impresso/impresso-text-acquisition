"""Tests for media conversion utilities in structure_media.py."""

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

from text_preparation.importer_scripts.preprocessing.structure_media import (
    DEFAULT_JP2_COMPRESSION_RATIO,
    RENAMING_INFO_FILENAME,
    AudioRecord,
    Config,
    IssueRecord,
    PageFile,
    ReportWriter,
    augment_index_entry_audio,
    build_target_path,
    convert_audio,
    convert_image_to_jp2,
    convert_to_jp2,
    copy_audio,
    copy_jp2,
    discover_audio_records,
    extract_pdf_page_to_jp2,
    write_issue_index,
    write_renaming_info,
    write_renaming_info_audio,
    _read_audio_duration,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IMG_WIDTH = 200
IMG_HEIGHT = 160

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
    """Create a fresh PIL Image with high-entropy pixel data (not saved to disk).

    Uses random pixels rather than a solid color so the lossless JP2 produced
    by opj_compress clears the MIN_VALID_JP2_BYTES (5000) validity guard — a
    flat-color image compresses to a few hundred bytes and would be (correctly)
    rejected as an implausible stub, which is unrealistic for a real scan.
    """
    rng = np.random.RandomState(7)
    arr = rng.randint(0, 256, (IMG_HEIGHT, IMG_WIDTH, 3), dtype=np.uint8)
    return Image.fromarray(arr)


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
        convert_image_to_jp2(source, target, compression_ratio=1)
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
        convert_image_to_jp2(source, target, compression_ratio=1)
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
        convert_image_to_jp2(source, target, compression_ratio=1)
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


@pytest.mark.xfail(
    reason="PDF extraction is a documented TODO (progress.md Step 4): "
    "extract_pdf_page_to_jp2 raises NotImplementedError until implemented. "
    "strict=True flips these to XPASS once the feature lands, prompting "
    "removal of this marker.",
    raises=NotImplementedError,
    strict=True,
)
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

    @pytest.mark.xfail(
        reason="PDF extraction is a documented TODO (progress.md Step 4): "
        "convert_to_jp2 dispatches PDF to extract_pdf_page_to_jp2, which "
        "raises NotImplementedError until implemented.",
        raises=NotImplementedError,
        strict=True,
    )
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
            local_path=["/PROV/TEST/1900/01/01"],
            imgs_subdir="images",
            ext=".tif",
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
        expected_ocr = ["PROV/TEST/1900/01/01"]
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


# ---------------------------------------------------------------------------
# Audio fixtures & helpers
# ---------------------------------------------------------------------------

# Minimal valid MPEG-1 Layer III frame: 128 kbps, 44100 Hz, mono => 417 bytes,
# 1152 samples each. mutagen derives the duration from the frame headers.
_MP3_FRAME = bytes([0xFF, 0xFB, 0x90, 0xC0]) + b"\x00" * (417 - 4)


def _write_mp3(path: Path, n_frames: int = 80) -> Path:
    """Write a silent CBR MP3 of ``n_frames`` frames (~0.026 s each).

    80 frames ≈ 2.09 s -> duration string "00:00:02".
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_MP3_FRAME * n_frames)
    return path


@pytest.fixture
def mp3_file(tmp_path):
    return _write_mp3(tmp_path / "src" / "rec_one.mp3", n_frames=80)


# ---------------------------------------------------------------------------
# Tests: IssueRecord.media_type
# ---------------------------------------------------------------------------


class TestMediaType:

    def _issue(self, ext, local_path=None):
        return IssueRecord(
            alias="x", date=date(1996, 12, 16), edition="a",
            local_path=local_path or ["p"], imgs_subdir="", ext=ext,
        )

    @pytest.mark.parametrize("ext", [".mp3", ".MP3", ".wav", ".flac", ".m4a", ".ogg", ".aac"])
    def test_audio_extensions(self, ext):
        assert self._issue(ext).media_type == "audio"

    @pytest.mark.parametrize("ext", [".tif", ".jp2", ".png", ".jpg", ".jpeg", ".pdf"])
    def test_image_extensions(self, ext):
        assert self._issue(ext).media_type == "image"


# ---------------------------------------------------------------------------
# Tests: discover_audio_records
# ---------------------------------------------------------------------------


class TestDiscoverAudioRecords:

    def _audio_issue(self, local_path, ext=".mp3"):
        return IssueRecord(
            alias="rts", date=date(1996, 12, 16), edition="a",
            local_path=local_path, imgs_subdir="", ext=ext,
        )

    def test_single_file(self, tmp_path):
        _write_mp3(tmp_path / "a" / "one.mp3")
        issue = self._audio_issue(["a/one.mp3"])
        records = discover_audio_records(issue, str(tmp_path))
        assert len(records) == 1
        assert records[0].record_num == 1
        assert records[0].source_format == "mp3"
        assert records[0].source_path == tmp_path / "a" / "one.mp3"

    def test_multiple_files_numbered_by_list_order(self, tmp_path):
        # Deliberately NOT alphabetical: list order must win, no re-sort.
        _write_mp3(tmp_path / "a" / "zzz.mp3")
        _write_mp3(tmp_path / "a" / "aaa.mp3")
        issue = self._audio_issue(["a/zzz.mp3", "a/aaa.mp3"])
        records = discover_audio_records(issue, str(tmp_path))
        assert [r.record_num for r in records] == [1, 2]
        assert records[0].source_path.name == "zzz.mp3"  # entry 0 -> r0001
        assert records[1].source_path.name == "aaa.mp3"  # entry 1 -> r0002

    def test_missing_file_raises(self, tmp_path):
        _write_mp3(tmp_path / "a" / "one.mp3")
        issue = self._audio_issue(["a/one.mp3", "a/missing.mp3"])
        with pytest.raises(FileNotFoundError, match="missing.mp3"):
            discover_audio_records(issue, str(tmp_path))

    def test_leading_slash_stripped(self, tmp_path):
        _write_mp3(tmp_path / "a" / "one.mp3")
        issue = self._audio_issue(["/a/one.mp3"])
        records = discover_audio_records(issue, str(tmp_path))
        assert len(records) == 1

    def test_multi_record_warning(self, tmp_path, caplog):
        _write_mp3(tmp_path / "a" / "one.mp3")
        _write_mp3(tmp_path / "a" / "two.mp3")
        issue = self._audio_issue(["a/one.mp3", "a/two.mp3"])
        with caplog.at_level(logging.WARNING):
            discover_audio_records(issue, str(tmp_path))
        assert "2 audio records" in caplog.text

    def test_extension_mismatch_warning(self, tmp_path, caplog):
        _write_mp3(tmp_path / "a" / "one.wav")  # file is .wav but declared .mp3
        issue = self._audio_issue(["a/one.wav"], ext=".mp3")
        with caplog.at_level(logging.WARNING):
            records = discover_audio_records(issue, str(tmp_path))
        assert "differs from declared ext" in caplog.text
        assert records[0].source_format == "wav"


# ---------------------------------------------------------------------------
# Tests: build_target_path (audio)
# ---------------------------------------------------------------------------


class TestBuildTargetPathAudio:

    def test_audio_record_filename(self):
        issue = IssueRecord(
            alias="ana_media", date=date(1996, 12, 16), edition="a",
            local_path=["RTS/x.mp3"], imgs_subdir="", ext=".mp3",
        )
        path = build_target_path("/T", issue, 1)
        assert path.as_posix() == "/T/ana_media/1996/12/16/a/ana_media-1996-12-16-a-r0001.mp3"

    def test_audio_record_zero_padded(self):
        issue = IssueRecord(
            alias="ana_media", date=date(1996, 12, 16), edition="a",
            local_path=["RTS/x.mp3"], imgs_subdir="", ext=".mp3",
        )
        assert build_target_path("/T", issue, 42).name == "ana_media-1996-12-16-a-r0042.mp3"

    def test_image_still_p_jp2(self):
        # Backward-compat: image issues keep -pNNNN.jp2 from a positional call.
        issue = IssueRecord(
            alias="TEST", date=date(1900, 1, 1), edition="a",
            local_path=["X"], imgs_subdir="", ext=".tif",
        )
        assert build_target_path("/T", issue, 1).name == "TEST-1900-01-01-a-p0001.jp2"


# ---------------------------------------------------------------------------
# Tests: _read_audio_duration / copy_audio / convert_audio
# ---------------------------------------------------------------------------


class TestReadAudioDuration:

    def test_hms_format(self, mp3_file):
        # 80 frames ≈ 2.09 s
        assert _read_audio_duration(mp3_file) == "00:00:02"

    def test_longer_file(self, tmp_path):
        # 460 frames * (1152/44100) s ≈ 12.01 s, but mutagen estimates from
        # bitrate (128 kbps CBR) -> ~11.99 s, truncated by gmtime to 11 s.
        path = _write_mp3(tmp_path / "long.mp3", n_frames=460)
        assert _read_audio_duration(path) == "00:00:11"


class TestCopyAudio:

    def test_basic_copy_returns_duration(self, mp3_file, tmp_path):
        target = tmp_path / "out" / "rec.mp3"
        target.parent.mkdir(parents=True)
        duration = copy_audio(mp3_file, target)
        assert duration == "00:00:02"
        assert target.exists()
        assert filecmp.cmp(str(mp3_file), str(target), shallow=False)

    def test_dry_run_no_file(self, mp3_file, tmp_path):
        target = tmp_path / "out" / "rec.mp3"
        duration = copy_audio(mp3_file, target, dry_run=True)
        assert duration == "00:00:02"
        assert not target.exists()


class TestConvertAudio:

    def test_mp3_copy(self, mp3_file, tmp_path):
        target = tmp_path / "out" / "rec.mp3"
        result = convert_audio(mp3_file, target, source_format="mp3")
        assert result == {
            "duration": "00:00:02",
            "converted": False,
            "method": "copy",
            "source_format": "mp3",
        }
        assert filecmp.cmp(str(mp3_file), str(target), shallow=False)

    def test_creates_target_directory(self, mp3_file, tmp_path):
        target = tmp_path / "deep" / "nested" / "rec.mp3"
        convert_audio(mp3_file, target, source_format="mp3")
        assert target.exists()

    @pytest.mark.parametrize("fmt", ["wav", "flac", "m4a", "ogg", "aac"])
    def test_non_mp3_raises_not_implemented(self, fmt, tmp_path):
        with pytest.raises(NotImplementedError, match="not implemented"):
            convert_audio(tmp_path / f"x.{fmt}", tmp_path / "out.mp3", source_format=fmt)

    def test_dry_run_no_file(self, mp3_file, tmp_path):
        target = tmp_path / "out" / "rec.mp3"
        result = convert_audio(mp3_file, target, source_format="mp3", dry_run=True)
        assert result["duration"] == "00:00:02"
        assert not target.exists()


# ---------------------------------------------------------------------------
# Tests: write_renaming_info_audio
# ---------------------------------------------------------------------------


class TestWriteRenamingInfoAudio:

    @pytest.fixture
    def audio_issue(self):
        return IssueRecord(
            alias="ana_media", date=date(1996, 12, 16), edition="a",
            local_path=["RTS/ana_media/audio/a.mp3", "RTS/ana_media/audio/b.mp3"],
            imgs_subdir="", ext=".mp3",
        )

    @pytest.fixture
    def audio_records(self, tmp_path):
        records = []
        for i, name in ((1, "a.mp3"), (2, "b.mp3")):
            src = tmp_path / "src" / name
            src.parent.mkdir(parents=True, exist_ok=True)
            src.touch()
            records.append(AudioRecord(source_path=src, record_num=i, source_format="mp3"))
        return records

    @pytest.fixture
    def record_results(self):
        return {
            1: {"duration": "00:00:02", "converted": False, "method": "copy", "source_format": "mp3"},
            2: {"duration": "00:00:03", "converted": False, "method": "copy", "source_format": "mp3"},
        }

    def test_structure(self, audio_issue, audio_records, record_results, tmp_path):
        result = write_renaming_info_audio(
            audio_issue, audio_records, record_results,
            target_base_dir=str(tmp_path / "target"),
            source_base_dir="/mnt/original",
            dry_run=True,
        )
        assert set(result.keys()) == {"1", "2"}
        expected_fields = {
            "original_filename", "new_filename", "issue_id",
            "record_dir_path", "src_path", "duration",
        }
        for entry in result.values():
            assert set(entry.keys()) == expected_fields
        assert result["1"]["new_filename"] == "ana_media-1996-12-16-a-r0001.mp3"
        assert result["1"]["record_dir_path"] == "ana_media/1996/12/16/a"
        assert result["1"]["duration"] == "00:00:02"
        assert result["2"]["duration"] == "00:00:03"
        assert result["1"]["src_path"] == [
            "RTS/ana_media/audio/a.mp3", "RTS/ana_media/audio/b.mp3",
        ]
        # No image-only keys leak in.
        assert "width" not in result["1"]
        assert "img_dir_path" not in result["1"]

    def test_writes_json_file(self, audio_issue, audio_records, record_results, tmp_path):
        target_base = tmp_path / "target"
        target_dir = build_target_path(str(target_base), audio_issue, 1).parent
        target_dir.mkdir(parents=True)
        result = write_renaming_info_audio(
            audio_issue, audio_records, record_results,
            target_base_dir=str(target_base),
            source_base_dir="/mnt/original",
            dry_run=False,
        )
        info_path = target_dir / RENAMING_INFO_FILENAME
        assert info_path.exists()
        with open(info_path, "r", encoding="utf-8") as f:
            assert json.load(f) == result


# ---------------------------------------------------------------------------
# Tests: augment_index_entry_audio
# ---------------------------------------------------------------------------


class TestAugmentIndexEntryAudio:

    def test_audio_native_keys(self, tmp_path):
        issue = IssueRecord(
            alias="ana_media", date=date(1996, 12, 16), edition="a",
            local_path=["RTS/a.mp3"], imgs_subdir="", ext=".mp3",
        )
        index = {
            "ana_media": {"1996": {"12": [
                {"day": "16", "edition": "a", "local_path": ["RTS/a.mp3"], "ext": ".mp3"}
            ]}}
        }
        src = tmp_path / "a.mp3"
        src.touch()
        records = [AudioRecord(source_path=src, record_num=1, source_format="mp3")]
        results = {1: {"duration": "00:00:02"}}
        augment_index_entry_audio(index, issue, records, results, str(tmp_path / "target"))

        entry = index["ana_media"]["1996"]["12"][0]
        assert entry["issue_id"] == "ana_media-1996-12-16-a"
        assert entry["num_records"] == 1
        assert entry["record_dir_path"] == "ana_media/1996/12/16/a"
        assert entry["records"] == [
            {"record_num": 1, "original_filename": "a.mp3", "duration": "00:00:02"}
        ]
        assert "num_pages" not in entry
        assert "pages" not in entry


# ---------------------------------------------------------------------------
# Tests: ReportWriter
# ---------------------------------------------------------------------------


def _seed_prior_report(
    dir_path: Path,
    successes: list[str] | None = None,
    failures: list[dict] | None = None,
) -> Path:
    """Create a prior report directory with the given entries."""
    dir_path.mkdir(parents=True, exist_ok=True)
    if successes:
        with open(dir_path / ReportWriter.SUCCESS_FILENAME, "w") as f:
            for sid in successes:
                f.write(sid + "\n")
    if failures:
        with open(dir_path / ReportWriter.FAILED_FILENAME, "w") as f:
            for entry in failures:
                f.write(json.dumps(entry) + "\n")
    return dir_path


def _read_success(dir_path: Path) -> list[str]:
    p = dir_path / ReportWriter.SUCCESS_FILENAME
    if not p.exists():
        return []
    return [line.strip() for line in p.read_text().splitlines() if line.strip()]


def _read_failed(dir_path: Path) -> list[dict]:
    p = dir_path / ReportWriter.FAILED_FILENAME
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


class TestReportWriter:

    def test_fresh_run_writes_outcomes_only(self, tmp_path):
        report_dir = tmp_path / "report"
        with ReportWriter(report_dir=str(report_dir)) as r:
            r.prepare({"issue-a", "issue-b"})
            r.write_success("issue-a")
            r.write_failure("issue-b", num_pages=2, pages_ok=1, errors=[
                {"page": 2, "error": "boom"},
            ])
        assert _read_success(report_dir) == ["issue-a"]
        failed = _read_failed(report_dir)
        assert len(failed) == 1
        assert failed[0]["issue_id"] == "issue-b"
        assert failed[0]["failed_pages"] == [2]

    def test_retry_only_without_prior_raises(self, tmp_path):
        report_dir = tmp_path / "report"
        with ReportWriter(
            report_dir=str(report_dir),
            retry_failed_only=True,
        ) as r:
            with pytest.raises(RuntimeError, match="no prior failures loaded"):
                r.prepare({"issue-a"})

    def test_resume_different_dir_carries_forward_in_scope_only(self, tmp_path):
        prior = _seed_prior_report(
            tmp_path / "prior",
            successes=["in-1", "in-2", "in-3", "out-1", "out-2"],
        )
        report_dir = tmp_path / "report"
        scope = {"in-1", "in-2", "in-3", "new-1"}
        with ReportWriter(
            report_dir=str(report_dir),
            prior_report_dir=str(prior),
        ) as r:
            r.prepare(scope)
            r.write_success("new-1")
        # In-scope priors carried forward; out-of-scope dropped; new ones appended.
        assert sorted(_read_success(report_dir)) == ["in-1", "in-2", "in-3", "new-1"]
        # Prior dir untouched.
        assert sorted(_read_success(prior)) == [
            "in-1", "in-2", "in-3", "out-1", "out-2",
        ]

    def test_retry_only_no_stale_failures(self, tmp_path):
        prior = _seed_prior_report(
            tmp_path / "prior",
            failures=[
                {"issue_id": "fail-1", "status": "failed", "num_pages": 3,
                 "pages_ok": 0, "failed_pages": [1, 2, 3], "errors": [],
                 "timestamp": "2025-01-01T00:00:00"},
            ],
        )
        report_dir = tmp_path / "report"
        with ReportWriter(
            report_dir=str(report_dir),
            prior_report_dir=str(prior),
            retry_failed_only=True,
        ) as r:
            r.prepare({"fail-1"})
            r.write_success("fail-1")  # retry succeeds
        assert _read_success(report_dir) == ["fail-1"]
        # No stale failure row carried forward.
        assert _read_failed(report_dir) == []

    def test_resume_same_dir_preserves_prior_successes(self, tmp_path):
        report_dir = _seed_prior_report(
            tmp_path / "report",
            successes=["done-1", "done-2"],
        )
        scope = {"done-1", "done-2", "new-1"}
        with ReportWriter(
            report_dir=str(report_dir),
            prior_report_dir=str(report_dir),
        ) as r:
            r.prepare(scope)
            r.write_success("new-1")
        # Prior in-scope successes preserved; new outcomes appended.
        assert sorted(_read_success(report_dir)) == ["done-1", "done-2", "new-1"]

    def test_dry_run_no_files_written(self, tmp_path):
        report_dir = tmp_path / "report"
        with ReportWriter(report_dir=str(report_dir), dry_run=True) as r:
            r.prepare({"issue-a"})
            r.write_success("issue-a")
            r.write_failure("issue-b", 2, 1, [{"page": 2, "error": "x"}])
        # No directory or files should have been created.
        assert not report_dir.exists()

    def test_refuse_clobber_when_report_dir_nonempty_no_prior(self, tmp_path):
        report_dir = _seed_prior_report(
            tmp_path / "report",
            successes=["existing"],
        )
        with ReportWriter(report_dir=str(report_dir)) as r:
            with pytest.raises(RuntimeError, match="non-empty success.txt"):
                r.prepare({"new-1"})
        # File untouched.
        assert _read_success(report_dir) == ["existing"]

    def test_no_clobber_check_when_prior_matches(self, tmp_path):
        report_dir = _seed_prior_report(
            tmp_path / "report",
            successes=["existing"],
        )
        with ReportWriter(
            report_dir=str(report_dir),
            prior_report_dir=str(report_dir),
        ) as r:
            r.prepare({"existing", "new-1"})  # no raise
            r.write_success("new-1")
        assert sorted(_read_success(report_dir)) == ["existing", "new-1"]

    def test_prepare_called_twice_raises(self, tmp_path):
        report_dir = tmp_path / "report"
        with ReportWriter(report_dir=str(report_dir)) as r:
            r.prepare({"a"})
            with pytest.raises(RuntimeError, match="called twice"):
                r.prepare({"a"})

    def test_should_process_default_skips_prior_successes(self, tmp_path):
        prior = _seed_prior_report(
            tmp_path / "prior",
            successes=["done-1"],
            failures=[{"issue_id": "fail-1", "status": "failed",
                       "num_pages": 1, "pages_ok": 0, "failed_pages": [1],
                       "errors": [], "timestamp": "x"}],
        )
        r = ReportWriter(prior_report_dir=str(prior))
        assert r.should_process("done-1") is False
        assert r.should_process("fail-1") is True
        assert r.should_process("never-seen") is True

    def test_write_issue_index_full_run_canonical_name(self, tmp_path):
        write_issue_index({}, str(tmp_path), "sub", scope_suffix=None)
        assert (tmp_path / "issue_index" / "issue_index.sub.json").exists()
        assert not (tmp_path / "issue_index" / "issue_index.sub.partial.json").exists()
        assert not (tmp_path / "issue_index" / "issue_index.sub.sample.json").exists()

    def test_write_issue_index_sample_suffix(self, tmp_path):
        write_issue_index({}, str(tmp_path), "sub", scope_suffix="sample")
        assert (tmp_path / "issue_index" / "issue_index.sub.sample.json").exists()
        assert not (tmp_path / "issue_index" / "issue_index.sub.json").exists()

    def test_write_issue_index_partial_suffix(self, tmp_path):
        write_issue_index({}, str(tmp_path), "sub", scope_suffix="partial")
        assert (tmp_path / "issue_index" / "issue_index.sub.partial.json").exists()
        assert not (tmp_path / "issue_index" / "issue_index.sub.json").exists()

    def test_should_process_retry_only_filters_to_prior_failed(self, tmp_path):
        prior = _seed_prior_report(
            tmp_path / "prior",
            successes=["done-1"],
            failures=[{"issue_id": "fail-1", "status": "failed",
                       "num_pages": 1, "pages_ok": 0, "failed_pages": [1],
                       "errors": [], "timestamp": "x"}],
        )
        r = ReportWriter(
            prior_report_dir=str(prior),
            retry_failed_only=True,
        )
        assert r.should_process("done-1") is False
        assert r.should_process("fail-1") is True
        assert r.should_process("never-seen") is False


# ---------------------------------------------------------------------------
# Tests: Config
# ---------------------------------------------------------------------------
class TestConfig:

    def test_compression_ratio_default(self):
        cfg = Config(issues_json_path="x.json", target_base_dir="/tmp/out")
        assert cfg.compression_ratio == DEFAULT_JP2_COMPRESSION_RATIO == 10

    def test_compression_ratio_rejects_below_one(self):
        with pytest.raises(ValueError, match="compression_ratio"):
            Config(
                issues_json_path="x.json",
                target_base_dir="/tmp/out",
                compression_ratio=0,
            )
