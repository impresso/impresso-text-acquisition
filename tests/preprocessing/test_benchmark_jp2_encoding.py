"""Benchmark: Pillow vs opj_compress for lossless JPEG2000 encoding.

Run with:  pytest tests/preprocessing/test_benchmark_jp2_encoding.py -m benchmark -v -s
"""

import json
import os
import shutil
import statistics
import tempfile
import time
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from text_preparation.importer_scripts.preprocessing.structure_facsimiles import (
    _run_opj_compress,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ITERATIONS = 10
WARMUP = 1

IMAGE_CONFIGS = [
    ("small", 100, 80),
    ("medium", 1024, 1536),
    ("large", 2048, 3072),
]

SOURCE_FORMATS = ["tif", "png", "jpg"]

_PIL_FORMAT = {"tif": "TIFF", "png": "PNG", "jpg": "JPEG"}

requires_opj_compress = pytest.mark.skipif(
    shutil.which("opj_compress") is None,
    reason="opj_compress not installed (brew install openjpeg)",
)

# ---------------------------------------------------------------------------
# Encoding helpers
# ---------------------------------------------------------------------------


def _encode_pillow(source_path: Path, target_path: Path, fmt: str) -> None:
    """Replicate the old Pillow-based JP2 encoding pipeline."""
    with Image.open(str(source_path)) as img:
        img.load()
        img.save(str(target_path), format="JPEG2000", irreversible=False)


def _encode_opj(source_path: Path, target_path: Path, fmt: str) -> None:
    """Encode via opj_compress, with JPEG→temp-TIF step when needed."""
    if fmt == "jpg":
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with Image.open(str(source_path)) as img:
                img.save(tmp_path, format="TIFF")
            _run_opj_compress(tmp_path, target_path)
        finally:
            os.unlink(tmp_path)
    else:
        _run_opj_compress(source_path, target_path)


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------


def _time_encoder(encoder, source: Path, target_dir: Path, fmt: str,
                  iterations: int, warmup: int) -> dict:
    """Time *iterations* runs (after *warmup* discarded runs).

    Returns dict with mean, std, median, min, max (all in seconds).
    """
    times: list[float] = []
    for i in range(warmup + iterations):
        target = target_dir / f"out_{i}.jp2"
        t0 = time.perf_counter()
        encoder(source, target, fmt)
        t1 = time.perf_counter()
        if i >= warmup:
            times.append(t1 - t0)
        target.unlink(missing_ok=True)

    return {
        "mean": statistics.mean(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0.0,
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "times": times,
    }


# ---------------------------------------------------------------------------
# Session-scoped fixtures — generate source images once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def benchmark_images(tmp_path_factory) -> dict[tuple[str, str], Path]:
    """Create test images for all (size, format) combinations.

    Returns a dict mapping (size_label, format) -> Path.
    """
    base = tmp_path_factory.mktemp("bench_sources")
    rng = np.random.RandomState(42)
    images: dict[tuple[str, str], Path] = {}

    for label, w, h in IMAGE_CONFIGS:
        arr = rng.randint(0, 256, (h, w, 3), dtype=np.uint8)
        img = Image.fromarray(arr)
        for fmt in SOURCE_FORMATS:
            path = base / f"{label}.{fmt}"
            if fmt == "jpg":
                img.save(str(path), format="JPEG", quality=95)
            else:
                img.save(str(path), format=_PIL_FORMAT[fmt])
            images[(label, fmt)] = path

    return images


# ---------------------------------------------------------------------------
# Parametrize over all (size, format) combinations
# ---------------------------------------------------------------------------

_PARAMS = [
    (label, fmt)
    for label, _, _ in IMAGE_CONFIGS
    for fmt in SOURCE_FORMATS
]


def _param_id(val):
    return f"{val[0]}_{val[1]}"


# ---------------------------------------------------------------------------
# Benchmark test class
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
@requires_opj_compress
class TestJp2EncodingBenchmark:
    """Statistical comparison of Pillow vs opj_compress JP2 encoding."""

    @pytest.mark.parametrize("size_fmt", _PARAMS, ids=[f"{s}_{f}" for s, f in _PARAMS])
    def test_encoding_time(self, size_fmt, benchmark_images, tmp_path):
        """Measure encoding time for both methods and print comparison."""
        size_label, fmt = size_fmt
        source = benchmark_images[(size_label, fmt)]

        pillow_dir = tmp_path / "pillow"
        pillow_dir.mkdir()
        opj_dir = tmp_path / "opj"
        opj_dir.mkdir()

        pillow_stats = _time_encoder(
            _encode_pillow, source, pillow_dir, fmt, ITERATIONS, WARMUP
        )
        opj_stats = _time_encoder(
            _encode_opj, source, opj_dir, fmt, ITERATIONS, WARMUP
        )

        speedup = pillow_stats["mean"] / opj_stats["mean"] if opj_stats["mean"] > 0 else float("inf")
        faster = "opj_compress" if speedup > 1 else "Pillow"

        print(f"\n{'=' * 60}")
        print(f"Config: {size_label}_{fmt} (source: {source.name}, {os.path.getsize(source) / 1024:.1f} KB)")
        print(f"  Pillow:       mean={pillow_stats['mean']:.4f}s  std={pillow_stats['std']:.4f}s  "
              f"median={pillow_stats['median']:.4f}s  min={pillow_stats['min']:.4f}s  max={pillow_stats['max']:.4f}s")
        print(f"  opj_compress: mean={opj_stats['mean']:.4f}s  std={opj_stats['std']:.4f}s  "
              f"median={opj_stats['median']:.4f}s  min={opj_stats['min']:.4f}s  max={opj_stats['max']:.4f}s")
        print(f"  Speedup:      {speedup:.2f}x ({faster} is faster)")

    @pytest.mark.parametrize("size_fmt", _PARAMS, ids=[f"{s}_{f}" for s, f in _PARAMS])
    def test_output_size(self, size_fmt, benchmark_images, tmp_path):
        """Compare output JP2 file sizes between both methods."""
        size_label, fmt = size_fmt
        source = benchmark_images[(size_label, fmt)]

        pillow_out = tmp_path / "pillow.jp2"
        opj_out = tmp_path / "opj.jp2"

        _encode_pillow(source, pillow_out, fmt)
        _encode_opj(source, opj_out, fmt)

        psize = os.path.getsize(pillow_out)
        osize = os.path.getsize(opj_out)
        ratio = psize / osize if osize > 0 else float("inf")

        print(f"\n  File size ({size_label}_{fmt}): "
              f"Pillow={psize / 1024:.1f} KB  opj={osize / 1024:.1f} KB  "
              f"ratio={ratio:.3f}")

    @pytest.mark.parametrize("size_fmt", _PARAMS, ids=[f"{s}_{f}" for s, f in _PARAMS])
    def test_pixel_identity(self, size_fmt, benchmark_images, tmp_path):
        """Verify both methods produce pixel-identical lossless output."""
        size_label, fmt = size_fmt
        source = benchmark_images[(size_label, fmt)]

        pillow_out = tmp_path / "pillow.jp2"
        opj_out = tmp_path / "opj.jp2"

        _encode_pillow(source, pillow_out, fmt)
        _encode_opj(source, opj_out, fmt)

        with Image.open(str(pillow_out)) as p_img:
            p_pixels = np.array(p_img)
        with Image.open(str(opj_out)) as o_img:
            o_pixels = np.array(o_img)

        assert p_pixels.shape == o_pixels.shape, (
            f"Shape mismatch: Pillow {p_pixels.shape} vs opj {o_pixels.shape}"
        )
        assert np.array_equal(p_pixels, o_pixels), (
            f"Pixel mismatch ({size_label}_{fmt}) — "
            f"max diff: {np.abs(p_pixels.astype(int) - o_pixels.astype(int)).max()}"
        )

    def test_summary_report(self, benchmark_images, tmp_path):
        """Run full benchmark and write JSON + summary table."""
        results = {}

        for size_label, fmt in _PARAMS:
            key = f"{size_label}_{fmt}"
            source = benchmark_images[(size_label, fmt)]

            pillow_dir = tmp_path / f"pillow_{key}"
            pillow_dir.mkdir()
            opj_dir = tmp_path / f"opj_{key}"
            opj_dir.mkdir()

            pillow_stats = _time_encoder(
                _encode_pillow, source, pillow_dir, fmt, ITERATIONS, WARMUP
            )
            opj_stats = _time_encoder(
                _encode_opj, source, opj_dir, fmt, ITERATIONS, WARMUP
            )

            # File sizes (single measurement)
            p_out = tmp_path / f"size_p_{key}.jp2"
            o_out = tmp_path / f"size_o_{key}.jp2"
            _encode_pillow(source, p_out, fmt)
            _encode_opj(source, o_out, fmt)

            # Pixel identity
            with Image.open(str(p_out)) as p_img:
                p_px = np.array(p_img)
            with Image.open(str(o_out)) as o_img:
                o_px = np.array(o_img)
            pixels_match = bool(np.array_equal(p_px, o_px))

            speedup = pillow_stats["mean"] / opj_stats["mean"] if opj_stats["mean"] > 0 else 0
            results[key] = {
                "size_label": size_label,
                "format": fmt,
                "source_bytes": os.path.getsize(source),
                "pillow": {k: v for k, v in pillow_stats.items() if k != "times"},
                "opj_compress": {k: v for k, v in opj_stats.items() if k != "times"},
                "speedup": round(speedup, 3),
                "pillow_jp2_bytes": os.path.getsize(p_out),
                "opj_jp2_bytes": os.path.getsize(o_out),
                "pixels_identical": pixels_match,
            }

        # Write JSON
        json_path = (
            Path(__file__).resolve().parent / "benchmark_results.json"
        )
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        # Print summary table
        print("\n")
        print("=" * 90)
        print(f"{'Config':<16} {'Pillow (s)':>12} {'opj (s)':>12} {'Speedup':>9} "
              f"{'P size':>10} {'O size':>10} {'Pixels':>8}")
        print("-" * 90)
        for key, r in results.items():
            print(
                f"{key:<16} "
                f"{r['pillow']['mean']:>11.4f}s "
                f"{r['opj_compress']['mean']:>11.4f}s "
                f"{r['speedup']:>8.2f}x "
                f"{r['pillow_jp2_bytes'] / 1024:>9.1f}K "
                f"{r['opj_jp2_bytes'] / 1024:>9.1f}K "
                f"{'yes' if r['pixels_identical'] else 'NO':>8}"
            )
        print("=" * 90)
        print(f"\nResults written to: {json_path}")
