"""Shared constants for Chronicling America bulk downloads."""

from __future__ import annotations

import re

BASE_URL = "https://chroniclingamerica.loc.gov"
# LOC migrated OCR manifests in 2025: /ocr.json now 404s after redirect; the live
# manifest is at /data/ocr/ocr.json with a list-shaped schema (archive_name, batch).
OCR_JSON_URLS = (
    f"{BASE_URL}/data/ocr/ocr.json",
    f"{BASE_URL}/ocr.json",
)
BATCHES_URL = f"{BASE_URL}/data/batches/"

# Official LOC rate limits (https://www.loc.gov/apis/json-and-yaml/working-within-limits/):
# JSON/YAML API: 20 req/min; storage/text/image: 150 req/min; block time: 1 hour.
# chroniclingamerica.loc.gov bulk crawls are throttled similarly in practice; stay
# under the JSON API limit to avoid 429 / CAPTCHA blocks.
LOC_JSON_API_REQUESTS_PER_MINUTE = 20
LOC_RATE_LIMIT_BLOCK_SECONDS = 3600
DEFAULT_REQUEST_DELAY = 60.0 / LOC_JSON_API_REQUESTS_PER_MINUTE  # 3.0 s
# Stay well under the official 20/min ceiling; directory crawls trigger CAPTCHA
# faster than tarball or XML file downloads in practice.
DEFAULT_MAX_REQUESTS_PER_MINUTE = 8
# Extra minimum spacing for HTML directory listings under /data/batches/.
DEFAULT_DIRECTORY_DELAY = 6.0
# Pause after enumerating reels for one batch/LCCN before METS downloads.
BATCH_ENUMERATION_COOLDOWN = 120.0
# Pause between finishing one OCR tarball and starting the next.
TARBALL_COOLDOWN = 180.0
# After this many METS downloads, pause to avoid sustained crawl bursts.
METS_BURST_SIZE = 15
METS_BURST_PAUSE = 90.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ImpressoTextPreparation/1.0; "
        "+https://github.com/impresso/impresso-text-acquisition)"
    )
}

CHALLENGE_MARKERS = (
    "captcha",
    "just a moment",
    "challenge-platform",
    "cloudflare",
)

ISSUE_DIR_RE = re.compile(r"^\d{10}$")
TARBALL_BATCH_RE = re.compile(r"^batch_(.+)\.tar\.bz2$")
PLAIN_TARBALL_RE = re.compile(r"^(.+_ver\d+)\.tar\.bz2$")
BATCH_VERSION_RE = re.compile(r"^(.+)_ver(\d+)$")
# LCCNs are either prefixed (sn83045462, mn99999999) or purely numeric (2010218500)
LCCN_RE = re.compile(r"^[a-z]{0,3}\d{8,12}$")
