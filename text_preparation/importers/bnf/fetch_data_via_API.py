#!/usr/bin/env python3
"""Download new BNF newspaper titles from the BnF authenticated IIIF API onto the NAS.

For each title listed in ``titles_to_download.csv``, read its issue arks from
``arks_num_per_ark_bib/{cb_ark}.txt``, fetch each issue's IIIF v3 manifest, download the
per-page ALTO.XML and the manifest JSON, name everything by canonical ID, and save under
``target_base_dir/{alias}/{YYYY}/{MM}/{DD}/{edition}/``.

This is an async, rate-limited, WSO2-throttle-aware downloader. It has **no config file** —
every setting is a CLI argument with a default (see ``main``). The live API is IP-whitelisted
to the DHLab server, so real runs happen there; local runs are for offline development against
the fixtures in ``data/sample_data/BNF_API/fixtures/``.

Design notes and the full step plan live in ``bnf_api_progress.md`` (impresso repo root).

Credentials come from the environment (never hardcoded / synced / committed):
    BNF_API_KEY, BNF_API_SECRET

Example (dhlab, real run)::

    conda activate cpu
    source ~/.env
    python fetch_data_via_API.py --dry_run=False

Example (local, offline dev smoke test)::

    python fetch_data_via_API.py \\
        --titles_csv .../sample_data/BNF_API/BnF_API_info/titles_to_download.csv \\
        --arks_dir   .../sample_data/BNF_API/BnF_API_info/arks_num_per_ark_bib
"""

import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from time import monotonic

import fire
import httpx
import pandas as pd
import stamina
from aiolimiter import AsyncLimiter
from tqdm import tqdm

from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)


class _TqdmLoggingHandler(logging.Handler):
    """Route log records through ``tqdm.write`` so they don't corrupt an active progress bar.

    ``init_logger``'s console handler writes to stderr, the same stream the tqdm bar lives on, so
    a log line emitted mid-run would smear the bar. ``tqdm.write`` clears the bar, prints the line,
    and redraws. Only used when logging to the console (a ``log_file`` never clashes with the bar).
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            tqdm.write(self.format(record))
        except Exception:  # noqa: BLE001 — logging must never raise
            self.handleError(record)


# --- BnF API endpoints (confirmed live; see bnf_api_progress.md) ----------------------------
TOKEN_URL = "https://apimauthproext.bnf.fr/oauth2/token"
MANIFEST_TMPL = "https://openapiproext.bnf.fr/iiif/presentation/v3/ark:/12148/{ark}/manifest.json"

# --- Default input location (the dhlab sync target; override locally) -----------------------
INFO_DIR = "/home/piconti/impresso-text-acquisition/text_preparation/data/sample_data/BNF_API/BnF_API_info"  # "/rcp-scratch/journe/impresso-acquisition/bnf_api/BnF_API_info"

VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

# --- API client (Step 3) --------------------------------------------------------------------
# Transient errors worth retrying: network/timeout (TransportError) and 5xx (surfaced as
# HTTPStatusError by raise_for_status). WSO2 throttles are handled explicitly, never retried here.
RETRYABLE = (httpx.TransportError, httpx.HTTPStatusError)
# Fallback sleep when a throttle body carries no usable nextAccessTime (e.g. burst limit 900807).
DEFAULT_THROTTLE_BACKOFF_S = 30.0
# Safety net: if a single request is throttled this many times in a row (e.g. the server keeps
# returning a throttle with a stale/past nextAccessTime), halt instead of spinning.
MAX_CONSECUTIVE_THROTTLES = 20
# French month abbreviations as they appear in WSO2 `nextAccessTime` (accented + ASCII fallbacks).
FR_MONTHS = {
    "janv": 1,
    "févr": 2,
    "fevr": 2,
    "mars": 3,
    "avr": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "août": 8,
    "aout": 8,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "déc": 12,
    "dec": 12,
}


def _split_csv(value) -> list[str]:
    """Parse a comma-separated aliases argument into a clean list.

    ``fire`` auto-parses ``--aliases=a,b`` into a tuple and ``--aliases=a`` into a str, so accept
    both (plus an empty value).
    """
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Step 2 — input parsing → work list
# ---------------------------------------------------------------------------


@dataclass
class TitleWork:
    """One BNF title to download, with its list of issue arks.

    ``issue_arks`` are bare document arks (e.g. ``bpt6k4690357j``) with no ``ark:/12148/``
    prefix, in the order they appear in ``{cb_ark}.txt``, deduplicated.
    """

    alias: str
    cb_ark: str
    title: str
    start_year: int
    end_year: int
    expected_docs: int
    issue_arks: list[str] = field(default_factory=list)


def load_titles(
    titles_csv: str, aliases_include: list[str], aliases_exclude: list[str]
) -> pd.DataFrame:
    """Read ``titles_to_download.csv`` and apply alias include/exclude filtering.

    Columns are referenced by name (``ARK ID``, ``Alias``, ``start_year``, ``end_year``,
    ``DOCS``); the unnamed leading index column and ``Title`` are ignored. Include-filter (when
    non-empty) is applied first, then exclude-filter, case-sensitively on ``Alias``.
    """
    df = pd.read_csv(titles_csv, dtype={"ARK ID": str, "Alias": str})
    required = {"ARK ID", "Alias", "start_year", "end_year", "DOCS"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{titles_csv} is missing required columns: {sorted(missing)}")

    # Defensive: cb_ark should map to a single alias. Flag (don't handle) duplicates —
    # they would need year-range partitioning, which the current data doesn't require.
    dup_arks = df["ARK ID"][df["ARK ID"].duplicated(keep=False)].unique()
    if len(dup_arks) > 0:
        logger.warning(
            "%d ARK ID(s) appear on multiple rows (unhandled — issues would be downloaded "
            "under several aliases): %s",
            len(dup_arks),
            ", ".join(map(str, dup_arks)),
        )

    if aliases_include:
        found = set(df["Alias"])
        for alias in aliases_include:
            if alias not in found:
                logger.warning("aliases_include: alias not found in CSV: %s", alias)
        df = df[df["Alias"].isin(aliases_include)]
    if aliases_exclude:
        df = df[~df["Alias"].isin(aliases_exclude)]

    return df


def read_issue_arks(arks_dir: str, cb_ark: str) -> list[str] | None:
    """Read ``{cb_ark}.txt`` → list of bare issue arks, deduped (order preserved).

    Returns ``None`` if the file does not exist.
    """
    path = os.path.join(arks_dir, f"{cb_ark}.txt")
    if not os.path.isfile(path):
        return None
    seen: set[str] = set()
    arks: list[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            ark = line.strip()
            if ark and ark not in seen:
                seen.add(ark)
                arks.append(ark)
    return arks


def build_work_list(
    titles_csv: str,
    arks_dir: str,
    aliases_include: list[str],
    aliases_exclude: list[str],
) -> list[TitleWork]:
    """Combine the CSV and arks files into an alias-filtered list of ``TitleWork``.

    Titles whose arks file is missing or empty are skipped with a warning (never fatal). A
    per-title count mismatch against ``DOCS`` is logged as a warning. Emits run totals.
    """
    df = load_titles(titles_csv, aliases_include, aliases_exclude)

    work: list[TitleWork] = []
    n_skipped = 0
    for _, row in df.iterrows():
        alias = row["Alias"]
        cb_ark = row["ARK ID"]
        docs = int(row["DOCS"])
        arks = read_issue_arks(arks_dir, cb_ark)
        if arks is None:
            logger.warning("Skipping %s (%s): arks file not found.", alias, cb_ark)
            n_skipped += 1
            continue
        if not arks:
            logger.warning("Skipping %s (%s): arks file is empty.", alias, cb_ark)
            n_skipped += 1
            continue
        if len(arks) != docs:
            logger.warning("%s (%s): issue count %d != DOCS %d.", alias, cb_ark, len(arks), docs)
        work.append(
            TitleWork(
                alias=alias,
                cb_ark=cb_ark,
                title=str(row["Title"]),
                start_year=int(row["start_year"]),
                end_year=int(row["end_year"]),
                expected_docs=docs,
                issue_arks=arks,
            )
        )

    n_issues = sum(len(tw.issue_arks) for tw in work)
    logger.info(
        "Work list: %d titles, %d issues%s.",
        len(work),
        n_issues,
        f" ({n_skipped} titles skipped)" if n_skipped else "",
    )
    return work


def _log_settings(settings: dict) -> None:
    """Log the resolved settings at INFO."""
    logger.info("Resolved settings:")
    for key, val in settings.items():
        logger.info("  %s: %s", key, val)


# ---------------------------------------------------------------------------
# Step 3 — async, WSO2-throttle-aware API client
# ---------------------------------------------------------------------------


class ThrottleHalt(Exception):
    """Raised when a WSO2 throttle's ``nextAccessTime`` is too far off to sleep through.

    The caller (Step 6/7) is expected to checkpoint progress and exit 75 (EX_TEMPFAIL) so a
    later re-run resumes. ``info`` is the raw throttle body.
    """

    def __init__(self, info: dict):
        self.info = info
        super().__init__(
            f"Throttled until {info.get('nextAccessTime', '?')} (code {info.get('code', '?')})"
        )


_NEXT_ACCESS_RE = re.compile(r"(\d{4})-([^-]+?)\.?-(\d{1,2})\s+(\d{2}):(\d{2}):(\d{2})([+-]\d{4})")


def parse_next_access(value: str) -> datetime:
    """Parse a WSO2 ``nextAccessTime`` string into a tz-aware datetime.

    Format is French-localized, e.g. ``"2026-avr.-22 15:31:00+0000 UTC"`` (``avr.`` = avril).
    Locale-independent (uses ``FR_MONTHS``); raises ``ValueError`` if unparseable.
    """
    match = _NEXT_ACCESS_RE.match(value.replace(" UTC", "").strip())
    if not match:
        raise ValueError(f"unparseable nextAccessTime: {value!r}")
    year, mon, day, hh, mm, ss, offset = match.groups()
    month = FR_MONTHS.get(mon.rstrip(".").lower())
    if month is None:
        raise ValueError(f"unknown French month {mon!r} in {value!r}")
    sign = 1 if offset[0] == "+" else -1
    tz = timezone(sign * timedelta(minutes=int(offset[1:3]) * 60 + int(offset[3:5])))
    return datetime(int(year), month, int(day), int(hh), int(mm), int(ss), tzinfo=tz)


def wso2_throttle(resp: httpx.Response) -> dict | None:
    """Return the WSO2 throttle body if ``resp`` is a throttle-out response, else ``None``.

    A throttle arrives as HTTP 429 (subscription/app/resource/burst) or 503 (hard-limit/blocked)
    with a JSON body whose ``code`` starts ``"9008"``. Matching on the body (not status alone)
    avoids treating a genuine 503 backend outage as a throttle.
    """
    if resp.status_code not in (429, 503):
        return None
    try:
        body = resp.json()
    except Exception:
        return None
    if isinstance(body, dict) and str(body.get("code", "")).startswith("9008"):
        return body
    return None


def throttle_decision(info: dict, threshold_min: float, now: datetime) -> tuple[str, float]:
    """Decide how to react to a throttle: ``("sleep", seconds)`` or ``("halt", 0.0)``.

    - ``nextAccessTime`` within ``threshold_min`` → sleep exactly that long.
    - further off → halt (checkpoint + exit upstream).
    - missing / unparseable → sleep ``DEFAULT_THROTTLE_BACKOFF_S``.
    - already in the past → sleep 0 (retry immediately).
    """
    nat = info.get("nextAccessTime")
    if not nat:
        return ("sleep", DEFAULT_THROTTLE_BACKOFF_S)
    try:
        wait = (parse_next_access(nat) - now).total_seconds()
    except ValueError:
        return ("sleep", DEFAULT_THROTTLE_BACKOFF_S)
    if wait <= 0:
        return ("sleep", 0.0)
    if wait <= threshold_min * 60:
        return ("sleep", wait)
    return ("halt", 0.0)


class TokenManager:
    """Caches and refreshes the OAuth2 client-credentials bearer token.

    Token fetches deliberately do NOT go through the client's semaphore/limiter: a request
    awaits ``token()`` *before* acquiring a slot, so routing the fetch through the semaphore
    could deadlock when every slot is held waiting for a token.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        token_url: str,
        key: str | None,
        secret: str | None,
        *,
        leeway: float = 60.0,
        max_retries: int = 5,
    ):
        self._client = client
        self._url = token_url
        self._key = key
        self._secret = secret
        self._leeway = leeway
        self._max_retries = max_retries
        self._token: str | None = None
        self._expiry: float = 0.0  # monotonic seconds
        self._lock = asyncio.Lock()

    def _valid(self) -> bool:
        return self._token is not None and monotonic() < self._expiry - self._leeway

    async def token(self) -> str:
        if self._valid():
            return self._token  # type: ignore[return-value]
        async with self._lock:
            if not self._valid():  # double-check under the lock
                await self._fetch()
            return self._token  # type: ignore[return-value]

    async def force_refresh(self) -> None:
        async with self._lock:
            self._token = None
            await self._fetch()

    async def _fetch(self) -> None:
        if not self._key or not self._secret:
            raise RuntimeError("BNF_API_KEY / BNF_API_SECRET not set — cannot fetch an API token.")
        resp = None
        async for attempt in stamina.retry_context(
            on=httpx.TransportError, attempts=self._max_retries, timeout=None
        ):
            with attempt:
                resp = await self._client.post(
                    self._url,
                    auth=(self._key, self._secret),
                    data={"grant_type": "client_credentials"},
                )
                resp.raise_for_status()
        body = resp.json()  # type: ignore[union-attr]
        self._token = body["access_token"]
        self._expiry = monotonic() + float(body.get("expires_in", 3600))
        logger.debug("Fetched a new API token (expires_in=%ss).", body.get("expires_in", 3600))


class BnfApiClient:
    """Async client for the BnF IIIF API: auth + rate limit + concurrency cap + retry + throttle.

    Manifest and ALTO requests are governed by SEPARATE limiter+semaphore pairs (BnF caps the
    presentation/manifest API at 50/min but lets ALTO scale to ~900/min).

    Usage::

        async with BnfApiClient(
            manifest_rate_per_min=50, alto_rate_per_min=900,
            manifest_concurrency=4, alto_concurrency=20, ...
        ) as api:
            manifest = await api.get_manifest("bpt6k4690357j")
            alto = await api.get_alto(alto_url)
    """

    def __init__(
        self,
        *,
        manifest_rate_per_min: float,
        alto_rate_per_min: float,
        manifest_concurrency: int,
        alto_concurrency: int,
        max_retries: int,
        timeout: float,
        throttle_sleep_threshold_min: float,
        manifest_tmpl: str = MANIFEST_TMPL,
        token_url: str = TOKEN_URL,
        transport: httpx.BaseTransport | None = None,
    ):
        self.manifest_tmpl = manifest_tmpl
        self.max_retries = max_retries
        self.threshold_min = throttle_sleep_threshold_min
        # BnF's quota (per Ludovic): 1000 req/min/IP across ALL APIs, but the *presentation* API
        # (manifest.json) has a much tighter 50/min bucket, while ALTO may use the rest (~900/min).
        # So the two endpoint classes get INDEPENDENT limiter+semaphore pairs — a single shared cap
        # would let manifest requests burst to the ALTO rate and get throttled (WSO2 900802) at once.
        #
        # aiolimiter's first arg is the bucket *capacity* (== max burst), and each request does
        # acquire(1). AsyncLimiter(rate, 1) therefore breaks for sub-1/s rates (the manifest 50/min
        # ≈ 0.83/s) — capacity < 1 < the acquired amount → "Can't acquire more than the maximum
        # capacity". Express each cap as 1 permit per (60/rate_per_min) seconds instead: valid for
        # any rate > 0, and capacity 1 means no initial burst — the smoother pacing the strict BnF
        # per-window quota needs.
        self.manifest_limiter = AsyncLimiter(1, 60.0 / manifest_rate_per_min)
        self.alto_limiter = AsyncLimiter(1, 60.0 / alto_rate_per_min)
        self.manifest_sem = asyncio.Semaphore(manifest_concurrency)
        self.alto_sem = asyncio.Semaphore(alto_concurrency)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_connections=alto_concurrency + manifest_concurrency + 5),
            transport=transport,
        )
        self.tokens = TokenManager(
            self.client,
            token_url,
            os.environ.get("BNF_API_KEY"),
            os.environ.get("BNF_API_SECRET"),
            max_retries=max_retries,
        )

    async def __aenter__(self) -> "BnfApiClient":
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.client.aclose()

    async def get_manifest(self, ark: str) -> dict:
        """Fetch and return an issue's IIIF v3 manifest as a dict (manifest limiter, 50/min)."""
        resp = await self._get(
            self.manifest_tmpl.format(ark=ark),
            limiter=self.manifest_limiter,
            sem=self.manifest_sem,
        )
        return resp.json()

    async def get_alto(self, url: str) -> bytes:
        """Fetch an ALTO.XML page and return its raw bytes (ALTO limiter, ~900/min)."""
        resp = await self._get(url, limiter=self.alto_limiter, sem=self.alto_sem)
        return resp.content

    async def _get(
        self, url: str, *, limiter: AsyncLimiter, sem: asyncio.Semaphore
    ) -> httpx.Response:
        """GET ``url`` with auth, rate limiting, transient-retry, 401-refresh and throttle handling.

        ``limiter``/``sem`` are the endpoint-specific pair (manifest vs ALTO), so the two request
        classes are paced independently under BnF's split quota.
        """
        did_401 = False
        throttles = 0
        while True:
            token = await self.tokens.token()  # outside the semaphore (avoid deadlock)
            resp: httpx.Response | None = None
            async for attempt in stamina.retry_context(
                on=RETRYABLE, attempts=self.max_retries, timeout=None
            ):
                with attempt:
                    async with sem:
                        async with limiter:
                            resp = await self.client.get(
                                url, headers={"Authorization": f"Bearer {token}"}
                            )
                    # Retry genuine 5xx (not WSO2 throttles) via stamina.
                    if resp.status_code >= 500 and wso2_throttle(resp) is None:
                        resp.raise_for_status()

            if resp.status_code == 401 and not did_401:
                did_401 = True
                await self.tokens.force_refresh()
                continue

            throttle = wso2_throttle(resp)
            if throttle is not None:
                throttles += 1
                if throttles > MAX_CONSECUTIVE_THROTTLES:
                    logger.error(
                        "Throttled %d times in a row for %s — halting to avoid a spin loop.",
                        throttles,
                        url,
                    )
                    raise ThrottleHalt(throttle)
                await self._handle_throttle(throttle)
                continue

            resp.raise_for_status()  # other 4xx → error out
            return resp

    async def _handle_throttle(self, info: dict) -> None:
        """Sleep through a short throttle, or raise ``ThrottleHalt`` for a long one."""
        action, seconds = throttle_decision(info, self.threshold_min, datetime.now(timezone.utc))
        if action == "halt":
            raise ThrottleHalt(info)
        logger.warning(
            "Throttled (code %s) — sleeping %.0fs before retrying.",
            info.get("code", "?"),
            seconds,
        )
        await asyncio.sleep(seconds)


# ---------------------------------------------------------------------------
# Step 4 — manifest parsing → IssueMeta
# ---------------------------------------------------------------------------


class ManifestError(Exception):
    """A manifest is malformed / unusable (no items, no date, no usable pages)."""


@dataclass
class PageInfo:
    """One page of an issue: physical number, ALTO.XML URL, and pixel dimensions."""

    page_num: int
    alto_url: str  # host already rewritten to openapiproext.bnf.fr
    width: int
    height: int


@dataclass
class IssueMeta:
    """Parsed issue-level metadata from a IIIF v3 manifest."""

    issue_ark: str
    title: str | None
    date: date
    language: str | None
    pages: list[PageInfo]  # sorted by page_num, deduped


_ALTO_PAGE_RE = re.compile(r"/f(\d+)/[^/]*$")


def _meta_value(manifest: dict, label_fr: str) -> str | None:
    """Return the French value of the ``metadata`` entry whose French label == ``label_fr``.

    Matches the label **exactly** (so ``Date`` never picks up ``Date de mise en ligne``).
    """
    for entry in manifest.get("metadata", []):
        try:
            if entry["label"]["fr"][0] == label_fr:
                return entry["value"]["fr"][0]
        except (KeyError, IndexError, TypeError):
            continue
    return None


def _canvas_alto_url(canvas: dict) -> str | None:
    """Return the ALTO.XML URL for a canvas (host rewritten), or ``None`` if absent.

    Prefers the ``seeAlso`` entry whose ``profile`` mentions ALTO; falls back to the first
    XML-format entry. ``openapi.bnf.fr`` is rewritten to the authenticated ``openapiproext.bnf.fr``.
    """
    see_also = canvas.get("seeAlso") or []
    chosen = None
    for entry in see_also:
        profile = str(entry.get("profile", "")).lower()
        if "alto" in profile:
            chosen = entry
            break
        if chosen is None and entry.get("format") == "application/xml":
            chosen = entry
    if chosen is None or "id" not in chosen:
        return None
    return str(chosen["id"]).replace("openapi.bnf.fr", "openapiproext.bnf.fr")


def _page_num_from_alto_url(url: str) -> int | None:
    """Extract the physical page number from the ``…/f{n}/alto.xml`` segment."""
    match = _ALTO_PAGE_RE.search(url)
    return int(match.group(1)) if match else None


def _normalize_title(value: str) -> str:
    """Lowercase + keep only alphanumerics — for a punctuation-insensitive title compare."""
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def parse_manifest(manifest: dict, issue_ark: str, expected_title: str | None = None) -> IssueMeta:
    """Parse a IIIF v3 manifest into an ``IssueMeta``.

    Raises ``ManifestError`` if the manifest has no ``items``, no parseable ``Date``, or no usable
    pages. Missing title/language degrade to ``None`` with a warning. A canvas missing its ALTO
    link / dimensions / page number is skipped (warning), not fatal.
    """
    items = manifest.get("items")
    if not items:
        raise ManifestError(f"{issue_ark}: manifest has no items (canvases).")

    title = _meta_value(manifest, "Titre")
    if title is None:
        try:
            title = manifest["summary"]["fr"][0]
        except (KeyError, IndexError, TypeError):
            title = None
    if title is None:
        logger.warning("%s: no title (Titre/summary) in manifest.", issue_ark)

    date_str = _meta_value(manifest, "Date")
    # is_exact_date = True if issue index is implemented from here
    issue_date = None
    if not date_str:
        raise ManifestError(f"{issue_ark}: manifest has no 'Date' metadata.")
    try:
        issue_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        msg = f"{issue_ark}: Found unparseable date {date_str!r} ({exc}) - Applying fix."
        print(msg)
        if "-" in date_str:
            # year-month only date, need to add the day, 1st of the month
            f_date_str = "-".join([date_str, "01"])
        else:
            # year only date, need to add the day and month, 1st of january
            f_date_str = "-".join([date_str, "01", "01"])
        # is_exact_date = False
        msg = f"{issue_ark}: unparseable Date {date_str!r} - Fixed to {f_date_str}, setting is_exact_date=False."
        print(msg)
        logger.warning(msg)

    # Retrying with the fixed date
    if not issue_date:
        try:
            issue_date = datetime.strptime(f_date_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ManifestError(f"{issue_ark}: unparseable Date {date_str!r} ({exc}).") from exc

    language = _meta_value(manifest, "Langue")
    if language is None:
        logger.warning("%s: no language (Langue) in manifest.", issue_ark)

    pages: list[PageInfo] = []
    seen_nums: set[int] = set()
    for idx, canvas in enumerate(items):
        alto_url = _canvas_alto_url(canvas)
        width = canvas.get("width")
        height = canvas.get("height")
        page_num = _page_num_from_alto_url(alto_url) if alto_url else None
        if alto_url is None or width is None or height is None or page_num is None:
            logger.warning(
                "%s: skipping canvas %d (missing ALTO url / width / height / page number).",
                issue_ark,
                idx,
            )
            continue
        if page_num in seen_nums:
            logger.warning("%s: duplicate page number %d — keeping the first.", issue_ark, page_num)
            continue
        seen_nums.add(page_num)
        pages.append(PageInfo(page_num, alto_url, int(width), int(height)))

    if not pages:
        raise ManifestError(f"{issue_ark}: no usable pages in manifest.")
    pages.sort(key=lambda p: p.page_num)

    if expected_title and title:
        exp, got = _normalize_title(expected_title), _normalize_title(title)
        if exp and got and exp not in got and got not in exp:
            logger.warning(
                "%s: manifest title %r differs from expected %r.",
                issue_ark,
                title,
                expected_title,
            )

    return IssueMeta(issue_ark, title, issue_date, language, pages)


# ---------------------------------------------------------------------------
# Step 5 — canonical IDs & edition disambiguation
# ---------------------------------------------------------------------------

# NOTE: impresso_essentials.io.fs_utils.check_id is broken (its regexes use `\\d`, a literal
# backslash-d, so it rejects every valid ID). We validate locally instead — these mirror the
# *working* parse_canonical_filename convention. Not fixing check_id: not our code (CLAUDE.md).
_ISSUE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\d{4}-\d{2}-\d{2}-[a-z]{1,2}$")
_PAGE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\d{4}-\d{2}-\d{2}-[a-z]{1,2}-p\d{4}$")


def get_edition(n: int) -> str:
    """Map a 1-based edition ordinal to a canonical edition letter.

    ``1 -> "a" … 26 -> "z", 27 -> "aa", 28 -> "ab", …`` (the canonical convention allows one or
    two lowercase letters). Mirrors the RTS ``get_edition`` counter.
    """
    if n < 1:
        raise ValueError(f"edition ordinal must be >= 1, got {n}")
    if n <= 26:
        return chr(96 + n)
    first, second = divmod(n, 26)
    if second == 0:
        first -= 1
        second = 26
    return chr(96 + first) + chr(96 + second)


def build_issue_id(alias: str, d: date, edition: str) -> str:
    """Build a canonical issue ID: ``{alias}-{YYYY}-{MM}-{DD}-{edition}``."""
    return f"{alias}-{d.year:04d}-{d.month:02d}-{d.day:02d}-{edition}"


def build_page_id(issue_id: str, page_num: int) -> str:
    """Build a canonical page ID: ``{issue_id}-p{NNNN}`` (4-digit zero-padded)."""
    if page_num > 9999:
        logger.warning(
            "page number %d > 9999 for %s — exceeds the 4-digit canonical convention.",
            page_num,
            issue_id,
        )
    return f"{issue_id}-p{page_num:04d}"


def assign_editions(dated: list[tuple[int, date]]) -> dict[int, str]:
    """Assign edition letters to one title's issues, keyed by their arks-file index.

    ``dated`` is ``(arks_index, date)`` for the successfully-parsed issues of a single title.
    Issues are ordered by ``arks_index`` (the deterministic arks-file order, independent of
    fetch/concurrency order); within each date a 1-based counter drives ``get_edition``. A day
    with a single issue yields ``"a"``.
    """
    counters: dict[date, int] = {}
    editions: dict[int, str] = {}
    for arks_index, issue_date in sorted(dated, key=lambda item: item[0]):
        counters[issue_date] = counters.get(issue_date, 0) + 1
        editions[arks_index] = get_edition(counters[issue_date])
    return editions


def is_canonical(cid: str, kind: str = "issue") -> bool:
    """Validate a canonical ID against the local (correct) regex. ``kind`` in {issue, page}."""
    pattern = _PAGE_ID_RE if kind == "page" else _ISSUE_ID_RE
    return pattern.match(cid) is not None


_ISSUE_ID_PARTS_RE = re.compile(
    r"^(?P<alias>[A-Za-z][A-Za-z0-9_]*)-(\d{4})-(\d{2})-(\d{2})-(?P<edition>[a-z]{1,2})$"
)


def _issue_dir_from_id(target_base_dir: str, issue_id: str) -> str | None:
    """Reconstruct an issue's on-disk directory from its canonical ``issue_id``.

    Builds the canonical ``{alias}/{YYYY}/{MM}/{DD}/{edition}/`` output dir from an ``issue_id`` (used
    by the worker to place ``manifest.json`` + pages). Returns ``None`` if the id is malformed.
    """
    match = _ISSUE_ID_PARTS_RE.match(issue_id)
    if not match:
        return None
    alias, year, month, day, edition = (
        match.group("alias"),
        match.group(2),
        match.group(3),
        match.group(4),
        match.group("edition"),
    )
    return os.path.join(target_base_dir, alias, year, month, day, edition)


def _date_from_issue_id(issue_id: str) -> date | None:
    """Recover the issue date from a canonical issue_id (``…-YYYY-MM-DD-{edition}``).

    Used on resume to seed edition assignment from prior-run successes (Step 7) without re-fetching
    their manifests. Returns ``None`` if the id is malformed / the date is invalid.
    """
    match = re.search(r"-(\d{4})-(\d{2})-(\d{2})-[a-z]{1,2}$", issue_id)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Step 6 — async download & save (V1 core)
# ---------------------------------------------------------------------------


@dataclass
class Stats:
    """Running counters for a download run."""

    issues_ok: int = 0
    issues_failed: int = 0
    pages_downloaded: int = 0
    pages_skipped: int = 0
    manifests_reused: int = 0  # served from the ark-keyed cache instead of the API

    def log_summary(self) -> None:
        logger.info(
            "Done: %d issues ok, %d failed | %d pages downloaded, %d skipped | "
            "%d manifests reused from cache.",
            self.issues_ok,
            self.issues_failed,
            self.pages_downloaded,
            self.pages_skipped,
            self.manifests_reused,
        )


def _atomic_write(path: str, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically (temp file in the same dir, then ``os.replace``)."""
    tmp = f"{path}.part"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


def _read_json_file(path: str) -> dict:
    """Read and JSON-decode ``path`` (used off-thread to reuse an on-disk manifest.json)."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _manifest_bytes(manifest: dict) -> bytes:
    """Serialize a manifest dict to the on-disk manifest.json byte form (UTF-8, indented)."""
    return json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")


async def _download_page(
    api: "BnfApiClient",
    issue_dir: str,
    page_id: str,
    alto_url: str,
    *,
    overwrite: bool,
    stats: Stats,
) -> bool:
    """Download one page's ALTO into ``issue_dir/{page_id}.xml``. Returns True on success.

    Skips (counts as success) when the target already exists non-empty and ``overwrite`` is False.
    """
    target = os.path.join(issue_dir, f"{page_id}.xml")
    if not overwrite and os.path.isfile(target) and os.path.getsize(target) > 0:
        stats.pages_skipped += 1
        return True
    data = await api.get_alto(alto_url)
    if not data:
        raise RuntimeError(f"empty ALTO for {page_id} ({alto_url})")
    await asyncio.to_thread(_atomic_write, target, data)
    stats.pages_downloaded += 1
    return True


def _cache_path(cache_dir: str, alias: str, ark: str) -> str:
    """Ark-keyed manifest cache path ``{cache_dir}/{alias}/{ark}.json`` (input-structured)."""
    return os.path.join(cache_dir, alias, f"{ark}.json")


def _write_cache_file(path: str, data: bytes) -> None:
    """Atomically write a cache file, creating its ``{cache}/{alias}/`` parent."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _atomic_write(path, data)


async def get_manifest_cached(
    api: "BnfApiClient",
    ark: str,
    alias: str,
    cache_dir: str,
    *,
    recompute: bool,
    write: bool,
    stats: Stats,
) -> dict:
    """Return an issue's manifest, using an ark-keyed on-disk cache to avoid re-fetching.

    The cache path ``{cache_dir}/{alias}/{ark}.json`` is keyed on ``(alias, ark)`` — both known
    *before* the fetch — so a resume/throttle-halt reuses any previously-fetched manifest with **no
    API call and no report record** (the filesystem is the index; this replaces the old
    prior_issue_id/manifest_path reuse). On a cache miss (or ``recompute``, or a corrupt entry) the
    manifest is fetched from the API and, when ``write`` (i.e. not dry_run), cached atomically.
    """
    path = _cache_path(cache_dir, alias, ark)
    if not recompute and os.path.isfile(path) and os.path.getsize(path) > 0:
        try:
            manifest = await asyncio.to_thread(_read_json_file, path)
            stats.manifests_reused += 1
            return manifest
        except Exception as exc:  # noqa: BLE001 — a corrupt cache entry just means refetch
            logger.warning(
                "%s (%s): corrupt manifest cache %s (%s) — refetching.", alias, ark, path, exc
            )
    manifest = await api.get_manifest(ark)
    if write:
        await asyncio.to_thread(_write_cache_file, path, _manifest_bytes(manifest))
    return manifest


async def _fetch_parse_cached(
    api: "BnfApiClient",
    tw: TitleWork,
    ark: str,
    cache_dir: str,
    stats: Stats,
    progress: tqdm,
    report: "DownloadReport",
    *,
    recompute: bool,
    write: bool,
) -> IssueMeta | None:
    """Fetch (cache-first) + parse one manifest → ``IssueMeta`` (for the date; the worker re-reads the
    manifest from the cache). ``ThrottleHalt`` propagates; other errors → record + advance + ``None``.
    """
    try:
        manifest = await get_manifest_cached(
            api, ark, tw.alias, cache_dir, recompute=recompute, write=write, stats=stats
        )
        return parse_manifest(manifest, ark, expected_title=tw.title)
    except ThrottleHalt:
        raise
    except Exception as exc:  # noqa: BLE001 — one bad issue must not abort the run
        logger.warning("%s (%s): manifest fetch/parse failed: %s", tw.alias, ark, exc)
        stats.issues_failed += 1
        report.write_failure(ark, None, tw.alias, 0, 0, [{"page": None, "error": str(exc)}])
        progress.update(1)
        return None


async def _download_issue(
    api: "BnfApiClient",
    tw: TitleWork,
    ark: str,
    issue_id: str,
    target_base_dir: str,
    cache_dir: str,
    *,
    overwrite: bool,
    recompute_manifest: bool,
    stats: Stats,
    progress: tqdm,
    report: "DownloadReport",
) -> None:
    """Worker: (re)load the manifest from the ark-keyed cache, write the canonical ``manifest.json``
    at the issue root (before the pages), download all ALTO pages, and record the outcome.

    Only when every page succeeds is the issue counted ok + recorded as a success. Advances
    ``progress`` by one when the issue is done (success or failure), in a ``finally``.
    """
    try:
        try:
            manifest = await asyncio.to_thread(
                _read_json_file, _cache_path(cache_dir, tw.alias, ark)
            )
            meta = parse_manifest(manifest, ark, expected_title=tw.title)
        except Exception as exc:  # noqa: BLE001 — a bad cache entry fails just this issue
            logger.error("%s (%s): could not load cached manifest: %s", issue_id, ark, exc)
            stats.issues_failed += 1
            report.write_failure(
                ark, issue_id, tw.alias, 0, 0, [{"page": None, "error": f"cache load: {exc}"}]
            )
            return

        issue_dir = _issue_dir_from_id(target_base_dir, issue_id)
        if issue_dir is None:
            logger.error("%s: non-canonical issue_id — skipping.", issue_id)
            stats.issues_failed += 1
            report.write_failure(
                ark,
                issue_id,
                tw.alias,
                len(meta.pages),
                0,
                [{"page": None, "error": "non-canonical issue_id"}],
            )
            return
        os.makedirs(issue_dir, exist_ok=True)
        # Canonical manifest at the issue root, *before* the pages (so a partial issue keeps it).
        manifest_target = os.path.join(issue_dir, "manifest.json")
        if recompute_manifest or not (
            os.path.isfile(manifest_target) and os.path.getsize(manifest_target) > 0
        ):
            await asyncio.to_thread(_atomic_write, manifest_target, _manifest_bytes(manifest))

        results = await asyncio.gather(
            *[
                _download_page(
                    api,
                    issue_dir,
                    build_page_id(issue_id, page.page_num),
                    page.alto_url,
                    overwrite=overwrite,
                    stats=stats,
                )
                for page in meta.pages
            ],
            return_exceptions=True,
        )
        throttle = next((r for r in results if isinstance(r, ThrottleHalt)), None)
        if throttle is not None:
            raise throttle
        errors = [
            {"page": page.page_num, "error": str(r)}
            for page, r in zip(meta.pages, results)
            if isinstance(r, Exception)
        ]
        if errors:
            logger.warning(
                "%s: %d/%d pages failed (%s) — good pages + manifest kept, not a success.",
                issue_id,
                len(errors),
                len(meta.pages),
                errors[0]["error"],
            )
            stats.issues_failed += 1
            report.write_failure(
                ark, issue_id, tw.alias, len(meta.pages), len(meta.pages) - len(errors), errors
            )
            return

        stats.issues_ok += 1
        report.write_success(ark, issue_id)
    finally:
        progress.update(1)


async def run_downloads(
    work: list[TitleWork],
    api: "BnfApiClient",
    target_base_dir: str,
    cache_dir: str,
    *,
    overwrite: bool,
    dry_run: bool,
    recompute_manifest: bool,
    limit: int,
    report: "DownloadReport",
    prefetch: int = 200,
) -> Stats:
    """Drive the whole download as an **issue-granular streaming** pipeline.

    A single **producer** walks each title's arks in arks-index order on the manifest bucket
    (50/min), fetching each manifest **through the ark-keyed cache** (``get_manifest_cached`` →
    ``{cache_dir}/{alias}/{ark}.json``), assigning the edition from a running per-date counter, and
    putting a **minimal token** ``(tw, ark, issue_id)`` on a bounded queue immediately. A pool of
    **workers** drains the queue and, per issue, re-reads the manifest from the cache and downloads
    ALTO (~900/min bucket) via ``_download_issue``. So ALTO overlaps manifest-fetching *within* a
    title (no stall), the queue holds no manifests (RAM independent of pages), and any resume reuses
    the cache with 0 re-fetch.

    The running counter (skipped/resumed arks reserve their letter, parsed to-do arks emit) is in
    arks-index order, so it reproduces ``assign_editions`` exactly. ``prefetch`` bounds the token
    queue (backpressure only — memory is negligible).
    """
    stats = Stats()
    total_issues = sum(len(tw.issue_arks) for tw in work)
    workers = max(1, limit)
    queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, prefetch))

    with tqdm(total=total_issues, unit="issue", desc="issues") as progress:

        async def producer() -> None:
            for tw in work:
                n_todo = sum(1 for ark in tw.issue_arks if report.should_process(ark))
                logger.info(
                    "Title %s (%s): %d issues (%d to process, %d skipped).",
                    tw.alias,
                    tw.cb_ark,
                    len(tw.issue_arks),
                    n_todo,
                    len(tw.issue_arks) - n_todo,
                )
                counters: dict[date, int] = (
                    {}
                )  # per-date running edition counter (arks-index order)
                for ark in tw.issue_arks:
                    if not report.should_process(ark):
                        # Already done / not selected: reserve its edition letter, don't re-fetch.
                        prior_id = report.prior_issue_id(ark)
                        prior_date = _date_from_issue_id(prior_id) if prior_id else None
                        if prior_date is not None:
                            counters[prior_date] = counters.get(prior_date, 0) + 1
                        report.add_resumed(1)
                        progress.update(1)
                        continue
                    meta = await _fetch_parse_cached(
                        api,
                        tw,
                        ark,
                        cache_dir,
                        stats,
                        progress,
                        report,
                        recompute=recompute_manifest,
                        write=not dry_run,
                    )
                    if meta is None:  # failed/unparseable — logged + progress advanced already
                        continue
                    counters[meta.date] = counters.get(meta.date, 0) + 1
                    issue_id = build_issue_id(tw.alias, meta.date, get_edition(counters[meta.date]))
                    if not is_canonical(issue_id, "issue"):
                        logger.error("%s: built a non-canonical issue_id — skipping.", issue_id)
                        stats.issues_failed += 1
                        report.write_failure(
                            ark,
                            issue_id,
                            tw.alias,
                            len(meta.pages),
                            0,
                            [{"page": None, "error": "non-canonical issue_id"}],
                        )
                        progress.update(1)
                        continue
                    if dry_run:
                        logger.info(
                            "[dry-run] would write %d pages for %s", len(meta.pages), issue_id
                        )
                        stats.issues_ok += 1
                        progress.update(1)
                        continue
                    await queue.put((tw, ark, issue_id))  # blocks when full → backpressure
            for _ in range(workers):
                await queue.put(None)  # one sentinel per worker

        async def worker() -> None:
            while (item := await queue.get()) is not None:
                tw, ark, issue_id = item
                await _download_issue(
                    api,
                    tw,
                    ark,
                    issue_id,
                    target_base_dir,
                    cache_dir,
                    overwrite=overwrite,
                    recompute_manifest=recompute_manifest,
                    stats=stats,
                    progress=progress,
                    report=report,
                )

        # asyncio.gather (not TaskGroup) so a ThrottleHalt propagates unwrapped to main's handler.
        await asyncio.gather(producer(), *[worker() for _ in range(workers)])

    stats.log_summary()
    return stats


async def download_all(
    work: list[TitleWork],
    target_base_dir: str,
    cache_dir: str,
    *,
    dry_run: bool,
    overwrite: bool,
    recompute_manifest: bool,
    manifest_rate_per_min: float,
    alto_rate_per_min: float,
    manifest_concurrency: int,
    alto_concurrency: int,
    max_retries: int,
    timeout: float,
    throttle_sleep_threshold_min: float,
    report: "DownloadReport",
    prefetch: int = 200,
    transport: httpx.BaseTransport | None = None,
) -> Stats:
    """Open a client and run the whole download. ``transport`` lets tests inject a MockTransport."""
    async with BnfApiClient(
        manifest_rate_per_min=manifest_rate_per_min,
        alto_rate_per_min=alto_rate_per_min,
        manifest_concurrency=manifest_concurrency,
        alto_concurrency=alto_concurrency,
        max_retries=max_retries,
        timeout=timeout,
        throttle_sleep_threshold_min=throttle_sleep_threshold_min,
        transport=transport,
    ) as api:
        # The client's dual limiter+semaphore pairs are the real rate cap; the ALTO concurrency is
        # the right worker count (issues in flight).
        return await run_downloads(
            work,
            api,
            target_base_dir,
            cache_dir,
            overwrite=overwrite,
            dry_run=dry_run,
            recompute_manifest=recompute_manifest,
            limit=alto_concurrency,
            report=report,
            prefetch=prefetch,
        )


# ---------------------------------------------------------------------------
# Step 7 — robustness: resume report, tqdm-safe logging, graceful shutdown
# ---------------------------------------------------------------------------


class DownloadReport:
    """Split resume report for the BNF downloader, keyed by **issue ark**.

    Two fixed-name files live under ``report_dir``:

    - ``success.txt`` — one completed issue per line, ``"{ark}\\t{issue_id}"`` (ark = resume key;
      issue_id kept for readability + edition seeding).
    - ``failed.jsonl`` — one JSON object per failed issue / throttle-halt, with ``ark``,
      ``issue_id``, ``alias``, ``status``, ``num_pages``, ``pages_ok``, ``failed_pages``,
      ``errors``, ``timestamp``.

    Unlike ``structure_media.py``'s ``ReportWriter`` (which keys on ``issue_id``, known upfront from
    the issue index), the canonical ``issue_id`` here is only known *after* the manifest is fetched,
    so resume must key on the **ark** — the work unit known before any request. This lets a rerun
    skip a completed issue *without* re-fetching its manifest (the corpus is ~720k issues; re-fetching
    every manifest just to skip would cost hours at the rate cap).

    Resume (when ``prior_report_dir`` is set):
    - ``retry_failed_only=False`` → skip arks in the prior ``success.txt``; process everything else.
    - ``retry_failed_only=True``  → process *only* arks in the prior ``failed.jsonl``.

    On-disk skip-if-exists (Step 6, per page) still applies underneath as a second safety net, so a
    run with no report (or a partially-done issue absent from the report) still avoids re-downloading
    pages already on disk. Writes are line-buffered + flushed per record so an interruption loses at
    most nothing already flushed; all coroutine-side writes are non-awaiting, hence atomic under the
    single event loop (no lock needed).
    """

    SUCCESS_FILENAME = "success.txt"
    FAILED_FILENAME = "failed.jsonl"
    INDEX_FILENAME = "issue_index.bnf.json"

    def __init__(
        self,
        report_dir: str = "",
        prior_report_dir: str = "",
        retry_failed_only: bool = False,
        dry_run: bool = False,
    ):
        self._report_dir = report_dir
        self._prior_report_dir = prior_report_dir
        self.retry_failed_only = retry_failed_only
        self._dry_run = dry_run
        self._prepared = False
        self._counts = {"success": 0, "failed": 0, "resumed": 0}

        self._prior_success: dict[str, str] = {}  # ark -> issue_id
        self._prior_failed: dict[str, dict] = {}  # ark -> entry
        if prior_report_dir:
            self._prior_success, self._prior_failed = self._load_prior_report(prior_report_dir)
            logger.info(
                "Loaded prior report from %s (%d success, %d failed).",
                prior_report_dir,
                len(self._prior_success),
                len(self._prior_failed),
            )

        self._success_fh = None
        self._failed_fh = None

    # --- prior report loading ---

    @classmethod
    def _load_prior_report(cls, prior_report_dir: str) -> tuple[dict[str, str], dict[str, dict]]:
        """Read a prior ``success.txt`` + ``failed.jsonl``. Both maps may be empty."""
        success_path = os.path.join(prior_report_dir, cls.SUCCESS_FILENAME)
        failed_path = os.path.join(prior_report_dir, cls.FAILED_FILENAME)

        success: dict[str, str] = {}
        if os.path.isfile(success_path):
            with open(success_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    ark, _, issue_id = line.partition("\t")
                    success[ark] = issue_id
        else:
            logger.info("Prior success file not found: %s", success_path)

        failed: dict[str, dict] = {}
        if os.path.isfile(failed_path):
            with open(failed_path, "r", encoding="utf-8") as fh:
                for line_num, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("Skipping malformed line %d in %s.", line_num, failed_path)
                        continue
                    ark = entry.get("ark")
                    if ark and entry.get("status") != "throttle_halt":
                        failed[ark] = entry
        else:
            logger.info("Prior failed file not found: %s", failed_path)

        return success, failed

    # --- lifecycle ---

    def prepare(self, scope_arks: set[str]) -> None:
        """Open the report files and carry forward in-scope prior successes.

        ``scope_arks`` = every ark in this run's work list (after alias filtering). Prior successes
        in that scope are re-emitted so the fresh ``success.txt`` stays a complete snapshot even
        though those arks are skipped this run. In ``dry_run`` (or with no ``report_dir``) nothing is
        written. Refuses to clobber an existing non-empty report unless ``prior_report_dir`` is set.
        """
        if self._prepared:
            raise RuntimeError("DownloadReport.prepare() called twice")
        self._prepared = True

        if self.retry_failed_only and not self._prior_failed:
            raise RuntimeError(
                "retry_failed_only=True but no prior failures loaded — set prior_report_dir to a "
                "directory containing a failed.jsonl with entries."
            )

        if self._dry_run or not self._report_dir:
            return

        os.makedirs(self._report_dir, exist_ok=True)
        success_path = os.path.join(self._report_dir, self.SUCCESS_FILENAME)
        failed_path = os.path.join(self._report_dir, self.FAILED_FILENAME)

        if not self._prior_report_dir:
            for path in (success_path, failed_path):
                if os.path.isfile(path) and os.path.getsize(path) > 0:
                    raise RuntimeError(
                        f"report_dir {self._report_dir!r} already contains a non-empty "
                        f"{os.path.basename(path)}. Pass prior_report_dir={self._report_dir!r} to "
                        f"resume, or remove the directory to start fresh."
                    )

        self._success_fh = open(success_path, "w", encoding="utf-8")
        self._failed_fh = open(failed_path, "w", encoding="utf-8")

        carried = 0
        for ark, issue_id in self._prior_success.items():
            if ark in scope_arks:
                self._success_fh.write(f"{ark}\t{issue_id}\n")
                carried += 1
        if carried:
            self._success_fh.flush()
            logger.info(
                "Carried forward %d prior success entr%s into %s.",
                carried,
                "y" if carried == 1 else "ies",
                success_path,
            )
        logger.info("Report files: %s, %s (truncated).", success_path, failed_path)

    # --- resume decision & seeding ---

    def should_process(self, ark: str) -> bool:
        """Whether ``ark`` should be fetched/downloaded this run (vs skipped as already done)."""
        if self.retry_failed_only:
            return ark in self._prior_failed
        return ark not in self._prior_success

    def prior_issue_id(self, ark: str) -> str | None:
        """The canonical issue_id recorded for ``ark`` in a prior run, or ``None``.

        Checks prior successes first, then prior (partial) failures — the latter carry an issue_id
        whenever the manifest parsed, which is exactly when an on-disk ``manifest.json`` exists to
        reuse. Used for edition seeding and for resolving the reusable manifest path on resume.
        """
        if ark in self._prior_success:
            return self._prior_success[ark]
        entry = self._prior_failed.get(ark)
        return entry.get("issue_id") if entry else None

    # --- write entries (called from coroutines; non-awaiting → atomic under the event loop) ---

    def write_success(self, ark: str, issue_id: str) -> None:
        self._counts["success"] += 1
        if self._success_fh is not None:
            self._success_fh.write(f"{ark}\t{issue_id}\n")
            self._success_fh.flush()

    def write_failure(
        self,
        ark: str,
        issue_id: str | None,
        alias: str,
        num_pages: int,
        pages_ok: int,
        errors: list[dict],
    ) -> None:
        """Record a failed issue. ``errors`` = list of ``{"page": int|None, "error": str}``."""
        self._counts["failed"] += 1
        if self._failed_fh is None:
            return
        entry = {
            "ark": ark,
            "issue_id": issue_id,
            "alias": alias,
            "status": "failed",
            "num_pages": num_pages,
            "pages_ok": pages_ok,
            "failed_pages": [e["page"] for e in errors if e.get("page") is not None],
            "errors": errors,
            "timestamp": datetime.now().isoformat(),
        }
        self._failed_fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self._failed_fh.flush()

    def write_throttle_halt(self, info: dict) -> None:
        """Record the throttle-halt point so the resume context is on the record (not resumable)."""
        if self._failed_fh is None:
            return
        entry = {
            "status": "throttle_halt",
            "code": info.get("code"),
            "nextAccessTime": info.get("nextAccessTime"),
            "timestamp": datetime.now().isoformat(),
        }
        self._failed_fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self._failed_fh.flush()

    def add_resumed(self, count: int) -> None:
        self._counts["resumed"] += count

    # --- context manager & cleanup ---

    def __enter__(self) -> "DownloadReport":
        return self

    def __exit__(self, *_exc) -> None:
        self.log_summary()
        self.close()

    def log_summary(self) -> None:
        logger.info(
            "Report summary: %d success, %d failed, %d resumed (skipped).",
            self._counts["success"],
            self._counts["failed"],
            self._counts["resumed"],
        )

    def close(self) -> None:
        for attr in ("_success_fh", "_failed_fh"):
            fh = getattr(self, attr)
            if fh is not None:
                fh.close()
                setattr(self, attr, None)


def main(
    # --- inputs (default to the dhlab sync location; override locally) ---
    titles_csv: str = f"{INFO_DIR}/titles_to_download.csv",
    arks_dir: str = f"{INFO_DIR}/arks_num_per_ark_bib",
    target_base_dir: str = "/mnt/project_impresso_rw/original/BNF",
    cache_dir: str = "/mnt/project_impresso_rw/original/BNF/.manifest_cache",
    # --- filtering (comma-separated aliases; "" = all titles) ---
    aliases: str = "",
    exclude: str = "",
    # --- rate limit / concurrency / retries (Step 3; BnF quota per Ludovic: 1000/min/IP all APIs,
    #     manifest bucket 50/min, ALTO gets the rest) ---
    manifest_rate_per_min: float = 50.0,  # BnF's presentation-API cap
    alto_rate_per_min: float = 900.0,  # the rest of the 1000/min budget (headroom for token POSTs)
    manifest_concurrency: int = 4,  # small: 50/min needs little in-flight parallelism
    alto_concurrency: int = 20,
    prefetch: int = 200,  # token-queue depth (backpressure only; tokens are tiny)
    throttle_sleep_threshold_min: float = 15.0,  # WSO2 wait <= this -> sleep; else checkpoint+exit
    timeout: float = 30.0,
    max_retries: int = 5,
    # --- resume / report (Step 7) ---
    report_dir: str = "/rcp-scratch/journe/experiments/bnf_api_query/reports/",
    prior_report_dir: str = "",
    retry_failed_only: bool = False,
    # --- behaviour ---
    overwrite: bool = False,
    recompute_manifest: bool = False,  # re-fetch manifest.json even if already on disk
    dry_run: bool = True,  # SAFE DEFAULT: nothing is written
    # --- logging ---
    log_level: str = "INFO",
    log_file: str = "/rcp-scratch/journe/experiments/bnf_api_query/logs/run1.log",
) -> None:
    """Download new BNF titles from the BnF IIIF API onto the NAS.

    Every setting is a CLI argument with a default — there is no config file. Credentials are
    read from the ``BNF_API_KEY`` / ``BNF_API_SECRET`` environment variables.

    Args:
        titles_csv: Path to ``titles_to_download.csv`` (cols incl. ``ARK ID``, ``Alias``).
        arks_dir: Directory of ``{cb_ark}.txt`` files (one issue ark per line).
        target_base_dir: NAS root under which ``{alias}/{YYYY}/{MM}/{DD}/{edition}/`` is written.
        cache_dir: Persistent ark-keyed manifest cache (``{cache_dir}/{alias}/{ark}.json``) — a
            transparent check-cache-then-API layer; a resume/throttle-halt reuses it with 0 re-fetch.
        aliases: Comma-separated alias include-list (empty = all titles).
        exclude: Comma-separated alias exclude-list.
        manifest_rate_per_min: Rate cap for manifest (presentation-API) requests. BnF caps this at
            50/min — a separate, much tighter bucket than ALTO.
        alto_rate_per_min: Rate cap for ALTO requests. Uses the rest of the 1000/min/IP budget
            (default 900, leaving headroom for token POSTs). manifest+alto should stay under 1000.
        manifest_concurrency: Max in-flight manifest requests (asyncio.Semaphore).
        alto_concurrency: Max in-flight ALTO requests (asyncio.Semaphore).
        prefetch: Token-queue depth — how many issue tokens may wait ahead of the ALTO workers.
            Backpressure only; the queue holds tiny (tw, ark, issue_id) tokens, not manifests, so
            memory is negligible at any value.
        throttle_sleep_threshold_min: On a WSO2 throttle, sleep if the wait is within this many
            minutes; otherwise checkpoint and exit 75 (resume later).
        timeout: Per-request timeout in seconds.
        max_retries: Retry attempts for transient network/5xx errors.
        report_dir: Directory for this run's resume report (``success.txt`` / ``failed.jsonl``).
            Empty = no report written (resume then relies only on on-disk skip-if-exists).
        prior_report_dir: Directory of a previous run's report to resume from — its successes are
            skipped (not re-fetched) and carried forward. To resume a throttle-halted run, pass the
            same path as ``report_dir``.
        retry_failed_only: With ``prior_report_dir``, process *only* the arks that failed before.
        overwrite: Re-download pages even if a non-empty target already exists.
        recompute_manifest: Re-fetch each issue's manifest.json even if one is already on disk
            (default reuses the on-disk copy — resolvable on resume via the report's issue_id).
        dry_run: When True (default), fetch/parse but write nothing.
        log_level: One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
        log_file: Log destination. Empty = stdout.
    """
    # --- validate (fail fast, clear messages) ---
    errors = []
    if not os.path.isfile(titles_csv):
        errors.append(f"titles_csv not found: {titles_csv}")
    if not os.path.isdir(arks_dir):
        errors.append(f"arks_dir not found: {arks_dir}")
    if log_level.upper() not in VALID_LOG_LEVELS:
        errors.append(f"log_level must be one of {VALID_LOG_LEVELS}, got '{log_level}'")
    if manifest_rate_per_min <= 0:
        errors.append(f"manifest_rate_per_min must be > 0, got {manifest_rate_per_min}")
    if alto_rate_per_min <= 0:
        errors.append(f"alto_rate_per_min must be > 0, got {alto_rate_per_min}")
    if manifest_concurrency < 1:
        errors.append(f"manifest_concurrency must be >= 1, got {manifest_concurrency}")
    if alto_concurrency < 1:
        errors.append(f"alto_concurrency must be >= 1, got {alto_concurrency}")
    if max_retries < 0:
        errors.append(f"max_retries must be >= 0, got {max_retries}")
    if timeout <= 0:
        errors.append(f"timeout must be > 0, got {timeout}")
    if throttle_sleep_threshold_min < 0:
        errors.append(
            f"throttle_sleep_threshold_min must be >= 0, got {throttle_sleep_threshold_min}"
        )
    if errors:
        for err in errors:
            print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)

    # --- logging (mirror structure_media.main): file + ERROR-to-stderr when logging to a file ---
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    level = getattr(logging, log_level.upper())
    init_logger(logger, level, log_file or None)

    fmt = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    if log_file:
        stderr_handler = logging.StreamHandler()
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(fmt)
        logger.addHandler(stderr_handler)
    else:
        # Logging to the console: swap init_logger's stderr StreamHandler for a tqdm-aware one so
        # log lines route through tqdm.write and don't smear the progress bar (also on stderr).
        for handler in list(logger.handlers):
            if type(handler) is logging.StreamHandler:
                logger.removeHandler(handler)
        tqdm_handler = _TqdmLoggingHandler()
        tqdm_handler.setFormatter(fmt)
        logger.addHandler(tqdm_handler)

    # --- startup banner (unconditional stdout, so the operator sees where output lands) ---
    print(f"Log file:   {log_file or 'stdout'}", flush=True)
    print(f"Report dir: {report_dir or '(none — no report written)'}", flush=True)
    if prior_report_dir:
        print(
            f"Resume from: {prior_report_dir}"
            f"{' (retry failed only)' if retry_failed_only else ''}",
            flush=True,
        )
    print(f"Dry run:    {dry_run}", flush=True)
    print(flush=True)

    aliases_include = _split_csv(aliases)
    aliases_exclude = _split_csv(exclude)

    _log_settings(
        {
            "titles_csv": titles_csv,
            "arks_dir": arks_dir,
            "target_base_dir": target_base_dir,
            "cache_dir": cache_dir,
            "aliases_include": aliases_include,
            "aliases_exclude": aliases_exclude,
            "manifest_rate_per_min": manifest_rate_per_min,
            "alto_rate_per_min": alto_rate_per_min,
            "manifest_concurrency": manifest_concurrency,
            "alto_concurrency": alto_concurrency,
            "prefetch": prefetch,
            "throttle_sleep_threshold_min": throttle_sleep_threshold_min,
            "timeout": timeout,
            "max_retries": max_retries,
            "report_dir": report_dir,
            "prior_report_dir": prior_report_dir,
            "retry_failed_only": retry_failed_only,
            "overwrite": overwrite,
            "recompute_manifest": recompute_manifest,
            "dry_run": dry_run,
            "log_level": log_level.upper(),
            "log_file": log_file or "stdout",
            "token_url": TOKEN_URL,
            "manifest_tmpl": MANIFEST_TMPL,
        }
    )

    # BnF's global cap is 1000 req/min/IP across all APIs (token POSTs included). Warn — not fatal —
    # if the manifest + ALTO rates alone already exceed it.
    if manifest_rate_per_min + alto_rate_per_min > 1000:
        logger.warning(
            "manifest_rate_per_min (%s) + alto_rate_per_min (%s) = %s exceeds BnF's 1000/min/IP "
            "cap — expect throttling.",
            manifest_rate_per_min,
            alto_rate_per_min,
            manifest_rate_per_min + alto_rate_per_min,
        )

    # --- Step 2: build the work list ---
    work = build_work_list(titles_csv, arks_dir, aliases_include, aliases_exclude)
    if not work:
        logger.warning("No titles to process — nothing to do.")
        return

    # Credentials are required for any run (even --dry_run fetches manifests). Fail fast so a
    # missing token doesn't surface as one "failure" per issue. Values are never logged.
    missing_creds = [v for v in ("BNF_API_KEY", "BNF_API_SECRET") if not os.environ.get(v)]
    if missing_creds:
        logger.error(
            "Credentials not set: %s. `source ~/.env` on dhlab (the API is IP-whitelisted "
            "there) before running.",
            ", ".join(missing_creds),
        )
        sys.exit(1)

    # --- Step 7: resume report (built + prepared before any request so refuse-clobber fails fast) ---
    report = DownloadReport(
        report_dir=report_dir,
        prior_report_dir=prior_report_dir,
        retry_failed_only=retry_failed_only,
        dry_run=dry_run,
    )
    scope_arks = {ark for tw in work for ark in tw.issue_arks}
    try:
        report.prepare(scope_arks)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    # --- Steps 3–6: download & save ---
    with report:
        try:
            asyncio.run(
                download_all(
                    work,
                    target_base_dir,
                    cache_dir,
                    dry_run=dry_run,
                    overwrite=overwrite,
                    recompute_manifest=recompute_manifest,
                    manifest_rate_per_min=manifest_rate_per_min,
                    alto_rate_per_min=alto_rate_per_min,
                    manifest_concurrency=manifest_concurrency,
                    alto_concurrency=alto_concurrency,
                    max_retries=max_retries,
                    timeout=timeout,
                    throttle_sleep_threshold_min=throttle_sleep_threshold_min,
                    report=report,
                    prefetch=prefetch,
                )
            )
        except ThrottleHalt as halt:
            report.write_throttle_halt(halt.info)
            logger.error(
                "Throttled until %s — progress saved (%s); re-run with prior_report_dir to resume.",
                halt.info.get("nextAccessTime", "?"),
                report_dir or "on-disk files",
            )
            sys.exit(75)  # EX_TEMPFAIL — a scheduler/operator can re-invoke to resume
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Atomic writes + per-record flushed report → the on-disk state is consistent; a rerun
            # with prior_report_dir picks up where this left off.
            logger.warning(
                "Interrupted — progress saved (%s); re-run with prior_report_dir to resume.",
                report_dir or "on-disk files",
            )
            sys.exit(130)  # 128 + SIGINT
        except RuntimeError as exc:  # e.g. missing credentials from the token fetch
            logger.error("%s", exc)
            sys.exit(1)


if __name__ == "__main__":
    fire.Fire(main)
