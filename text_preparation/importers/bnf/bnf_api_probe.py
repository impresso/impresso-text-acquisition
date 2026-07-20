#!/usr/bin/env python3
"""Probe the BnF authenticated IIIF API's rate limit, collect metrics, estimate corpus cost.

Standalone companion to ``fetch_data_via_API.py`` (the downloader). It answers three questions:

1. **What is the exact rate limit?** — a gentle, bounded active measurement (the BnF WSO2 gateway
   throttles with an HTTP 429 + a JSON body carrying a French ``nextAccessTime``; whether it also
   emits ``X-RateLimit-*`` headers is unknown, so we capture headers *and* measure by trial).
2. **Metrics to report to BnF** (contact: Ludovic) — throttle behaviour (codes/body/headers),
   per-endpoint latency percentiles (p50/p95/p99), reliability tallies.
3. **How long to download the whole corpus?** — from a small stratified sample (real avg pages/issue
   and avg ALTO bytes) × the 719,607 issues, at the measured rate and hypothetical higher tiers.

Design choices (per the plan): probe **quick** (per-minute window + burst, no hourly/daily hunt) and
**gentle** (serial concurrency-1 measurement, bounded burst ramp ≤ ``max_burst``, never a wide
instantaneous spike). It trips the throttle only a handful of times to read the limit, and halts
gracefully if the key gets blocked (WSO2 900805).

Reuses the downloader's primitives (auth, throttle detection, manifest parsing, work-list building)
— see the imports below — so nothing is duplicated. Live runs are **dhlab-only + Adrien-initiated**
(the API is IP-whitelisted there); local dev is offline against ``httpx.MockTransport``.

Credentials come from the environment (never hardcoded / synced / committed):
    BNF_API_KEY, BNF_API_SECRET

Example (dhlab)::

    conda activate cpu && set -a && source ~/.env && set +a
    python3 bnf_api_probe.py --out_dir /rcp-scratch/journe/experiments/bnf_api_query/probe \\
        --log_file /rcp-scratch/journe/experiments/bnf_api_query/probe.log
"""

import asyncio
import itertools
import json
import logging
import os
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import monotonic

import fire
import httpx

from impresso_essentials.utils import init_logger

# Reuse the downloader's primitives — do NOT duplicate auth/throttle/parse logic.
from fetch_data_via_API import (
    INFO_DIR,
    MANIFEST_TMPL,
    TOKEN_URL,
    VALID_LOG_LEVELS,
    TokenManager,
    build_work_list,
    parse_manifest,
    parse_next_access,
    wso2_throttle,
)

logger = logging.getLogger(__name__)

# Grounding fallbacks for the corpus estimate if a byte sample is unavailable (from the local
# sample: manifest ≈ 19 KB, ALTO ≈ 967 KB — see fixtures/README.md).
FALLBACK_MANIFEST_BYTES = 19_000
FALLBACK_ALTO_BYTES = 967_000


class KeyBlocked(Exception):
    """Raised when the gateway returns WSO2 900805 (blocked) — stop probing immediately."""

    def __init__(self, info: dict):
        self.info = info
        super().__init__(f"Key blocked (code {info.get('code', '?')})")


# ---------------------------------------------------------------------------
# Metrics accumulation
# ---------------------------------------------------------------------------


def _percentile(vals_sorted: list[float], p: float) -> float | None:
    """Linear-interpolated percentile of an already-sorted list (``p`` in 0..100)."""
    n = len(vals_sorted)
    if n == 0:
        return None
    if n == 1:
        return vals_sorted[0]
    k = (n - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, n - 1)
    return vals_sorted[lo] + (vals_sorted[hi] - vals_sorted[lo]) * (k - lo)


@dataclass
class EndpointStats:
    """Latency + outcome tallies for one endpoint (token / manifest / alto)."""

    latencies: list[float] = field(default_factory=list)
    n_success: int = 0
    n_throttle: int = 0
    n_5xx: int = 0
    n_other: int = 0
    bytes_total: int = 0

    def record(self, status: int, elapsed: float, size: int, is_throttle: bool) -> None:
        self.latencies.append(elapsed)
        self.bytes_total += size
        if is_throttle:
            self.n_throttle += 1
        elif status >= 500:
            self.n_5xx += 1
        elif status == 200:
            self.n_success += 1
        else:
            self.n_other += 1

    def summary(self) -> dict:
        s = sorted(self.latencies)
        lat = {f"p{q}_ms": (round(_percentile(s, q) * 1000, 1) if s else None) for q in (50, 90, 95, 99)}
        lat["max_ms"] = round(max(s) * 1000, 1) if s else None
        lat["min_ms"] = round(min(s) * 1000, 1) if s else None
        return {
            "n_requests": len(s),
            "n_success": self.n_success,
            "n_throttle": self.n_throttle,
            "n_5xx": self.n_5xx,
            "n_other": self.n_other,
            "bytes_total": self.bytes_total,
            "latency": lat,
        }


class Recorder:
    """Writes one JSONL row per request and accumulates per-endpoint stats + header findings."""

    def __init__(self, jsonl_path: str | None):
        self._fh = open(jsonl_path, "w", encoding="utf-8") if jsonl_path else None
        self.by_endpoint: dict[str, EndpointStats] = {}
        self.first_success_headers: dict | None = None
        self.first_throttle_headers: dict | None = None
        self.first_throttle_body: dict | None = None
        self.ratelimit_headers_present = False
        self.ratelimit_headers_sample: dict | None = None
        self.retry_after_present = False
        self.retry_after_sample: str | None = None

    def record(self, endpoint: str, url: str, resp: httpx.Response, elapsed: float,
               throttle: dict | None) -> None:
        status = resp.status_code
        size = len(resp.content)
        self.by_endpoint.setdefault(endpoint, EndpointStats()).record(
            status, elapsed, size, throttle is not None
        )

        headers = dict(resp.headers)
        lower = {k.lower(): v for k, v in headers.items()}
        rl = {k: v for k, v in headers.items() if k.lower().startswith(("x-ratelimit", "ratelimit"))}
        if rl:
            self.ratelimit_headers_present = True
            self.ratelimit_headers_sample = self.ratelimit_headers_sample or rl
        if "retry-after" in lower:
            self.retry_after_present = True
            self.retry_after_sample = self.retry_after_sample or lower["retry-after"]
        if status == 200 and self.first_success_headers is None:
            self.first_success_headers = headers
        if throttle is not None and self.first_throttle_headers is None:
            self.first_throttle_headers = headers
            self.first_throttle_body = throttle

        if self._fh:
            self._fh.write(json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "endpoint": endpoint,
                "url": url,
                "status": status,
                "latency_s": round(elapsed, 4),
                "bytes": size,
                "throttle_code": throttle.get("code") if throttle else None,
                "next_access": throttle.get("nextAccessTime") if throttle else None,
            }, ensure_ascii=False) + "\n")
            self._fh.flush()

    def header_summary(self) -> dict:
        return {
            "x_ratelimit_headers_present": self.ratelimit_headers_present,
            "x_ratelimit_headers_sample": self.ratelimit_headers_sample,
            "retry_after_present": self.retry_after_present,
            "retry_after_sample": self.retry_after_sample,
            "first_success_headers": self.first_success_headers,
            "first_throttle_headers": self.first_throttle_headers,
            "first_throttle_body": self.first_throttle_body,
        }

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


# ---------------------------------------------------------------------------
# Raw timed client (bypasses the downloader's rate limiter / retry, on purpose)
# ---------------------------------------------------------------------------


class Probe:
    """Issues raw, individually-timed GETs with a bearer token; records every response."""

    def __init__(self, client: httpx.AsyncClient, tokens: TokenManager, rec: Recorder):
        self.client = client
        self.tokens = tokens
        self.rec = rec

    async def _get(self, endpoint: str, url: str) -> tuple[httpx.Response, float, dict | None]:
        token = await self.tokens.token()  # cached; refreshed only when expired
        t0 = monotonic()
        resp = await self.client.get(url, headers={"Authorization": f"Bearer {token}"})
        elapsed = monotonic() - t0
        throttle = wso2_throttle(resp)
        self.rec.record(endpoint, url, resp, elapsed, throttle)
        if throttle is not None and str(throttle.get("code", "")).startswith("900805"):
            raise KeyBlocked(throttle)
        return resp, elapsed, throttle

    async def manifest(self, ark: str):
        return await self._get("manifest", MANIFEST_TMPL.format(ark=ark))

    async def alto(self, url: str):
        return await self._get("alto", url)


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


def _reset_from_throttle(body: dict | None) -> datetime | None:
    """Parse a throttle body's ``nextAccessTime`` into a tz-aware datetime, or ``None``."""
    nat = body.get("nextAccessTime") if body else None
    if not nat:
        return None
    try:
        return parse_next_access(nat)
    except ValueError:
        return None


async def _sleep_until(when: datetime, epsilon: float = 1.0) -> None:
    """Sleep until ``when`` (+ a small epsilon so we land just inside the fresh window)."""
    delay = (when - datetime.now(timezone.utc)).total_seconds() + epsilon
    if delay > 0:
        logger.info("  sleeping %.1fs until window reset (%s)...", delay, when.isoformat())
        await asyncio.sleep(delay)


async def _count_until_throttle(
    probe: Probe, arks, min_interval: float, max_seconds: float = 150.0
) -> tuple[datetime | None, int, dict | None]:
    """Send manifests serially (paced ≥ ``min_interval`` apart) counting 200s until the first
    throttle. Returns ``(reset_dt, n_success, throttle_body)``; ``reset_dt``/body are ``None`` if no
    throttle occurred within ``max_seconds`` (i.e. the serial rate is under the limit)."""
    count = 0
    start = monotonic()
    while monotonic() - start < max_seconds:
        resp, elapsed, throttle = await probe.manifest(next(arks))
        if throttle is not None:
            return _reset_from_throttle(throttle), count, throttle
        if resp.status_code == 200:
            count += 1
        pause = min_interval - elapsed
        if pause > 0:
            await asyncio.sleep(pause)
    return None, count, None


# ---------------------------------------------------------------------------
# Probe phases
# ---------------------------------------------------------------------------


async def phase_limit(probe: Probe, arks, *, windows: int, max_probe_rate: float) -> dict:
    """Measure the per-window request limit L, window length W, and sustained rate = L/W.

    Serial (concurrency 1 — no spike), paced ≤ ``max_probe_rate``/s: trip a first throttle to find
    the reset, then measure a **window-aligned** success count (= L) over K windows.
    """
    logger.info("Phase 1/3 — per-window limit: finding the first throttle...")
    min_interval = 1.0 / max_probe_rate if max_probe_rate > 0 else 0.0
    reset, pre_count, body = await _count_until_throttle(probe, arks, min_interval)
    if reset is None:
        logger.warning(
            "No throttle at ~%.1f req/s serial — the limit is above the serial probe rate.",
            max_probe_rate,
        )
        return {
            "throttled": False,
            "note": "no throttle at serial max_probe_rate; limit >= observed count in one pass",
            "observed_count": pre_count,
            "sustained_rate_per_sec": None,
        }

    windows_data: list[dict] = []
    for k in range(windows):
        # Align to the reset, then count a full fresh window. If the very first request throttles
        # (L==0) we woke too early — clock skew between our host and the gateway, or a boundary race
        # — so re-align to the new reset and retry rather than recording a bogus 0.
        for _ in range(3):
            await _sleep_until(reset)
            boundary = datetime.now(timezone.utc)
            w_start = monotonic()
            reset2, count, body = await _count_until_throttle(probe, arks, min_interval)
            exhaust_s = monotonic() - w_start
            if count > 0 or reset2 is None:
                break
            logger.info("  window %d/%d: immediate re-throttle (mis-aligned) — retrying.",
                        k + 1, windows)
            reset = reset2
        window_len = (reset2 - boundary).total_seconds() if reset2 else None
        nat = body.get("nextAccessTime") if body else None
        top_of_minute = bool(reset2 and reset2.second == 0)
        windows_data.append({
            "L": count,
            "exhaust_s": round(exhaust_s, 2),
            "window_len_s": round(window_len, 1) if window_len else None,
            "reset_top_of_minute": top_of_minute,
            "next_access": nat,
        })
        logger.info("  window %d/%d: L=%d (exhausted in %.1fs, window≈%ss, top-of-minute=%s)",
                    k + 1, windows, count, exhaust_s,
                    round(window_len, 1) if window_len else "?", top_of_minute)
        if reset2 is None:
            break
        reset = reset2

    # A window that throttles on its very first request (L == 0) means we mis-aligned to the reset,
    # not a real measurement — exclude it from the averages.
    Ls = [w["L"] for w in windows_data if w["L"] > 0]
    Wlens = [w["window_len_s"] for w in windows_data if w["window_len_s"]]
    mean_L = statistics.mean(Ls) if Ls else None
    mean_W = statistics.mean(Wlens) if Wlens else None
    sustained = (mean_L / mean_W) if (mean_L is not None and mean_W) else None
    return {
        "throttled": True,
        "windows": windows_data,
        "L_mean": round(mean_L, 1) if mean_L is not None else None,
        "L_min": min(Ls) if Ls else None,
        "L_max": max(Ls) if Ls else None,
        "window_len_s_mean": round(mean_W, 1) if mean_W else None,
        "window_type": "fixed_top_of_minute" if all(w["reset_top_of_minute"] for w in windows_data)
        else "other/sliding",
        "throttle_code": body.get("code") if body else None,
        "sustained_rate_per_sec": round(sustained, 3) if sustained else None,
        "sustained_rate_per_min": round(sustained * 60, 1) if sustained else None,
    }


async def phase_burst(probe: Probe, arks, *, max_burst: int, window_limit: float | None) -> dict:
    """Fire one bounded concurrent burst in a *fresh* window to detect spike/burst control.

    Concurrency is capped **below the measured per-window limit** so a throttle here is genuinely
    spike-arrest (WSO2 900807), not quota exhaustion (900802). We first align to a fresh window (the
    limit phase just drained the current one), then fire ``cap`` requests at once.
    """
    cap = max_burst if window_limit is None else max(1, min(max_burst, int(window_limit) - 1))
    logger.info("Phase 2/3 — burst: firing %d concurrent in a fresh window (limit≈%s)...",
                cap, window_limit)
    # Align to a fresh window: one probe request; if it throttles, sleep to its reset.
    _, _, throttle = await probe.manifest(next(arks))
    if throttle is not None:
        reset = _reset_from_throttle(throttle)
        if reset:
            await _sleep_until(reset)

    picks = [next(arks) for _ in range(cap)]
    results = await asyncio.gather(*[probe.manifest(a) for a in picks], return_exceptions=True)
    for r in results:
        if isinstance(r, KeyBlocked):
            raise r
    throttles = [r for r in results if isinstance(r, tuple) and r[2] is not None]
    codes = sorted({str(r[2].get("code")) for r in throttles})
    spike = any(c == "900807" for c in codes)
    logger.info("  concurrency %d: %d throttled %s (spike-arrest=%s)",
                cap, len(throttles), codes or "—", spike)
    return {
        "concurrency_tested": cap,
        "n_throttled": len(throttles),
        "codes": codes,
        "spike_arrest_detected": spike,
        "note": "concurrency capped below the per-window limit, so a throttle here (900807) indicates "
                "spike/burst control rather than quota exhaustion (900802)",
    }


async def phase_corpus(
    probe: Probe, work, *, sample_per_title: int, alto_sample: int, sustained_rate: float | None
) -> dict:
    """Stratified sample (a few manifests per title + some ALTO) → avg pages/issue + avg bytes.

    Paced under the measured limit (≈0.9×sustained) so it doesn't throttle; if it does, it sleeps to
    the reset and retries once.
    """
    interval = 1.0 / (0.9 * sustained_rate) if sustained_rate else 1.5
    logger.info("Phase 3/3 — corpus sampling: %d/title across %d titles + %d ALTO (~%.2fs apart)...",
                sample_per_title, len(work), alto_sample, interval)
    page_counts: list[int] = []
    manifest_bytes: list[int] = []
    alto_sizes: list[int] = []
    alto_budget = alto_sample

    for tw in work:
        for ark in tw.issue_arks[:sample_per_title]:
            resp, _, throttle = await probe.manifest(ark)
            if throttle is not None:  # paced under the limit, but tolerate drift
                reset = _reset_from_throttle(throttle)
                if reset:
                    await _sleep_until(reset)
                resp, _, throttle = await probe.manifest(ark)
            if resp.status_code == 200:
                try:
                    meta = parse_manifest(resp.json(), ark)
                except Exception as exc:  # noqa: BLE001 — a bad manifest must not abort sampling
                    logger.debug("sample parse failed for %s: %s", ark, exc)
                else:
                    page_counts.append(len(meta.pages))
                    manifest_bytes.append(len(resp.content))
                    if alto_budget > 0 and meta.pages:
                        aresp, _, athrottle = await probe.alto(meta.pages[0].alto_url)
                        if athrottle is None and aresp.status_code == 200:
                            alto_sizes.append(len(aresp.content))
                            alto_budget -= 1
                        await asyncio.sleep(interval)
            await asyncio.sleep(interval)

    return {
        "n_issues_sampled": len(page_counts),
        "avg_pages": round(statistics.mean(page_counts), 2) if page_counts else None,
        "median_pages": statistics.median(page_counts) if page_counts else None,
        "min_pages": min(page_counts) if page_counts else None,
        "max_pages": max(page_counts) if page_counts else None,
        "avg_manifest_bytes": round(statistics.mean(manifest_bytes)) if manifest_bytes else None,
        "n_alto_sampled": len(alto_sizes),
        "avg_alto_bytes": round(statistics.mean(alto_sizes)) if alto_sizes else None,
        "min_alto_bytes": min(alto_sizes) if alto_sizes else None,
        "max_alto_bytes": max(alto_sizes) if alto_sizes else None,
    }


# ---------------------------------------------------------------------------
# Corpus estimate (pure)
# ---------------------------------------------------------------------------


def estimate_corpus(total_issues: int, corpus_sample: dict | None,
                    sustained_rate: float | None) -> dict:
    """Project total requests, bytes, and download time from the sampled averages + measured rate."""
    if not corpus_sample or not corpus_sample.get("avg_pages"):
        return {"total_issues": total_issues, "note": "no page sample — cannot estimate"}
    avg_pages = corpus_sample["avg_pages"]
    avg_mbytes = corpus_sample.get("avg_manifest_bytes") or FALLBACK_MANIFEST_BYTES
    avg_abytes = corpus_sample.get("avg_alto_bytes") or FALLBACK_ALTO_BYTES

    alto_reqs = total_issues * avg_pages
    total_reqs = total_issues + alto_reqs  # 1 manifest + avg_pages ALTO per issue
    total_bytes = total_issues * avg_mbytes + alto_reqs * avg_abytes

    scenarios = []
    for mult in (1, 2, 5, 10):
        r = sustained_rate * mult if sustained_rate else None
        secs = (total_reqs / r) if r else None
        scenarios.append({
            "tier": f"x{mult}" + (" (measured)" if mult == 1 else ""),
            "rate_per_min": round(r * 60, 1) if r else None,
            "days": round(secs / 86400, 1) if secs else None,
        })
    return {
        "total_issues": total_issues,
        "avg_pages_per_issue": round(avg_pages, 2),
        "total_alto_requests": round(alto_reqs),
        "total_requests": round(total_reqs),
        "total_bytes": round(total_bytes),
        "total_TB": round(total_bytes / 1e12, 2),
        "measured_sustained_rate_per_min": round(sustained_rate * 60, 1) if sustained_rate else None,
        "download_time_scenarios": scenarios,
        "note": "bottleneck is the request-count quota, not bandwidth; longer (hourly/daily) windows "
                "were NOT probed, so times are a lower bound.",
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _pick_probe_title(work, probe_title: str):
    """Choose the title whose arks drive phases 1–2: an explicit alias, else the smallest title with
    ≥ 200 issues (enough distinct arks to exhaust a window without repeating), else the largest."""
    if probe_title:
        for tw in work:
            if tw.alias == probe_title:
                return tw
        raise ValueError(f"probe_title {probe_title!r} not found in the work list")
    candidates = [tw for tw in work if len(tw.issue_arks) >= 200]
    return min(candidates, key=lambda t: len(t.issue_arks)) if candidates \
        else max(work, key=lambda t: len(t.issue_arks))


async def run_probe(work, probe_tw, *, out_dir, windows, max_probe_rate, max_burst,
                    sample_per_title, alto_sample, do_burst, do_corpus, timeout) -> dict:
    """Drive all phases against a single reused client + token, returning the full result dict."""
    rec = Recorder(os.path.join(out_dir, "metrics.jsonl"))
    result: dict = {
        "metadata": {
            "started": datetime.now(timezone.utc).isoformat(),
            "host": os.uname().nodename,
            "probe_title": probe_tw.alias,
            "endpoints": {"token": TOKEN_URL, "manifest": MANIFEST_TMPL},
            "params": {"windows": windows, "max_probe_rate": max_probe_rate, "max_burst": max_burst,
                       "sample_per_title": sample_per_title, "alto_sample": alto_sample},
        },
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        tokens = TokenManager(client, TOKEN_URL, os.environ.get("BNF_API_KEY"),
                              os.environ.get("BNF_API_SECRET"))
        probe = Probe(client, tokens, rec)
        arks = itertools.cycle(probe_tw.issue_arks)  # distinct within a window (title has ≥200)

        t0 = monotonic()
        await tokens.token()
        result["token_latency_s"] = round(monotonic() - t0, 4)

        try:
            result["limit"] = await phase_limit(probe, arks, windows=windows,
                                                max_probe_rate=max_probe_rate)
            sustained = result["limit"].get("sustained_rate_per_sec")
            if do_burst:
                result["burst"] = await phase_burst(
                    probe, arks, max_burst=max_burst,
                    window_limit=result["limit"].get("L_mean"))
            if do_corpus:
                result["corpus_sample"] = await phase_corpus(
                    probe, work, sample_per_title=sample_per_title, alto_sample=alto_sample,
                    sustained_rate=sustained)
        except KeyBlocked as blocked:
            result["blocked"] = blocked.info
            logger.error("Key blocked (900805) — stopping the probe and writing a partial report.")

    rec.close()
    result["headers"] = rec.header_summary()
    result["performance"] = {ep: st.summary() for ep, st in rec.by_endpoint.items()}
    total_issues = sum(len(tw.issue_arks) for tw in work)
    result["corpus_estimate"] = estimate_corpus(
        total_issues, result.get("corpus_sample"),
        result.get("limit", {}).get("sustained_rate_per_sec"))
    result["metadata"]["finished"] = datetime.now(timezone.utc).isoformat()
    return result


# ---------------------------------------------------------------------------
# Report writing
# ---------------------------------------------------------------------------


def _fmt(v, dash="—"):
    return dash if v is None else v


def write_reports(result: dict, out_dir: str) -> tuple[str, str]:
    """Write report.json (structured) and report.md (for BnF). Returns their paths."""
    json_path = os.path.join(out_dir, "report.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)

    md_path = os.path.join(out_dir, "report.md")
    m = result.get("metadata", {})
    lim = result.get("limit", {})
    hdr = result.get("headers", {})
    est = result.get("corpus_estimate", {})
    lines: list[str] = []
    lines.append("# BnF IIIF API — rate-limit & performance report\n")
    lines.append(f"- Measured: {m.get('started')} → {m.get('finished')}")
    lines.append(f"- Host (whitelisted): {m.get('host')}")
    lines.append(f"- Probe title: `{m.get('probe_title')}` · endpoints: manifest + ALTO on "
                 f"`openapiproext.bnf.fr`, token on `apimauthproext.bnf.fr`")
    lines.append("- Method: single reused token; serial (concurrency-1) window-aligned limit "
                 "measurement; bounded concurrency ramp; stratified corpus sample.\n")

    lines.append("## Measured rate limit\n")
    if lim.get("throttled"):
        lines.append(f"- **Per-window limit L ≈ {_fmt(lim.get('L_mean'))} requests** "
                     f"(min {_fmt(lim.get('L_min'))}, max {_fmt(lim.get('L_max'))} over "
                     f"{len(lim.get('windows', []))} windows)")
        lines.append(f"- **Window ≈ {_fmt(lim.get('window_len_s_mean'))} s** "
                     f"({lim.get('window_type')})")
        lines.append(f"- **Sustained rate ≈ {_fmt(lim.get('sustained_rate_per_min'))} req/min** "
                     f"({_fmt(lim.get('sustained_rate_per_sec'))} req/s)")
        lines.append(f"- Throttle fault code: `{_fmt(lim.get('throttle_code'))}`")
    else:
        lines.append(f"- No throttle at the serial probe rate — {lim.get('note')}")
    b = result.get("burst")
    if b:
        lines.append(f"- Burst: {b['n_throttled']}/{b['concurrency_tested']} concurrent throttled "
                     f"(codes {b['codes'] or '—'}); spike/burst control "
                     f"{'detected' if b.get('spike_arrest_detected') else 'not detected'} at that "
                     "concurrency.")
    if result.get("blocked"):
        lines.append(f"- ⚠️ Key was **blocked** (900805) mid-probe: `{result['blocked']}`")
    lines.append("")

    lines.append("## Throttle signalling\n")
    lines.append(f"- `X-RateLimit-*` headers present: **{hdr.get('x_ratelimit_headers_present')}**"
                 + (f" — sample `{hdr.get('x_ratelimit_headers_sample')}`"
                    if hdr.get("x_ratelimit_headers_sample") else ""))
    lines.append(f"- `Retry-After` header present: **{hdr.get('retry_after_present')}**"
                 + (f" (`{hdr.get('retry_after_sample')}`)" if hdr.get("retry_after_sample") else ""))
    lines.append(f"- Throttle body (verbatim): `{hdr.get('first_throttle_body')}`")
    lines.append("")

    lines.append("## Latency (per endpoint)\n")
    lines.append("| endpoint | n | p50 ms | p95 ms | p99 ms | max ms | throttled |")
    lines.append("|---|---|---|---|---|---|---|")
    for ep, s in result.get("performance", {}).items():
        lat = s["latency"]
        lines.append(f"| {ep} | {s['n_requests']} | {_fmt(lat['p50_ms'])} | {_fmt(lat['p95_ms'])} "
                     f"| {_fmt(lat['p99_ms'])} | {_fmt(lat['max_ms'])} | {s['n_throttle']} |")
    lines.append("")

    lines.append("## Corpus download estimate\n")
    if est.get("avg_pages_per_issue"):
        cs = result.get("corpus_sample", {})
        lines.append(f"- Issues (exact): **{est['total_issues']:,}**; avg pages/issue "
                     f"**{est['avg_pages_per_issue']}** (median {_fmt(cs.get('median_pages'))}, "
                     f"range {_fmt(cs.get('min_pages'))}–{_fmt(cs.get('max_pages'))}, "
                     f"n={cs.get('n_issues_sampled')})")
        lines.append(f"- Total requests: **{est['total_requests']:,}** "
                     f"({est['total_issues']:,} manifests + {est['total_alto_requests']:,} ALTO)")
        lines.append(f"- Est. total volume: **{est['total_TB']} TB** "
                     f"(avg ALTO {_fmt(cs.get('avg_alto_bytes'))} B, n={cs.get('n_alto_sampled')})")
        lines.append(f"- At the measured **{_fmt(est.get('measured_sustained_rate_per_min'))} "
                     "req/min**:\n")
        lines.append("| tier | rate req/min | download time (days) |")
        lines.append("|---|---|---|")
        for sc in est.get("download_time_scenarios", []):
            lines.append(f"| {sc['tier']} | {_fmt(sc['rate_per_min'])} | {_fmt(sc['days'])} |")
        lines.append(f"\n_{est.get('note')}_")
    else:
        lines.append(f"- {est.get('note')}")
    lines.append("\n## Ask\n")
    lines.append("The current quota makes a full harvest impractical (see the table). We request "
                 "either a substantially higher rate tier, or a bulk / offline export of the ALTO "
                 "for the listed titles.")

    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return json_path, md_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(
    titles_csv: str = f"{INFO_DIR}/titles_to_download.csv",
    arks_dir: str = f"{INFO_DIR}/arks_num_per_ark_bib",
    out_dir: str = "./bnf_probe",
    probe_title: str = "",
    windows: int = 3,
    max_probe_rate: float = 10.0,
    max_burst: int = 8,
    sample_per_title: int = 3,
    alto_sample: int = 100,
    do_burst: bool = True,
    do_corpus: bool = True,
    timeout: float = 30.0,
    log_level: str = "INFO",
    log_file: str = "",
) -> None:
    """Probe the BnF API rate limit, gather metrics, and estimate the full-corpus download cost.

    Writes ``metrics.jsonl``, ``report.json``, ``report.md`` under ``out_dir``. Credentials come from
    ``BNF_API_KEY`` / ``BNF_API_SECRET`` (never logged). Runs live only on the whitelisted dhlab host.

    Args:
        titles_csv: Path to ``titles_to_download.csv``.
        arks_dir: Directory of ``{cb_ark}.txt`` issue-ark files.
        out_dir: Output directory for the three artifacts.
        probe_title: Alias whose arks drive the limit/burst phases (default: smallest title ≥200 issues).
        windows: Number of window-aligned measurements of L (default 3).
        max_probe_rate: Serial probe pace cap in req/s (gentle; default 10).
        max_burst: Max concurrency in the burst ramp (kept low; default 8).
        sample_per_title: Manifests sampled per title for the corpus estimate (default 3).
        alto_sample: Total ALTO pages sampled for avg byte size (default 100).
        do_burst: Run the burst ramp phase.
        do_corpus: Run the corpus-sampling phase.
        timeout: Per-request timeout (s).
        log_level: One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
        log_file: Log destination (empty = stdout).
    """
    errors = []
    if not os.path.isfile(titles_csv):
        errors.append(f"titles_csv not found: {titles_csv}")
    if not os.path.isdir(arks_dir):
        errors.append(f"arks_dir not found: {arks_dir}")
    if log_level.upper() not in VALID_LOG_LEVELS:
        errors.append(f"log_level must be one of {VALID_LOG_LEVELS}, got '{log_level}'")
    if windows < 1:
        errors.append(f"windows must be >= 1, got {windows}")
    if max_probe_rate <= 0:
        errors.append(f"max_probe_rate must be > 0, got {max_probe_rate}")
    if max_burst < 1:
        errors.append(f"max_burst must be >= 1, got {max_burst}")
    if errors:
        for err in errors:
            print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)

    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    init_logger(logger, getattr(logging, log_level.upper()), log_file or None)
    os.makedirs(out_dir, exist_ok=True)

    print(f"Out dir:  {out_dir}", flush=True)
    print(f"Log file: {log_file or 'stdout'}", flush=True)
    print(flush=True)

    work = build_work_list(titles_csv, arks_dir, [], [])
    if not work:
        logger.warning("No titles — nothing to probe.")
        return

    missing = [v for v in ("BNF_API_KEY", "BNF_API_SECRET") if not os.environ.get(v)]
    if missing:
        logger.error("Credentials not set: %s. On dhlab: `set -a; source ~/.env; set +a` "
                     "(the file doesn't export). The API is IP-whitelisted to dhlab.",
                     ", ".join(missing))
        sys.exit(1)

    try:
        probe_tw = _pick_probe_title(work, probe_title)
    except ValueError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    logger.info("Probe title: %s (%d issues); corpus sample over %d titles.",
                probe_tw.alias, len(probe_tw.issue_arks), len(work))

    result = asyncio.run(run_probe(
        work, probe_tw, out_dir=out_dir, windows=windows, max_probe_rate=max_probe_rate,
        max_burst=max_burst, sample_per_title=sample_per_title, alto_sample=alto_sample,
        do_burst=do_burst, do_corpus=do_corpus, timeout=timeout))

    json_path, md_path = write_reports(result, out_dir)
    lim = result.get("limit", {})
    if lim.get("sustained_rate_per_min"):
        logger.info("MEASURED: ~%s req/min (L≈%s per %ss window).",
                    lim.get("sustained_rate_per_min"), lim.get("L_mean"),
                    lim.get("window_len_s_mean"))
    logger.info("Wrote %s, %s, and metrics.jsonl.", json_path, md_path)


if __name__ == "__main__":
    fire.Fire(main)
