# Chronicling America (Library of Congress) Newspaper Importer

This package contains the pipeline modules and scripts to download, detect, and import newspaper issues from the Library of Congress's **Chronicling America** archive into the Impresso canonical format.

**Agent / developer knowledge base:** [`.cursor/docs/chronicling-america.md`](../../../.cursor/docs/chronicling-america.md)

---

## Submodule Structure

- `classes.py`: Parser models for Chronicling America issues and pages (subclasses of `MetsAltoCanonicalIssue` / `MetsAltoCanonicalPage`).
- `detect.py`: Scans the local filesystem to discover issues and registers the `LOC` provider dynamically.
- `bulk.py`: Resumable bulk downloader (OCR tarballs + METS crawl).
- `fetch_data.py`: CLI for bulk and legacy single-title downloads.
- `chronicling_america_importer.py`: Main launcher under `text_preparation/importer_scripts/`.

Title registry config: `text_preparation/config/download_config/chronicling_america_titles.json`

---

## LOC site migration & access constraints (read this first)

In 2025–2026 the Library of Congress migrated Chronicling America onto the
`loc.gov` platform. This has direct consequences for automated downloading:

| Host | Use | Status for automated clients |
|---|---|---|
| `chroniclingamerica.loc.gov/data/ocr/ocr.json` | OCR tarball manifest | ✅ 200 (works) |
| `chroniclingamerica.loc.gov/data/batches/…` | Batch/issue directory crawl | ✅ 200 (works, but rate-limited) |
| `www.loc.gov/…?fo=json` | loc.gov JSON API (search + item records) | ❌ **403 Cloudflare "Just a moment…" challenge** |
| `tile.loc.gov/storage-services/…` | CDN for API-resolved file URLs | ❌ 403 |

**Key takeaways:**

- The **bulk downloader (`--config`) is the supported path.** It only talks to
  `chroniclingamerica.loc.gov`, which still serves the OCR manifest, batch
  directory listings, and the `.tar.bz2` archives.
- The **loc.gov JSON API mode is currently blocked.** `www.loc.gov` sits behind
  a Cloudflare bot challenge that returns `403` for any non-browser client
  (verified for both bot and real-browser User-Agents). The legacy single-issue
  `api.py` path therefore does **not** work for unattended runs at the moment.
- `/ocr.json` (legacy) now permanently redirects to a `404`; the live manifest is
  at **`/data/ocr/ocr.json`** with a new schema (see Bulk Downloader Design).

---

## How to Run

### 1. Bulk download on a server (recommended)

Run the downloader **directly on the server** (not locally over SSH). Use `tmux` so the job survives disconnects.

> **Use writable paths.** `--output-dir`, `--state-dir`, and the scratch dir must
> be writable by your user. On shared clusters `/data` is usually **not** writable
> (`PermissionError: [Errno 13] Permission denied: '/data'`); point everything at
> your scratch space instead, e.g. `/rcp-scratch/students/<project>/ca/…`.

```bash
# Clone and install on the server
git clone <repo-url> impresso-text-acquisition
cd impresso-text-acquisition
git checkout dev/chronicling-america
pip install -e .

# Pick a writable location once
export CA_ROOT=/rcp-scratch/students/impresso-CA-pilot/ca
mkdir -p "$CA_ROOT/raw" "$CA_ROOT/state"

# Inspect disk needs first (instant: estimate comes from the OCR manifest, no crawl)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/config/download_config/chronicling_america_titles.json \
  --output-dir "$CA_ROOT/raw" \
  --state-dir "$CA_ROOT/state" \
  --dry-run

# Run the download (resumable)
tmux new -s ca-download
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/config/download_config/chronicling_america_titles.json \
  --output-dir "$CA_ROOT/raw" \
  --state-dir "$CA_ROOT/state" \
  --workers 6 \
  --delay 3.0
```

A dry-run prints something like:

```
INFO - Built batch index from OCR manifest metadata for 2582 batches (no crawl)
INFO - Dry-run: estimated up to 39016 issues from OCR manifest (batch-level upper bound)
Chronicling America bulk download plan
- eveningstar [sn83045462]
Batches: 125
Tarballs: 125 (134.8 GB compressed)
Issues (METS files): ~39016 (OCR manifest estimate, batch-level upper bound)
```

Add titles by editing the config JSON. **Always set a date range for a first
run** — a single title with no range can mean tens of thousands of issues and
100+ GB of tarballs (Evening Star with no range ≈ 39 k issues / ~135 GB):

```json
{
  "titles": [
    {
      "lccn": "sn83045462",
      "alias": "eveningstar",
      "start_date": "1932-01-01",
      "end_date": "1932-12-31"
    }
  ]
}
```

Re-running the same command resumes from `state-dir/download_state.json`.

> **Rate limits.** `chroniclingamerica.loc.gov` throttles aggressively
> (we observed `429` after ≈60 directory requests in a minute). The HTTP client
> enforces a single global delay between requests (serialized across worker
> threads) and, on `429`, backs off for at least 60 s (or the `Retry-After`
> value). The default `--delay` is **3.0 s** (≈20 req/min). Lower it cautiously.

### 2. Download a single sample issue (local dev or server)

> **Currently degraded.** The default single-issue mode resolves files through the
> loc.gov JSON API (`www.loc.gov`), which is now behind a Cloudflare challenge and
> returns `403` to automated clients (see the access-constraints table above). The
> command below will fail until LOC relaxes that protection (or browser-grade
> challenge solving is added).

```bash
# May 403 due to the loc.gov Cloudflare challenge:
python -m text_preparation.importers.chronicling_america.fetch_data \
  --output-dir text_preparation/data/sample_data \
  --lccn sn83045462 \
  --alias eveningstar \
  --date 1932-06-20
```

For a working single-issue download today, use the batch-directory crawler, which
talks only to `chroniclingamerica.loc.gov`:

```bash
python -m text_preparation.importers.chronicling_america.fetch_data \
  --output-dir text_preparation/data/sample_data \
  --lccn sn83045462 \
  --alias eveningstar \
  --date 1932-06-20 \
  --use-crawl
```

### 3. Import to canonical format

```bash
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
python -m text_preparation.importer_scripts.chronicling_america_importer \
  --input-dir /data/ca/raw \
  --output-dir /data/ca/canonical \
  --provider LOC \
  --clear \
  --verbose
```

### 4. Run tests

```bash
pytest -vv tests/importers/test_ca_downloader.py   # bulk downloader (mocked, no network)
pytest -vv tests/importers/test_ca_api.py          # loc.gov API helpers (mocked)
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
pytest -vv tests/importers/test_chronicling_america_importer.py
```

---

## Bulk Downloader Design

1. **OCR manifest** (`fetch_ocr_tarballs`): fetched from
   **`chroniclingamerica.loc.gov/data/ocr/ocr.json`**, falling back to the legacy
   `…/ocr.json` URL. Both manifest shapes are parsed (`parse_ocr_manifest`):
   - *new (post-migration):* a JSON **list** of objects with
     `batch`, `archive_name`, `url`, `sha1`, `size`, `lccns`, `issue_count`;
   - *legacy:* `{"ocr": [{"name": "batch_<…>.tar.bz2", "url", "sha1", "size"}]}`.
2. **Batch index** (`build_or_load_batch_index`): cached JSON mapping batches to
   LCCNs (`state-dir/batch_index.json`). It is now **built from the manifest's
   per-batch `lccns`** (`index_from_tarball_manifest`) — no directory crawling, so
   indexing all ~2,600 batches is instant. Only batches missing from the manifest
   fall back to a `/data/{batch}/data/` crawl.
3. **Batch selection** (`batches_for_lccns`): all batches whose manifest `lccns`
   include a configured LCCN; highest `_verNN` kept when duplicate families exist.
4. **Dry-run estimate**: issue counts come straight from the manifest
   `issue_count` field (summed over selected batches). This is a **batch-level
   upper bound** (not date-filtered, and a batch may bundle several LCCNs) and
   costs **zero extra HTTP requests**. It deliberately avoids `www.loc.gov`.
5. **Issue enumeration (real run only)** (`list_issues_in_batch`): crawls
   `/data/batches/{batch}/data/{lccn}/{reel}/` listings on
   `chroniclingamerica.loc.gov`, filtering by the title's date range.
6. **ALTO via tarballs** (`process_tarball`): downloads `.tar.bz2` from the
   manifest `url`, verifies SHA-1, stream-extracts ALTO XML for selected LCCNs.
7. **METS crawl** (`download_issue_mets`): downloads one METS file per issue from
   `/data/batches/{batch}/data/{lccn}/{reel}/{issue}/{issue}.xml`.
8. **Layout** (`write_issue_layout`): writes `alias/YYYY/MM/DD/ed-N/{issue}.xml`
   and `alto/{href}.xml`, renaming tarball `seq-N/ocr.xml` paths to METS
   `fileSec` href names.
9. **Resume state**: `state-dir/download_state.json` records completed tarballs
   (by SHA-1) and completed issues; re-running resumes.

### Networking robustness (`HttpClient`)

- A single **global delay is enforced inside the request lock**, so concurrent
  worker threads can never burst past the configured rate (previously the sleep
  happened outside the lock and 6 workers fired near-simultaneously).
- Transient statuses (`429` and all `5xx`, including Cloudflare `52x` such as
  `525`) are retried with exponential backoff. On `429` the client waits at least
  **60 s** (LOC blocks for up to an hour) or honors the `Retry-After` header.
- `404` is not retried (callers use it to probe existence).

Output layout matches what the importer expects:

```
$CA_ROOT/raw/eveningstar/1932/06/20/ed-1/
  1932062001.xml
  alto/0567.xml
  alto/0568.xml
  ...
```

---

## Parser Notes

- ALTO coordinates are in `inch1200` units; divide by 3 for 400 DPI pixels.
- No OLR/article structure: page-level content items plus separate image/table items.
- `LOC` provider and aliases are registered at detect time in `impresso_essentials`.
