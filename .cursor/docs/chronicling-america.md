# Chronicling America Integration — Project Knowledge Base

> **Location:** `.cursor/docs/chronicling-america.md` (agent knowledge base; applied via
> `.cursor/rules/chronicling-america.mdc` when editing CA files).
>
> Reference for AI agents and developers working on the Chronicling America (Library of
> Congress) importer inside `impresso-text-acquisition`. Last verified: 2026-06-15, branch
> `dev/chronicling-america`, all 42 CA downloader/API tests passing.

---

## 0. LOC migration & host access (2025–2026) — read first

The Library of Congress migrated Chronicling America onto the `loc.gov` platform.
This breaks several previously-documented endpoints. Verified live on 2026-06-15:

| Host / endpoint | Purpose | Status for scripts |
|---|---|---|
| `chroniclingamerica.loc.gov/data/ocr/ocr.json` | OCR tarball manifest | ✅ 200 |
| `chroniclingamerica.loc.gov/data/batches/…` | batch/issue directory crawl | ✅ 200 (rate-limited) |
| `chroniclingamerica.loc.gov/data/ocr/*.tar.bz2` | OCR tarballs | ✅ 200 |
| `chroniclingamerica.loc.gov/ocr.json` (legacy) | old manifest URL | ❌ redirects → 404 |
| `www.loc.gov/…?fo=json` | loc.gov JSON API (search/item) | ❌ 403 Cloudflare challenge |
| `tile.loc.gov/storage-services/…` | API-resolved file CDN | ❌ 403 |

Consequences:

- **Bulk mode (`--config`) is the only fully working path** — it touches only
  `chroniclingamerica.loc.gov`.
- **loc.gov JSON API mode (`api.py`, default legacy single-issue) is blocked**:
  `www.loc.gov` returns a Cloudflare "Just a moment…" `403` to every non-browser
  client (tested with both a bot and a real-browser User-Agent). Use `--use-crawl`
  for single-issue downloads instead.
- The OCR manifest **moved** to `/data/ocr/ocr.json` and changed schema (see §6).

---

## 1. Project context

This repository (`impresso-text-acquisition`, Python package `impresso-text-preparation`,
import name `text_preparation`) converts heterogeneous OCR/ASR source formats from many
content providers into two unified Impresso representations:

- **Canonical format**: JSON preserving the logical structure of each *Issue* and its
  *Physical Supports* (pages or audio records). Produced by the **importers**.
- **Rebuilt format**: one flat document per *Content Item* (article, ad, image, table...)
  for downstream ML/IR. Produced by the **rebuilders** (`text_preparation/rebuilders/`),
  reading canonical JSON from S3.

### Importer architecture (shared by all providers)

Every provider importer consists of three pieces wired together by a thin launcher:

1. **`detect_issues(base_dir, ...)` / `select_issues(base_dir, config)`** — scan the local
   filesystem and return a list of `IssueDir` named tuples
   (`provider`, `alias`, `date`, `edition`, `path`).
2. **An issue class** subclassing `text_preparation.importers.classes.CanonicalIssue`
   (for METS/ALTO sources: `MetsAltoCanonicalIssue` / `MetsAltoCanonicalPage` in
   `text_preparation/importers/mets_alto/classes.py`). The constructor calls the abstract
   `_find_pages()` and `_parse_mets()`, which subclasses implement.
3. **`text_preparation/importers/generic_importer.py` `main()`** — CLI parsing (docopt),
   Dask setup, manifest initialization, then calls
   **`text_preparation/importers/core.py` `import_issues()`**, which:
   - builds a Dask bag of issues, instantiates the issue class per `IssueDir`,
   - groups issues by `(alias, year)` and writes compressed yearly files
     (`compress_issues`), then page/audio files,
   - optionally uploads everything to S3 (`upload_issues`) and updates a
     `DataManifest` (from `impresso_essentials`).

Canonical IDs follow `alias-YYYY-MM-DD-e` (edition as a lowercase letter, `ed-1` → `a`
via `text_preparation.utils.edition_num_to_code`); pages are `…-pNNNN`, content items
`…-iNNNN`.

JSON schemas live in the `text_preparation/impresso-schemas` git submodule.

---

## 2. Chronicling America: what it is

[Chronicling America](https://chroniclingamerica.loc.gov) is the Library of Congress
(LOC) open archive of digitized historic US newspapers, in a **METS/ALTO** flavor with
**no OLR** (no article segmentation — only page-level OCR).

Key identifiers:

- **LCCN**: Library of Congress Control Number identifying a newspaper title
  (e.g. `sn83045462` = *The Evening Star*, Washington D.C.). Regex used in code:
  `^[a-z]{0,3}\d{8,12}$` (prefixed like `sn…`/`mn…` or purely numeric).
- **Batch**: digitization batches named `{family}_ver{NN}` (e.g. `dlc_ferguson_ver01`).
  A title's issues are scattered across many batches; the same issue can appear in
  several batch *versions* — always prefer the highest `_verNN`.
- **Issue directory name**: 10 digits `YYYYMMDDEE` (e.g. `1932062001` = 1932-06-20,
  edition 1).

Remote data layout (crawlable directory listing):

```
https://chroniclingamerica.loc.gov/data/batches/{batch}/data/{lccn}/{reel}/{YYYYMMDDEE}/
    {YYYYMMDDEE}.xml          <- per-issue METS
    0567.xml, 0568.xml, ...   <- per-page ALTO (filenames = METS fileSec hrefs)
```

Bulk OCR is also distributed as per-batch tarballs listed in the OCR manifest at
**`https://chroniclingamerica.loc.gov/data/ocr/ocr.json`** (post-migration; the old
`/ocr.json` now 404s). The new manifest is a JSON **list** whose entries carry
`batch`, `archive_name` (`{batch}.tar.bz2`), `url`, `sha1`, `size`, `lccns`, and
`issue_count`. Tarball internal layout differs from the batch directory:
`{lccn}/YYYY/MM/DD/ed-N/seq-N/ocr.xml`. The manifest's `lccns` and `issue_count`
fields are now used to build the batch index and the dry-run estimate without any
directory crawling (see §6).

---

## 3. Code map (everything CA-specific)

| Path | Role |
|---|---|
| `text_preparation/importers/chronicling_america/classes.py` | `ChroniclingAmericaNewspaperIssue` / `ChroniclingAmericaNewspaperPage` parser classes |
| `text_preparation/importers/chronicling_america/detect.py` | `detect_issues` / `select_issues` |
| `text_preparation/importers/chronicling_america/bulk.py` | Resumable bulk downloader (tarballs + METS crawl), all pure functions + `run_bulk_download` |
| `text_preparation/importers/chronicling_america/api.py` | loc.gov JSON API discovery + tile.loc.gov URL resolution (official LOC approach for sampling) |
| `text_preparation/importers/chronicling_america/fetch_data.py` | CLI entry point for bulk, API, and crawl download modes |
| `text_preparation/importers/chronicling_america/README.md` | Short user-facing how-to |
| `text_preparation/importer_scripts/chronicling_america_importer.py` | Launcher: `generic_importer.main(ChroniclingAmericaNewspaperIssue, detect, select)` |
| `text_preparation/importers/chronicling_america/chronicling_america_pilot_titles.json` | **Pilot title registry** — the six newspapers targeted for the first bulk download (LCCN, alias, optional date range) |
| `text_preparation/importers/chronicling_america/download_plans/` | **Per-title download plans** — one `.txt` per pilot title listing batches, tarball sizes, and issue estimates (generated by `generate_plans.py`) |
| `text_preparation/importers/chronicling_america/generate_plans.py` | CLI to regenerate `download_plans/` from the live OCR manifest |
| `text_preparation/config/download_config/chronicling_america_titles.json` | Legacy/single-title download config (currently only `eveningstar`; prefer `chronicling_america_pilot_titles.json` for the pilot) |
| `text_preparation/config/importer_config/import_LOC.json` | Import-time selection config (`titles`, `exclude_titles`, `year_only`) |
| `tests/importers/test_ca_downloader.py` | Unit tests for `bulk.py` (all mocked, no network) |
| `tests/importers/test_ca_api.py` | Unit tests for `api.py` (all mocked, no network) |
| `tests/importers/test_chronicling_america_importer.py` | End-to-end import test on the bundled sample issue |
| `text_preparation/data/sample_data/eveningstar/1932/06/20/ed-1/` | Sample issue (Evening Star, 1932-06-20) used by the test |

Untracked working artifacts at repo root (not part of the code): `output.txt` and
`pytest_out.log` are old logs from a June 10 run; `scratch/` holds ad-hoc debug
scripts; `uv.lock` is a stub.

---

## 4. Local data layout expected by the importer

```
[base_dir]/[alias]/[year]/[month]/[day]/[edition]/
    {YYYYMMDDEE}.xml      <- METS (10-digit name + .xml = 14 chars)
    alto/
        0567.xml ...      <- per-page ALTO, named after METS fileSec hrefs
```

Example (the bundled sample):

```
text_preparation/data/sample_data/eveningstar/1932/06/20/ed-1/
    1932062001.xml
    1932062001_1.xml      <- stray duplicate, ignored by METS resolution
    alto/0567.xml ... alto/0579.xml   (13 files; METS references more, see §8)
```

The bulk downloader (§6) produces exactly this layout.

---

## 3b. Pilot titles & download plans

The newspapers targeted for the first bulk ingestion are listed in
**`text_preparation/importers/chronicling_america/chronicling_america_pilot_titles.json`**
(six titles: Daily Dispatch, Memphis Daily Appeal, New-York Tribune, San Francisco Call,
Seattle Star, Evening Star).

For each pilot title, a pre-computed download plan lives in
**`text_preparation/importers/chronicling_america/download_plans/`** as
`{alias}_{lccn}.txt`. Each file lists:

- batch count and tarball compressed size,
- estimated METS/issue count (manifest upper bound),
- the full **batch list** (e.g. `dlc_ferguson_ver01`, …).

Regenerate plans after manifest changes:

```bash
python -m text_preparation.importers.chronicling_america.generate_plans
```

Use the pilot config for bulk download:

```bash
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/importers/chronicling_america/chronicling_america_pilot_titles.json \
  --output-dir "$CA_ROOT/raw" --state-dir "$CA_ROOT/state" --dry-run
```

---

## 5. Parser details (`classes.py`)

`ChroniclingAmericaNewspaperIssue(MetsAltoCanonicalIssue)`:

- **`_find_pages()`**: resolves the METS file as the XML whose filename is 14 chars and
  all-digits before `.xml` (falls back to the first `.xml` found — this is also why the
  stray `1932062001_1.xml` in the sample is harmless). Builds a `fileID → filename` map
  from the METS `fileSec` (`FLocat xlink:href`), then walks `structMap` divs with
  `TYPE` containing "page", following `fptr` elements whose `FILEID` starts with
  `ocrFile`. Falls back to a sorted scan of `alto/` if `structMap` yields nothing.
  Page numbers are the 1-based order of page divs in the `structMap`.
- **`_find_content_items()`**: there is **no OLR**, so per page it creates:
  1. one **`page`-type content item** (`tp: "page"`) grouping all `TextBlock`s in
     `PrintSpace` that are not tables; legacy `parts` carry `comp_id` (block ID),
     `comp_role: "body"`, `comp_fileid`, `comp_page_no`;
  2. one **`image`** content item per `<Illustration>`;
  3. one **`table`** content item per `<ComposedBlock TYPE="table">`.
  Page CIs are numbered `i0001…` matching page numbers; image/table CIs continue the
  counter after `len(pages)`. Image/table coords come from
  `mets_alto.alto.distill_coordinates`, divided by 3 (see below). Each CI's `l` block
  records `src_files` (`mets_xml`, `alto_xml`).
- **`_parse_mets()`**: assembles `issue_data` with `st: "newspaper"`,
  `sm: "print"`, `olr: False`, reading order from
  `text_preparation.utils.get_reading_order`, page ID list, and optional
  `media_title_variant` from the METS `<title>`.

`ChroniclingAmericaNewspaperPage(MetsAltoCanonicalPage)`:

- Carries `file_id` (the METS `ocrFileN` ID) so CI `parts` can reference it.
- **`add_issue()`** sets `iiif_img_base_uri` to
  `https://impresso-project.ch/api/proxy/iiif/{page_id}` and reads page `fw`/`fh` from
  the ALTO `<Page WIDTH= HEIGHT=>`, divided by 3.
- **Coordinates**: CA ALTO uses **`inch1200` units** (1/1200 inch). All coordinates are
  converted to **400 DPI pixels by dividing by 3** (`_convert_coordinates` walks
  regions → paragraphs → lines → tokens). This matches the 400 DPI scan resolution of
  the source images.

Actual text extraction into page regions happens in the shared
`MetsAltoCanonicalPage.parse()` → `mets_alto/alto.py parse_printspace`, using a
`comp_id → ci_id` mapping restricted to parts whose `comp_page_no` equals the page
number (avoids ALTO block-ID collisions across pages).

---

## 6. Downloader (`api.py` + `bulk.py` + `fetch_data.py`)

LOC historically exposed two complementary access paths. After the 2025–2026
migration (see §0) **only the `chroniclingamerica.loc.gov` paths work for
scripts**; the `www.loc.gov` JSON API is Cloudflare-gated (`403`).

### Official loc.gov JSON API (`api.py`) — currently BLOCKED

Documentation:
- [Chronicling America API README](https://libraryofcongress.github.io/data-exploration/loc.gov%20JSON%20API/Chronicling_America/README.html)
- [Working within limits (rate limits)](https://www.loc.gov/apis/json-and-yaml/working-within-limits/)
- [Migration notice](https://loc.gov/ndnp/migration/)

Intended workflow (mirrors LOC notebook 6): **search** issues via the collection
API (`www.loc.gov/collections/chronicling-america/?fa=number_lccn:…&dl=issue&fo=json`),
**resolve** per-issue METS/ALTO from each item record
(`www.loc.gov/item/{lccn}/{date}/{ed}/?fo=json`), then **download** from
`tile.loc.gov/storage-services/…`.

`api.py` still implements this and its unit tests pass (mocked), but **live use is
blocked**: `www.loc.gov` returns a Cloudflare "Just a moment…" `403` to all
automated clients (verified 2026-06-15, bot and browser UAs alike), and
`tile.loc.gov` likewise returns `403`. Documented LOC limits if it were reachable:
JSON API 20 req/min, text/image/storage 150 req/min, 1-hour block on breach, and
queries >100k results are rejected (use date/LCCN facets).

### When to use which path

| Mode | Best for | Discovery | File source | Works now? |
|---|---|---|---|---|
| Bulk (`bulk.py`, `--config`) | Large-scale ingestion of full titles | OCR-manifest batch index (no crawl) + per-batch reel crawl | Tarballs for ALTO + per-issue METS crawl | ✅ |
| Crawl (`--use-crawl`) | Sampling specific issues/dates | Directory listing of `data/batches/` | `chroniclingamerica.loc.gov` paths | ✅ |
| API (`api.py`, default legacy) | (was) sampling via loc.gov JSON | loc.gov JSON search/item | `tile.loc.gov` URLs | ❌ 403 (Cloudflare) |

### Bulk mode (recommended for scale; `--config` flag)

Pipeline in `run_bulk_download`:

1. **Plan** (`build_download_plan`):
   - fetch the OCR manifest from `/data/ocr/ocr.json` (fallback legacy `/ocr.json`),
     parsed by `parse_ocr_manifest` into `TarballInfo` (now carrying `lccns` and
     `issue_count`);
   - build or load the **batch index** (`{batch: [lccns]}`) cached at
     `state_dir/batch_index.json`. It is now derived **from the manifest's per-batch
     `lccns`** (`index_from_tarball_manifest`) — **no crawling**, instant for all
     ~2,600 batches.
   - select batches containing any configured LCCN, deduped to the highest `_verNN`
     per family (`dedupe_batch_versions`);
   - **dry-run** (`--dry-run`): estimate issue count by summing manifest
     `issue_count` over selected batches — no crawl, no upfront enumeration.
   - **real run**: **no upfront issue enumeration** (that crawl triggered CAPTCHA
     blocks). Issues are discovered per tarball instead (see step 2).
2. **Per-batch processing** (`process_tarball`, one batch at a time):
   - download the OCR `.tar.bz2`, verify SHA-1, extract ALTO for configured LCCNs;
   - derive the issue list from tarball paths (`lccn/YYYY/MM/DD/ed-N/seq-N/ocr.xml`);
   - resolve METS base URLs via a **lazy, cached reel crawl**
     (`state_dir/issue_urls/{batch}_{lccn}.json`), stopping as soon as all tarball
     issues for that batch are mapped;
   - download METS per issue, then write the local layout (`write_issue_layout`).
3. **Resume state**: `state_dir/download_state.json` holds `completed_tarballs`
   (by SHA-1) and `completed_issues` (`alias/date/edition` keys). Issue URL caches
   are separate JSON files under `state_dir/issue_urls/`.

Robustness: `HttpClient` enforces a polite per-request delay (**default 3.0 s**,
≈20 req/min per [LOC official limits](https://www.loc.gov/apis/json-and-yaml/working-within-limits/))
**and** a sliding-window cap (**default 10 req/min**) **inside the request lock**.
Official LOC guidance for bulk access: use the batch/OCR datasets at
`chroniclingamerica.loc.gov/data/batches/` and split large jobs by title or date
(see [NDNP examples](https://www.loc.gov/ndnp/guidelines/examples.html) and the
[researcher guide](https://guides.loc.gov/chronicling-america/additional-features)).
Contact `ndnptech@loc.gov` for high-volume research downloads.

| Service | Requests/min | Block if exceeded |
|---|---|---|
| JSON/YAML API (`loc.gov`) | 20 | 1 hour |
| Text / image / storage | 150 | 1 hour |

Our bulk pipeline talks to `chroniclingamerica.loc.gov` (not the JSON API), but we
stay under the **20/min** JSON API ceiling because observed throttling on directory
crawls is stricter. Increase `--delay` (e.g. `--delay 5.0`) if you still see `429`s.

On `429` or HTML CAPTCHA/challenge pages (`403` with Cloudflare "Just a moment…")
the client waits **≥3600 s** (LOC's documented 1-hour block) or honors
`Retry-After`. Transient `5xx` (including Cloudflare `52x`) retry with exponential
backoff. `404` is deliberately *not* retried since callers use it to probe batch/LCCN
existence. `download_file` streams to a `.part` temp file, renames on success, and
retries mid-stream `ChunkedEncodingError`/`ConnectionError`/`Timeout` drops.

> **Scale warning.** A single title with no date range is large: Evening Star
> (`sn83045462`) spans **125 batches ≈ 135 GB compressed ≈ 39 k issues**. Always
> set `start_date`/`end_date` for pilots.

> **Permissions.** `--output-dir`/`--state-dir`/scratch must be writable. On shared
> clusters `/data` is typically read-only (`PermissionError [Errno 13] '/data'`);
> use scratch space (e.g. `/rcp-scratch/students/<project>/ca/…`).

### Legacy single-title mode (small samples)

Without `--config`, `fetch_data.py` defaults to the loc.gov API (`api.py`) — which
is **currently 403-blocked** (see above). Use `--use-crawl` to download a single
issue via the working `chroniclingamerica.loc.gov` directory crawl. Options:
`--lccn`, `--alias`, `--date`, `--edition` (default `ed-1`), `--limit`,
`--use-crawl` (batch-directory crawling), `--batch` (crawl only).

### Commands

```bash
export CA_ROOT=/rcp-scratch/students/impresso-CA-pilot/ca   # any writable path

# Dry-run plan (instant: manifest-based estimate, no crawl, no www.loc.gov)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/config/download_config/chronicling_america_titles.json \
  --output-dir "$CA_ROOT/raw" --state-dir "$CA_ROOT/state" --dry-run

# Real bulk download (resumable; run in tmux on the server)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config ... --output-dir "$CA_ROOT/raw" --state-dir "$CA_ROOT/state" \
  --workers 6 --delay 3.0

# One sample issue locally (use --use-crawl; API mode is 403-blocked)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --output-dir text_preparation/data/sample_data \
  --lccn sn83045462 --alias eveningstar --date 1932-06-20 --use-crawl
```

---

## 7. Provider registration (`impresso_essentials`)

`impresso_essentials` ≥1.4.4 defines the `LOC` provider and the six pilot aliases
(`dailydispatch`, `eveningstar`, `memphisdailyappeal`, `newyorktribune`,
`sanfranciscocall`, `seattlestar`) in its module-level registries
(`PARTNER_TO_MEDIA`, `PARTNERS_TO_SRC_MEDIUM_TO_MEDIA`, `PARTNERS_TO_SRC_TYPE_TO_MEDIA`,
`PARTNER_TO_COUNTRY`, `MEDIA_TO_COUNTRY`, `ALL_MEDIA`). No runtime registration is
needed in `detect.py`.

`select_issues` reads the importer config (`titles` dict keyed by alias, with optional
date ranges; `exclude_titles`; `year_only`) and applies
`text_preparation.importers.detect._apply_datefilter`. Note the key names differ from
other importers' configs (`titles` vs `aliases`).

---

## 8. Running the import and tests

```bash
# Import to canonical (S3 credentials are required by imports even if unused — use dummies)
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
python -m text_preparation.importer_scripts.chronicling_america_importer \
  --input-dir /data/ca/raw \
  --output-dir /data/ca/canonical \
  --provider LOC \
  --clear --verbose
```

Output structure: `out_dir/LOC/{alias}/{year}/{month}/{day}/{edition_letter}/` with
per-page JSON (`alias-YYYY-MM-DD-e-pNNNN.json`) plus compressed yearly issue files; the
test's output lands in `text_preparation/data/canonical_out/test_out/`.

```bash
# Tests (venv is .venv, Python 3.14)
.venv/bin/python -m pytest -q tests/importers/test_ca_downloader.py            # bulk downloader, no network
.venv/bin/python -m pytest -q tests/importers/test_ca_api.py                   # loc.gov API helpers, no network
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
.venv/bin/python -m pytest -q tests/importers/test_chronicling_america_importer.py  # 1 e2e test
```

Status on 2026-06-15: **all 42 downloader/API tests pass** (~0.3 s). The importer
test patches `read_manifest_from_s3` and uses a `DataManifest` with
`push_to_git=False`.

### Known sample-data caveat

The bundled sample issue's METS references more pages than the 13 ALTO files present
(`alto/0567.xml`–`0579.xml`); pages 15+ (`0580.xml`+) are missing. During import this
logs `IOError` retries and "Failed to parse Alto file for page N" errors but **does not
fail the run** — missing pages simply get no page-level content item, and page JSON is
written for the pages that exist. Don't "fix" these log errors by deleting pages from
the METS; they reflect the partial sample download.

---

## 9. Git history of this effort (branch `dev/chronicling-america`)

| Commit | Date | Summary |
|---|---|---|
| `cdeefed` | 2026-06-10 | Initial CA importer (classes, detect, launcher, importer test, sample data) |
| `a2f0d04` | 2026-06-12 | Resumable bulk downloader (`bulk.py`, `fetch_data.py`, downloader tests, configs) |
| `5d6f7dd` | 2026-06-12 | Fix legacy-mode batch discovery (`find_first_batch_for_lccn` probes actual `/data/{lccn}/` dirs) |
| `4f04f52` | 2026-06-12 | Retry downloads on mid-stream connection drops (`.part` temp files, retry loop) |
| (wip)     | 2026-06-15 | Post-migration fixes (see §11): manifest URL/schema, manifest-based index, dry-run estimate, rate-limit lock fix, API 403 documented |

## 11. 2026-06-15 server debugging session (post-migration fixes)

Triggered by a "rate limit error" report when downloading on a cluster. Root causes
found via runtime evidence and fixed (all in `bulk.py`/`api.py`/`fetch_data.py`):

1. **`PermissionError: '/data'`** — the README's example paths weren't writable on
   the cluster. *Resolution:* use scratch paths; docs updated.
2. **`404` on `ocr.json`** — LOC moved the manifest. `/ocr.json` redirects to a
   `404`; the live manifest is `/data/ocr/ocr.json` with a **new list schema**
   (`batch`, `archive_name`, `lccns`, `issue_count`, …). *Fix:* `fetch_ocr_tarballs`
   tries the new URL first then the legacy one; `parse_ocr_manifest` handles both
   shapes (and `batch_<…>.tar.bz2` vs `<…>.tar.bz2` names).
3. **Indexing all 2,582 batches (hours)** — the old index build crawled every
   batch. *Fix:* `index_from_tarball_manifest` builds `{batch: [lccns]}` straight
   from the manifest; crawling only for batches missing from it.
4. **`429` during planning** — enumerating issues crawled `/data/{lccn}/{reel}/`
   for many batches and tripped the limit. Two parts:
   - the rate-limit `time.sleep(delay)` ran **outside** the request lock, so 6
     workers burst together. *Fix:* delay is now taken **inside** the lock via
     `_rate_limit_wait()` (verified: inter-request gap went 0.001 s → ~1.0 s).
   - `429` backoff was only seconds. *Fix:* `_backoff_for_status` waits ≥60 s or
     `Retry-After`. Default `--delay` raised 1.0 → **3.0 s**.
5. **`403` from the loc.gov JSON API** — a mid-session pivot to discover issues via
   `www.loc.gov` failed because that host is behind a **Cloudflare bot challenge**
   (`403` for all UAs; verified live). *Resolution:* reverted to
   `chroniclingamerica.loc.gov`-only discovery; **dry-run estimates from manifest
   `issue_count`** (no `www.loc.gov`, no crawl). The `api.py` path is kept but
   documented as currently blocked.

Local end-to-end verification (2026-06-15): dry-run for Evening Star returns
125 batches / 134.8 GB / ~39,016 issues in ~1.5 s with a single HTTP request
(the manifest); crawl-based discovery + one METS download succeed; 42 tests pass.

## 12. Open items / future work ideas

- **loc.gov JSON API is Cloudflare-blocked** (`api.py`). If single-issue API mode is
  needed, add challenge-capable fetching (e.g. a real browser/session) or rely on
  `--use-crawl`. Watch the [migration page](https://loc.gov/ndnp/migration/);
  on 2026-04-08 the legacy OCR page began redirecting to the new Datasets page.
- The dry-run estimate is a **batch-level upper bound** (manifest `issue_count` is
  not date-filtered and may bundle multiple LCCNs). A date-accurate count would
  require crawling (the thing we avoid) — acceptable trade-off for sizing.
- The `language` attribute read from ALTO `<Page>` defaults to `"en"`; CA also hosts
  non-English titles, so language handling may need revisiting per title.
- IIIF: the page `iiif_img_base_uri` points at the Impresso proxy; LOC native IIIF
  endpoints are not used.
- Date-range filtering exists in both the download config (per LCCN) and the import
  config (per alias) — keep them consistent when adding titles.
- `eveningstar` is currently the only configured title (`sn83045462`); add entries to
  `chronicling_america_titles.json` and `import_LOC.json` to scale up.

## 13. Generalizing to the full CA corpus (2026-06-21)

CA is a single format (NDNP METS/ALTO) that varies along a few axes; the converter
now drives those from the data instead of hardcoding them. New/changed pieces:

| Path | Role |
|---|---|
| `chronicling_america/canonicalize.py` | Convert downloads **in place** to canonical JSON. Discovers issues by walking any directory, builds `IssueDir`s from the METS, and feeds them to `core.import_issues` (which schema-validates). Layout-agnostic; CA-gated by LCCN. |
| `chronicling_america/audit.py` | **Read-only** corpus probe. Tabulates the variation axes (layout, MeasurementUnit, block flavors, structMap TYPEs, MODS language/title, LCCNs) so you fix what actually occurs. No conversion, no network. |
| `tests/importers/test_ca_canonicalize.py` | Unit tests for the helpers + discovery (synthetic fixtures, no network). |

**Measure first.** Before a large import, run the audit; if pointed at mixed-provider
data it will *also* show non-CA METS (the audit does not LCCN-gate), so scope it or
read the LCCN count. The converter, by contrast, **requires an LCCN** in the METS, so
it never picks up other providers' data even if run over a shared tree.

### Coordinate scaling — no longer a hardcoded ÷3
`classes.py` now derives the divisor from the ALTO `MeasurementUnit`
(`measurement_divisor`): `inch1200 → ÷3`, `mm10 → ÷0.635`, `pixel → ÷1` (already in
pixels; cannot rescale without the scan DPI). All values normalize to
`TARGET_DPI = 400` pixels — the **assumption** is that Impresso's IIIF proxy serves
images at 400-DPI-equivalent. If that resolution differs, change `TARGET_DPI` in one
place. `pixel`-unit ALTO is the one case that needs the source DPI to be correct.

### Image content items — all flavors
`_find_content_items` now creates `image` CIs from `<Illustration>` **and**
`<ComposedBlock TYPE="Illustration"|"Image">` **and** standalone `<GraphicalElement>`
(`collect_image_elements`), deduplicated and skipping GraphicalElements nested inside a
captured image block. Previously only `<Illustration>` elements were captured — the
SF Call pilot encodes pictures as `ComposedBlock TYPE="Illustration"`, so images were
silently dropped. (Tables: `ComposedBlock TYPE="table"`, unchanged.)

### Language & title
Page-CI `lg` resolves ALTO `<Page language>` → MODS `<languageTerm>` (normalized to
ISO-639 via `normalize_language`, handles spelled-out names) → `"en"` fallback.
`media_title_variant` reads a MODS `<title>` element, else strips the trailing date
from the METS `LABEL` (the SF Call METS has no `<title>` element — title lives in
`LABEL`).

### Robustness
`mets_alto/alto.py parse_printspace` now guards the block-level
`distill_coordinates` call: a block missing `HPOS/VPOS/WIDTH/HEIGHT` is skipped with a
note instead of crashing the whole page. **Shared change** — affects all METS/ALTO
importers, but only changes behaviour on input that previously crashed.

### Still open for corpus-scale
- **Alias registration**: `impresso_essentials` only registers the 6 pilot aliases.
  `--title-slug` can mint an alias from the title for unmapped LCCNs, but it is **not**
  a registered Impresso medium — downstream provider/media lookups and IIIF hosting
  expect registered aliases. Decide a naming + registration process before scaling.
- **IIIF hosting**: `iiif_img_base_uri` points at the Impresso proxy keyed by
  `page_id`; the proxy must actually host the title's images or links break.
- **Tarball-only (no METS) inputs**: the parser needs a METS; `canonicalize.py` warns
  on `ocr.xml`-only directories. Use the bulk downloader (lazy METS crawl) to fetch
  METS first.
- **`pixel`-unit coordinates**: handled as identity; correct rendering needs the source
  scan DPI if such batches appear.

### Commands
```bash
# Audit a download (read-only): what variants are present?
python -m text_preparation.importers.chronicling_america.audit \
  --input-dir /path/to/ca/raw --alto-sample 3 --json /tmp/ca_audit.json

# Convert in place to canonical JSON (schema-validated by the engine)
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
python -m text_preparation.importers.chronicling_america.canonicalize \
  --input-dir /path/to/ca/raw --output-dir /path/to/ca/canonical --clear
# add --lccn-alias snXXXXXXXX=myalias  (repeatable) or --title-slug for unmapped LCCNs
```
The benign `InvalidAccessKeyId` at the end is only the *manifest* trying to upload
itself with dummy creds; the canonical JSON is fully written before that.
