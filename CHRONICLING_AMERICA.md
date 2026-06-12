# Chronicling America Integration — Project Knowledge Base

> Reference document for AI agents and developers working on the Chronicling America
> (Library of Congress) importer inside `impresso-text-acquisition`.
> Last verified: 2026-06-12, branch `dev/chronicling-america`, all 25 CA tests passing.

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

Bulk OCR is also distributed as per-batch tarballs listed at
`https://chroniclingamerica.loc.gov/ocr.json` (name `batch_{batch}.tar.bz2`, with `url`,
`sha1`, `size`). Tarball internal layout differs from the batch directory:
`{lccn}/YYYY/MM/DD/ed-N/seq-N/ocr.xml`.

---

## 3. Code map (everything CA-specific)

| Path | Role |
|---|---|
| `text_preparation/importers/chronicling_america/classes.py` | `ChroniclingAmericaNewspaperIssue` / `ChroniclingAmericaNewspaperPage` parser classes |
| `text_preparation/importers/chronicling_america/detect.py` | `detect_issues` / `select_issues` + dynamic `LOC` provider registration |
| `text_preparation/importers/chronicling_america/bulk.py` | Resumable bulk downloader (tarballs + METS crawl), all pure functions + `run_bulk_download` |
| `text_preparation/importers/chronicling_america/fetch_data.py` | CLI entry point for bulk and legacy single-title download modes |
| `text_preparation/importers/chronicling_america/README.md` | Short user-facing how-to |
| `text_preparation/importer_scripts/chronicling_america_importer.py` | Launcher: `generic_importer.main(ChroniclingAmericaNewspaperIssue, detect, select)` |
| `text_preparation/config/download_config/chronicling_america_titles.json` | Title registry for bulk download (`lccn`, `alias`, optional `start_date`/`end_date`) |
| `text_preparation/config/importer_config/import_LOC.json` | Import-time selection config (`titles`, `exclude_titles`, `year_only`) |
| `tests/importers/test_ca_downloader.py` | 24 unit tests for `bulk.py` (all mocked, no network) |
| `tests/importers/test_chronicling_america_importer.py` | End-to-end import test on the bundled sample issue |
| `text_preparation/data/sample_data/eveningstar/1932/06/20/ed-1/` | Sample issue (Evening Star, 1932-06-20) used by the test |

Untracked working artifacts at repo root (not part of the code): `output.txt` and
`pytest_out.log` are old logs from a June 10 run that failed with `KeyError: 'LOC'`
(since fixed, see §7); `scratch/` holds ad-hoc debug scripts; `uv.lock` is a stub.

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

## 6. Downloader (`bulk.py` + `fetch_data.py`)

### Bulk mode (recommended; `--config` flag)

Pipeline in `run_bulk_download`:

1. **Plan** (`build_download_plan`):
   - fetch `ocr.json` → list of OCR tarballs per batch;
   - build or load the **batch index** (`{batch: [lccns]}`) cached at
     `state_dir/batch_index.json`. Building it from scratch crawls every batch's
     `/data/` listing and **takes hours** — never delete the cache casually;
   - select batches containing any configured LCCN, deduped to the highest `_verNN`
     per family (`dedupe_batch_versions`);
   - crawl each selected batch's `/data/{lccn}/{reel}/` listings to enumerate issues
     (`list_issues_in_batch`), filter by title date range, dedupe per
     `lccn/date/edition` keeping the highest batch version (`dedupe_issues`).
2. **METS crawl**: one METS file per pending issue, downloaded in a
   `ThreadPoolExecutor` (`--workers`, default 6). Skips files already on disk.
3. **Tarball processing** (sequential): download `.tar.bz2` to `--scratch-dir`,
   **verify SHA-1**, stream-extract ALTO members for selected LCCNs only
   (`extract_alto_members`, expects exactly 7 path parts
   `lccn/YYYY/MM/DD/ed-N/seq-N/*.xml`), then `write_issue_layout` renames `seq-N`
   files to the METS `fileSec` href names (falling back to `seq-N.xml` if the METS
   lists fewer pages). Tarball deleted afterwards unless `--keep-tarballs`.
4. **Resume state**: `state_dir/download_state.json` holds `completed_tarballs`
   (by SHA-1) and `completed_issues` (`alias/date/edition` keys), saved after each
   unit of work. Re-running the same command resumes.

Robustness: `HttpClient` enforces a polite per-request delay (default 1.0 s), retries
with exponential backoff on request exceptions and on transient statuses
(`is_transient_status`: 429 and all 5xx — including Cloudflare 52x codes such as
**525 SSL handshake failed**, which `tile.loc.gov` returns intermittently after
`chroniclingamerica.loc.gov` redirects there). 404 is deliberately *not* retried since
callers use it to probe batch/LCCN existence. The client is serialized with a lock
(the `requests.Session` is shared across threads). `download_file` streams to a
`.part` temp file, renames on success, and retries mid-stream
`ChunkedEncodingError`/`ConnectionError`/`Timeout` drops (added in commit `4f04f52`).

### Legacy single-title mode (small samples)

Without `--config`, `fetch_data.py` downloads METS + per-page ALTO files directly from
the batch directory (no tarball, no renaming needed). It finds the first batch
containing the LCCN via `find_first_batch_for_lccn` (probes `/data/{lccn}/` per batch,
trying `dlc_*` batches first) instead of building the full index. Commit `5d6f7dd`
fixed this discovery. Options: `--lccn`, `--alias`, `--date YYYY-MM-DD`, `--limit`,
`--batch` (skip search).

### Commands

```bash
# Dry-run plan (prints batches/tarball sizes/issue counts)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/config/download_config/chronicling_america_titles.json \
  --output-dir /data/ca/raw --state-dir /data/ca/state --dry-run

# Real bulk download (resumable; run in tmux on the server)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config ... --output-dir /data/ca/raw --state-dir /data/ca/state \
  --workers 6 --delay 1.0

# One sample issue locally
python -m text_preparation.importers.chronicling_america.fetch_data \
  --output-dir text_preparation/data/sample_data \
  --lccn sn83045462 --alias eveningstar --limit 1
```

---

## 7. Provider registration: the `LOC` hack (important)

`impresso_essentials` (v1.4.2 installed in `.venv`) has **no built-in knowledge of the
`LOC` provider or CA aliases**. Its module-level registries
(`PARTNER_TO_MEDIA`, `PARTNERS_TO_SRC_MEDIUM_TO_MEDIA`, `PARTNERS_TO_SRC_TYPE_TO_MEDIA`,
`PARTNER_TO_COUNTRY`, `MEDIA_TO_COUNTRY`, `ALL_MEDIA`) are mutated **at detect time** by
`detect.py::detect_issues`:

- registers `LOC` with `{SourceMedium.PT: "all"}` / `{SourceType.NP: "all"}` and
  country `US`;
- registers **every subdirectory of `base_dir`** as a `LOC` alias with country `US`.

Consequences to keep in mind:

- `detect_issues` **must run in the same process** before anything calls
  `get_src_info_for_alias` / `get_provider_for_alias` (e.g. `core.compress_issues`).
  The old `pytest_out.log` / `output.txt` at repo root document a `KeyError: 'LOC'`
  from before the `"all"` registrations were complete; current code passes. As a
  second safety net, `core.compress_issues` now falls back to `print`/`audio` when
  `get_src_info_for_alias` raises `ValueError`.
- When pointed at the shared `sample_data/` directory, the registration sweeps in
  **other importers' sample folders** (RERO2, Luxembourg, BL, …) as fake LOC aliases.
  Harmless for tests (the test filters to `alias == "eveningstar"`), but don't rely on
  `PARTNER_TO_MEDIA["LOC"]` contents being meaningful.
- A proper fix would be upstreaming `LOC` + aliases into `impresso_essentials`.

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
.venv/bin/python -m pytest -q tests/importers/test_ca_downloader.py            # 24 tests, no network
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
.venv/bin/python -m pytest -q tests/importers/test_chronicling_america_importer.py  # 1 e2e test
```

Status on 2026-06-12: **all 25 pass** (~11 s). The importer test patches
`read_manifest_from_s3` and uses a `DataManifest` with `push_to_git=False`.

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

## 10. Open items / future work ideas

- Upstream `LOC` provider + alias registration into `impresso_essentials` instead of
  runtime mutation in `detect.py`.
- The `language` attribute read from ALTO `<Page>` defaults to `"en"`; CA also hosts
  non-English titles, so language handling may need revisiting per title.
- IIIF: the page `iiif_img_base_uri` points at the Impresso proxy; LOC native IIIF
  endpoints are not used.
- Date-range filtering exists in both the download config (per LCCN) and the import
  config (per alias) — keep them consistent when adding titles.
- `eveningstar` is currently the only configured title (`sn83045462`); add entries to
  `chronicling_america_titles.json` and `import_LOC.json` to scale up.
