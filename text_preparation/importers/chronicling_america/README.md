# Chronicling America (Library of Congress) Newspaper Importer

This package contains the pipeline modules and scripts to download, detect, and import newspaper issues from the Library of Congress's **Chronicling America** archive into the Impresso canonical format.

---

## Submodule Structure

- `classes.py`: Parser models for Chronicling America issues and pages (subclasses of `MetsAltoCanonicalIssue` / `MetsAltoCanonicalPage`).
- `detect.py`: Scans the local filesystem to discover issues and registers the `LOC` provider dynamically.
- `bulk.py`: Resumable bulk downloader (OCR tarballs + METS crawl).
- `fetch_data.py`: CLI for bulk and legacy single-title downloads.
- `chronicling_america_importer.py`: Main launcher under `text_preparation/importer_scripts/`.

Title registry config: `text_preparation/config/download_config/chronicling_america_titles.json`

---

## How to Run

### 1. Bulk download on a server (recommended)

Run the downloader **directly on the server** (not locally over SSH). Use `tmux` so the job survives disconnects.

```bash
# Clone and install on the server
git clone <repo-url> impresso-text-acquisition
cd impresso-text-acquisition
git checkout dev/chronicling-america
pip install -e .

# Inspect disk needs first
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/config/download_config/chronicling_america_titles.json \
  --output-dir /data/ca/raw \
  --state-dir /data/ca/state \
  --dry-run

# Run the download (resumable)
tmux new -s ca-download
python -m text_preparation.importers.chronicling_america.fetch_data \
  --config text_preparation/config/download_config/chronicling_america_titles.json \
  --output-dir /data/ca/raw \
  --state-dir /data/ca/state \
  --workers 6 \
  --delay 1.0
```

Add titles by editing the config JSON:

```json
{
  "titles": [
    {
      "lccn": "sn83045462",
      "alias": "eveningstar",
      "start_date": "1900-01-01",
      "end_date": "1960-12-31"
    }
  ]
}
```

Re-running the same command resumes from `state-dir/download_state.json`.

### 2. Download a single sample issue (local dev)

```bash
python -m text_preparation.importers.chronicling_america.fetch_data \
  --output-dir text_preparation/data/sample_data \
  --lccn sn83045462 \
  --alias eveningstar \
  --limit 1
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
pytest -vv tests/importers/test_ca_downloader.py
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
pytest -vv tests/importers/test_chronicling_america_importer.py
```

---

## Bulk Downloader Design

1. **Batch index**: cached JSON mapping batches to LCCNs (`state-dir/batch_index.json`).
2. **Batch selection**: all batches containing configured LCCNs; highest `_verNN` kept when duplicates exist.
3. **ALTO via tarballs**: downloads `.tar.bz2` OCR batches from `chroniclingamerica.loc.gov/ocr.json`, verifies SHA-1, stream-extracts ALTO XML for selected LCCNs.
4. **METS crawl**: downloads one METS file per issue from `/data/batches/{batch}/data/{lccn}/...`.
5. **Layout**: writes `alias/YYYY/MM/DD/ed-N/{issue}.xml` and `alto/{href}.xml`, renaming tarball `seq-N/ocr.xml` paths to METS `fileSec` href names.

Output layout matches what the importer expects:

```
/data/ca/raw/eveningstar/1932/06/20/ed-1/
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
