# Chronicling America (Library of Congress) Newspaper Importer

This package contains the pipeline modules and scripts to download, detect, and import newspaper issues from the Library of Congress's **Chronicling America** archive into the Impresso canonical format.

---

## 📂 Submodule Structure

- [classes.py](file:///Users/corentinsteinhauser/EPFL/SHS-DH/impresso/impresso-text-acquisition/text_preparation/importers/chronicling_america/classes.py): Object-oriented parser models representing Chronicling America issues and pages. They inherit from the generic `MetsAltoCanonicalIssue` and `MetsAltoCanonicalPage` classes.
- [detect.py](file:///Users/corentinsteinhauser/EPFL/SHS-DH/impresso/impresso-text-acquisition/text_preparation/importers/chronicling_america/detect.py): Scans the local filesystem to discover issues and dynamically registers the `LOC` provider and its media aliases to the global configurations of `impresso_essentials`.
- [fetch_data.py](file:///Users/corentinsteinhauser/EPFL/SHS-DH/impresso/impresso-text-acquisition/text_preparation/importers/chronicling_america/fetch_data.py): Downloader utility script that crawls the Library of Congress batch indexes, finds issues for a given LCCN (e.g., `sn83045462` for *The Evening Star*), and downloads METS and ALTO XML files.
- [chronicling_america_importer.py](file:///Users/corentinsteinhauser/EPFL/SHS-DH/impresso/impresso-text-acquisition/text_preparation/importer_scripts/chronicling_america_importer.py): Main launcher wrapper located under `text_preparation/importer_scripts/` to execute the pipeline using the generic importer engine.

---

## 🛠️ Summary of Changes & Fixes

1. **Downloader Engine**:
   - Crawls the LOC batches index (`https://chroniclingamerica.loc.gov/data/batches/`), detects matching LCCN data directories on reels, and downloads the raw METS/ALTO files.
   - Saves them in the standard hierarchy structure: `[output_dir]/[newspaper-name]/[year]/[month]/[day]/[edition]/` (with ALTO files residing inside `alto/` sub-directories).

2. **Parser and Coordinate Conversion**:
   - Chronicling America specifies ALTO coordinates in `inch1200` units (1/1200th of an inch). We convert these to standard **400 DPI pixels** by dividing all coordinates (`h`, `v`, `w`, `hp`) by `3`.
   - Structural mapping in METS extracts page orders via `structMap` and maps the OCR file references to ALTO filenames via `fileSec`.

3. **No-OLR Content Item Grouping**:
   - Since Chronicling America does not provide physical/logical article structure (no OLR), we group all page `TextBlock` tags into a single page-level `page` content item.
   - Separate content items are created for illustrations (`image`) and tables (`table`).

4. **Dynamic Metadata Registration**:
   - Registered `LOC` as a provider and the detected aliases (e.g., `eveningstar`) inside `impresso_essentials.utils` variables (`PARTNER_TO_MEDIA`, `PARTNERS_TO_SRC_MEDIUM_TO_MEDIA`, `PARTNERS_TO_SRC_TYPE_TO_MEDIA`, `PARTNER_TO_COUNTRY`, `MEDIA_TO_COUNTRY`, `ALL_MEDIA`). This avoids downstream `KeyError` exceptions when validating and compressing issues.

5. **Page validation & skip fix**:
   - Fixed a bug in `text_preparation/importers/core.py` where a validation warning would cause an `UnboundLocalError` inside the dask collection loop. It now correctly prints/logs the warning and uses `continue` to proceed with the remaining pages.

---

## 🚀 How to Run

### 1. Download Newspaper Issues
Use `fetch_data.py` to download raw METS/ALTO data from Chronicling America.

```bash
# Download a sample issue of the Evening Star (LCCN: sn83045462)
python -m text_preparation.importers.chronicling_america.fetch_data \
  --output-dir text_preparation/data/sample_data \
  --lccn sn83045462 \
  --alias eveningstar \
  --limit 1
```

### 2. Run the Importer CLI
To import the raw data into Impresso canonical format (JSON files and zipped JSONL bundles):

```bash
# Import the downloaded Evening Star data locally
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
python -m text_preparation.importer_scripts.chronicling_america_importer \
  --input-dir text_preparation/data/sample_data \
  --output-dir text_preparation/data/canonical_out \
  --provider LOC \
  --clear \
  --verbose
```

### 3. Run the Unit Tests
All implemented logic is validated by a dedicated pytest test suite:

```bash
SE_ACCESS_KEY=dummy SE_SECRET_KEY=dummy \
.venv/bin/pytest -vv tests/importers/test_chronicling_america_importer.py
```

---

## 💡 How it Works Under the Hood

### Downloader Pipeline (`fetch_data.py`)
1. Checks the URL `https://chroniclingamerica.loc.gov/data/batches/` to list all batches.
2. Scans each batch (preferring `dlc_*` batches) for the existence of the newspaper LCCN directory (e.g., `data/sn83045462/`).
3. Inside the LCCN directory, lists reels and scans for YYYYMMDDxx directories.
4. Downloads the METS file (`YYYYMMDDxx.xml`) into the target folder and ALTO files into the `alto/` subfolder.

### Parser Pipeline (`classes.py`)
1. **METS Parsing**: `ChroniclingAmericaNewspaperIssue` reads the METS file. It maps `ocrFileX` identifiers to ALTO filenames by scanning the `<fileSec>` element, and sequences pages by scanning the `<structMap>`.
2. **Page Parsing & Scaling**: `ChroniclingAmericaNewspaperPage` loads the corresponding ALTO XML file, extracts the width and height attributes from the `<Page>` element, and scales them down by a factor of 3 to convert `inch1200` to 400 DPI pixel coordinates.
3. **No-OLR Content Extraction**:
   - Chronicling America files lack article boundaries. The importer maps all body `TextBlock` elements to a page-level content item of type `"page"`.
   - It identifies `<Illustration>` and `<ComposedBlock TYPE="table">` tags and maps them to content items of type `"image"` and `"table"`, applying the coordinate scaling.
4. **Validation and Packaging**: The canonical issue structures are validated against the Impresso canonical JSON schema and serialized into a bzip2 compressed JSONL archive (e.g., `eveningstar-1932-issues.jsonl.bz2`) ready for downstream rebuilding or ingestion.
