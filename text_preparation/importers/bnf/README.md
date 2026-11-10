# BNF importers

BNF data comes in three structurally different formats. Each format has its own canonical
class and its own issue index. The shared, index-driven `detect.py` selects the index.

| Format key (`detect.set_json_file`) | Importer class | Issue index (`data/issue_indices/`) | Entry script (`importer_scripts/`) | Content |
| --- | --- | --- | --- | --- |
| `BNF-OCR` | `no_olr.classes.BnfNewspaperIssue` | `issue_index.bnf_ocr.json` | `bnfimporter.py` | Titles without OLR: ALTO pages + IIIF `manifest.json`. One content item per page. |
| `MP-OLR` | `mp_olr.classes.BnfMpNewspaperIssue` | `issue_index.bnf_mp.json` | `bnfmpimporter.py` | "Marché Presse" titles with OLR (METS + ALTO): legacy and new data. |
| `EN-OLR` | `en_olr.classes.BnfEnNewspaperIssue` | `issue_index.bnf_en.json` | `bnfenimporter.py` | "Europeana" titles with OLR (METS + ALTO): legacy and new data. |

`issue_index.bnf_1.json` is an intermediate file: it holds the legacy Marché Presse entries, which
were merged into `issue_index.bnf_mp.json`. No importer reads it.

## Issue index entries

The indices are nested as `{alias: {year: {month: [entry, ...]}}}`. The keys in an entry that
drive ingestion are:

- `batch`: where the data comes from.
  - `BNF_MP_old` and `BNF_EN_old` are the legacy Impresso-1 data (11 titles), with their
    original on-disk layouts.
  - `BNF_API_NEW` is data downloaded through the BnF IIIF API.
  - The OLR classes branch on `batch` to find the files of each layout.
- `ark_id`: the issue-level ARK. `title_ark` is the title-level ARK.
- `olr_file`: set to `true` by `preprocessing/copy_olr_to_issues.py` when an `<ark_id>_olr.xml`
  METS file was copied into the issue directory. With `MP-OLR` and `EN-OLR`, `detect.py` only
  keeps entries that have this key. `BNF-OCR` keeps all entries. Duplicated *oeuvre* issues get
  extra filtering (`detect.filter_oeuvre_entries`).
- `local_path`: the issue directory, or directories, relative to the original data mount.

## Building the indices (`preprocessing/`)

Run these in order. Each script documents its CLI in its module docstring.

1. `fetch_data_via_API.py`: downloads the ALTO pages and `manifest.json` of the new titles from
   the BnF API. The API only accepts the DHLab server's IP, and credentials come from the
   environment variables `BNF_API_KEY` and `BNF_API_SECRET`.
2. `analyze_failed.py`: sorts the downloader's `failed.jsonl` errors into categories.
3. `generate_bnf_issue_index.py`: walks the downloaded data and writes
   `issue_index.bnf_new.json`. That file is local only and too large for git.
4. `bash_scripts/fetch_BNF_olr.sh`, then `copy_olr_to_issues.py`: fetches the OLR deposit and
   attaches each OLR file to its issue (`olr_file`). It also writes the OLR coverage reports
   (`sample_data/BNF_API/olr_coverage_by_alias.json`).
5. `Impresso1_detect_to_issue_index.ipynb`: builds the index entries for the 11 legacy titles.
   It then uses the OLR coverage to assign each alias a format, splits `bnf_new` plus the legacy
   entries into `bnf_{ocr,mp,en}.json`, and sorts each index.
6. `fetch_en_manifest_dims.py`: fetches a `manifest.json` for each `BNF_EN_old` issue. The legacy
   data has none, and `en_olr` needs the real IIIF page dimensions to rescale coordinates.
7. `classify_alto_formats.py` (optional diagnostic): samples ALTO pages per issue and labels
   their structure (flat or nested `ComposedBlock`, complete or incomplete coordinates).

## Decisions taken during the migration

- Parsers stay separate per format. Only orchestration is shared, through the issue index and
  `detect.py`.
- Indices are split **by format**, not by batch. The legacy titles live in the same per-format
  indices and are told apart by `batch`.
- Titles with little or no OLR are imported as `BNF-OCR`: aliases where every ARK failed,
  `liberation`, and `oenantes`.
- Some `BNF_API_NEW` entries in the MP and EN indices have the no-OLR layout on disk. The
  `olr_file` filter keeps those entries out of the OLR importers.

## Known follow-ups

- `en_olr`: handle the new oenantes OLR data organisation once it arrives. Also consider
  `media_title_variant`.
- `detect.JSON_FILE` is resolved relative to the working directory.
