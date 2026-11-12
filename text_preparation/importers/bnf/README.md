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
- Some `BNF_API_NEW` entries in the MP and EN indices have the no-OLR layout on disk. The
  `olr_file` filter keeps those entries out of the OLR importers.

## Ingestion status and decisions (as of 2026-11-11)

"OLR coverage" is the share of a title's issues for which we have been able to fetch a METS OLR file. These are the issues that `detect.py` keeps in `MP-OLR`/`EN-OLR`
mode, that is, entries with `olr_file`, plus legacy `BNF_MP_old`/`BNF_EN_old` entries that have
an `ark_id`.

### Decided

- **`bnf_ocr` (47 titles):** these titles have no OLR and were never expected to have any. They
  are ingested in full as `BNF-OCR`.
- **`bnf_mp` / `bnf_en` with ≥ 80% OLR coverage (55 MP titles, 7 EN titles):** ingested in the
  `MP-OLR`/`EN-OLR` formats. Issues without OLR are **excluded**, which is what the `olr_file`
  filter does. The lowest-covered titles in this group are grandechonord (86.7%), parisbalkans
  (87.5%), terreeurope (87.5%), lacharente (89.2%) and notretemps (89.5%).
- **`bnf_mp` / `bnf_en` with < 80% OLR coverage, or none at all:** these titles *should* have OLR,
  so **their ingestion is skipped for now**. That means all their issues, including the ones
  that do have OLR.

  > ⚠ `detect.py` does not enforce this skip yet. Pass these aliases in the run config's
  > `exclude_aliases`, or MP-OLR will import their issues that have OLR.

  | Index | Title | Issues with OLR / total | Coverage |
  | --- | --- | --- | --- |
  | mp | abendland | 0 / 19 | 0.0% |
  | mp | bouffon | 0 / 174 | 0.0% |
  | mp | defensenationalparis | 0 / 35 | 0.0% |
  | mp | demokratischeztg | 0 / 5 | 0.0% |
  | mp | etatsuniseurope | 0 / 6 | 0.0% |
  | mp | europenouvelle | 0 / 30 | 0.0% |
  | mp | francelibre | 0 / 1,190 | 0.0% |
  | mp | lacroix ¹ | 0 / 26,856 | 0.0% |
  | mp | lejournal | 0 / 18,752 | 0.0% |
  | mp | liberation | 0 / 3,518 | 0.0% |
  | mp | unionfrancaise | 0 / 36 | 0.0% |
  | mp | vendredi | 0 / 163 | 0.0% |
  | mp | vsve | 0 / 60 | 0.0% |
  | mp | lafranceparis | 1,396 / 22,913 | 6.1% |
  | mp | laliberte | 9,341 / 52,803 | 17.7% |
  | mp | cinejournal | 207 / 868 | 23.8% |
  | mp | nyherald | 4,953 / 17,262 | 28.7% |
  | mp | francesoir | 1,166 / 3,514 | 33.2% |
  | mp | aurore1943 | 1,940 / 5,561 | 34.9% |
  | mp | lettresfrancaises | 221 / 618 | 35.8% |
  | mp | combat | 1,334 / 3,522 | 37.9% |
  | mp | franctireur | 1,335 / 2,897 | 46.1% |
  | mp | petitmarocain | 4,115 / 7,503 | 54.8% |
  | mp | lepeuple | 4,129 / 7,280 | 56.7% |
  | mp | cesoir | 2,197 / 3,539 | 62.1% |
  | mp | europefinanciere2 | 15 / 19 | 78.9% |
  | en | oenantes | 0 / 9,875 | 0.0% |

  In total, 27 titles and 189,018 issues are on hold.

  ¹ The OLR download analysis (`Impresso1_detect_to_issue_index.ipynb`) found that about 19% of
  lacroix ARKs *succeeded*, yet no lacroix entry has `olr_file`. Check whether
  `copy_olr_to_issues.py` was run for this title before going back to the BNF.

### Still open (pending exchanges with the BNF)

For each title on hold, find out whether OLR exists for it at the BNF, or whether the existing
OLR can be completed. Then:

- if the title reaches **≥ 80%** OLR coverage, re-run `copy_olr_to_issues.py` for it and ingest
  it in the `MP-OLR`/`EN-OLR` format, excluding issues without OLR;
- if it stays **under 80%**, move it to `bnf_ocr` (re-run the per-format split in
  `Impresso1_detect_to_issue_index.ipynb`) and ingest it as `BNF-OCR`.

## Known follow-ups

- `en_olr`: handle the new oenantes OLR data organisation once it arrives. Also consider
  `media_title_variant`.
- `detect.JSON_FILE` is resolved relative to the working directory.
