# CLAUDE.md

Notes for contributors and for Claude Code sessions working in this repository. See `README.md`
and the [docs](https://impresso-text-importer.readthedocs.io/en/latest/) for full documentation.

## Project overview

`impresso-text-preparation` converts OCR and ASR data from many providers into Impresso's
**canonical** format (issues + pages/audio records), and then into the **rebuilt** format
(one document per content item, for Solr and Passim).

- `text_preparation/importers/`: one sub-package per provider or format (`bnf`, `bl`, `lux`,
  `rero`, `sub`, `ina`, `rts`, …). Most of them follow these conventions:
  - a `detect.py` with `detect_issues`/`select_issues` that reads an **issue index** from
    `text_preparation/data/issue_indices/`;
  - a `classes.py` with the canonical issue and page classes;
  - shared METS/ALTO logic in `mets_alto/`, and the shared import loop in `core.py` and
    `generic_importer.py`.
- `text_preparation/importer_scripts/`: CLI entry points, one per importer.
- `text_preparation/rebuilders/`: canonical → rebuilt (`rebuilder.py`, paper/audio helpers).
- `text_preparation/impresso-schemas/`: **git submodule** with the JSON schemas used for
  validation. It is currently pinned to the schemas repo's `dev` branch.
- `text_preparation/config/`: importer and rebuilt run configs. Importer configs use the keys
  `aliases`, `exclude_aliases` and `year_only` (the old `titles`/`exclude_titles` keys are no
  longer read).
- BNF-specific documentation (formats, preprocessing, ingestion status):
  `text_preparation/importers/bnf/README.md`.

## Open tasks

### Full test-suite update (future issue/PR)

The tests in `tests/` are largely out of date. In particular, `tests/importers/test_bnf_importer.py`
and `test_bnf_en_importer.py` only have their imports updated and are **skipped**
(`pytestmark`). Rewrite the suite for the current code:

- [ ] Decide where the tests and their test data and logs live. `tests/conftest.py`
      currently writes logs to `text_preparation/data/logs/test_logs/`, which must exist
      locally. A new tests directory is planned.
- [ ] Use the current config keys (`aliases`/`exclude_aliases`), validate against the canonical
      and rebuilt schemas from the submodule, and drop the obsolete arguments (for example
      `access_rights` in BNF detection).
- [ ] At least one test per format:
  - [ ] BNF no OLR (`bnf/no_olr`, `detect.set_json_file("BNF-OCR")`): sample in
        `data/sample_data/BNF_API/samples/petitmarocain`
  - [ ] BNF Marché Presse OLR (`bnf/mp_olr`, `"MP-OLR"`): sample in `data/sample_data/BNF`
  - [ ] BNF Europeana OLR (`bnf/en_olr`, `"EN-OLR"`): BNF-EN sample
  - [ ] the other importers: bl, bcul, lux, rero, swa, olive, tetml, fedgaz, sub, ina, rts,
        swissinfo, onb_annop, kbr
- [ ] Regression check for the `<ComposedBlock>` recursion in `mets_alto/alto.parse_printspace`,
      which affects every METS/ALTO importer (including region → CI mapping and
      `section_pOf` inside ComposedBlocks).
- [ ] Rebuilder tests: Solr output uses `ci_id`, and invalid content items are dropped by the
      rebuilt-schema validation (`rebuilder._validate_and_log`).

### Planned next steps

- [ ] **Makefile.** Add a top-level `Makefile` to make common workflows easier:
  - environment preparation and install (including the schemas submodule)
  - generating and checking run configs
  - launching the processing steps: importers per provider/format, rebuilders, BNF preprocessing
  - building and uploading the documentation
  - running the tests
- [ ] **Full update of the Olive importer** (`importers/olive`), so that the Le Temps data and the
      RERO 1 (SNL) data can be re-ingested in the current canonical format. Bring it in line with
      the other importers: issue-index-based detection, `aliases` config keys, current
      canonical schema fields, and IIIF information.
- [ ] **Full diagnostic of the processing pipeline.** Review the import → canonical → rebuilt
      process end to end, and list what could be changed or updated to use more recent or more
      efficient packages, libraries and processes. Examples to evaluate:
  - XML parsing (BeautifulSoup vs lxml)
  - Dask usage and partitioning
  - S3 I/O (smart_open/boto3 versions, the botocore checksum workaround)
  - JSON (de)serialisation and compression
  - packaging (`pyproject.toml` vs `setup.py`/`requirements.txt`) and the Docker image

### Other follow-ups

- [ ] BNF titles on hold, pending exchanges with the BNF: the 27 titles under 80% OLR coverage
      listed in `importers/bnf/README.md` must be excluded via `exclude_aliases` until then.
- [ ] `bnf/en_olr`: support the new oenantes OLR data organisation once it arrives. Consider
      `media_title_variant`.
- [ ] `bnf/detect.py`: `JSON_FILE` is resolved relative to the working directory, and
      `set_json_file` overwrites the path template, so it cannot be called twice.
- [ ] Large issue indices (`issue_index.bnf_mp.json` is about 84 MB, close to GitHub's 100 MB
      limit): consider bz2 compression or Git LFS.
- [ ] Old `newspapers`/`exclude_newspapers` configs (olive, ONB, tetml, RERO debug) predate even
      the `titles` keys. Update or delete them.
- [ ] Once impresso-essentials relaxes its own `smart_open==7.0.4` pin, check the importers and
      rebuilders against smart_open ≥ 8.
