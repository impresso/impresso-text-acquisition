# Impresso Text Preparation

[![Documentation Status](https://readthedocs.org/projects/impresso-text-importer/badge/?version=latest)](https://impresso-text-importer.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/impresso-text-preparation.svg)](https://badge.fury.io/py/impresso-text-preparation)
![PyPI - License](https://img.shields.io/pypi/l/impresso-text-preparation)

Impresso Text Preparation provides **OCR format conversion pipelines for newspaper and radio historical data**, from a variety of source formats into Impresso's unified representations: **Canonical and Rebuilt** formats.
- The canonical format is focuses on maintaining the logical structure and hierarchy of the source historical media content; defining *Issues* and *Physical Support* objects (pages, audio recordings).
- The rebuilt format defines a uniform document representation of *Content Items* (articles, ads, images...) designed for large-scale downstream applications such as Machine Learning fine-tuning and inference and Information Retrieval.

The repository is fully documented in the [Impresso Text Preparation docs](https://impresso-text-importer.readthedocs.io/en/latest/), which includes detailed descriptions of the architecture, pipelines, and usage instructions.

## Installation

Install from PyPI:

```bash
pip install impresso-text-preparation
```

Install from the repository root:

```bash
pip install -e .
```

## Repository overview

### 1. Importer submodule

- 🧩 **Overall logic**: ingest raw OCR/XML source collections, detect and select individual issues from a local file-system, convert them into Impresso canonical JSON, and upload the result to S3. Optionally, a data manifest versioning the contents of the created data can be computed on the fly and pushed to the same location on S3 (for more info on Data Versioning Manifests in Impresso see [here](https://github.com/impresso/impresso-essentials#data-versioning)).
- 📂 **Main folder**: [`text_preparation/importers/`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers) 
- 🚀 **Main launcher**: format-specific wrapper scripts under [`text_preparation/importer_scripts/`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importer_scripts), which call the generic importer engine in [`text_preparation/importers/generic_importer.py`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/generic_importer.py).

**Supported importers by format:**

#### 📰 Newspapers

Base modules for formats represented in multiple sources:
- **METS/ALTO** — Widely used format supporting both OCR only (only ALTO) or OCR+OLR (ALTO + METS) variants
  - Each source has its own METS/ALTO flavor with specific file and metadata organization logic, requiring custom processing, but they all share some core parsing and formatting logic.
  - Wrapper: `mets_alto_importer.py` |Package: [`text_preparation/importers/mets_alto`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/mets_alto)
- **TETML** — OCR format from TET (Text Extraction Toolkit) software, used for PDF-embedded OCR in some sources (e.g. FedGaz). No OLR, but has a unique structure and metadata organization.
  - Wrapper: `tetmlimporter.py` | Package: [`text_preparation/importers/tetml`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/tetml)

Per-provider submodules:
- 🇨🇭 **SNL** - Swiss National Library + LeTemps
  - Olive XML format (with OLR ✅) - First set of shared data
    - Wrapper: `oliveimporter.py` | Package: [`text_preparation/importers/olive`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/olive)
  - Mets/Alto - RERO variant (with OLR ✅) - second and third sets of shared data
    - Wrapper: `reroimporter.py` | Package: [`text_preparation/importers/rero`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/rero)
- 🇱🇺 **LUX** — National Library of Luxembourg
  - Mets/Alto - BNL variant (with OLR ✅)
  - Wrapper: `luximporter.py` | Package: [`text_preparation/importers/lux`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/lux)
- 🇫🇷 **BNF** — Bibliothèque Nationale de France
  - Mets/Alto - BNF variant (with OLR ✅)
    - Wrapper: `bnfimporter.py` | Package: [`text_preparation/importers/bnf`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/bnf)
  - Mets/Alto - BNF-Europeana variant (with OLR ✅)
    - Wrapper: `bnfen_importer.py` | Package: [`text_preparation/importers/bnf_en`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/bnf_en)
- 🇨🇭 **BCUL** — Bibliothèque Cantonale Universitaire de Lausanne
  - ABBYY format (No OLR ❌)
  - Wrapper: `bculimporter.py` | Package: [`text_preparation/importers/bcul`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/bcul)
- 🇬🇧 **BL** — British Library 
  - Mets/Alto format - OmniPage variant (with OLR ✅)
  - Wrapper: `blomnimporter.py` | Package: [`text_preparation/importers/bl/omni`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/bl/omni)
- 🇧🇪 **KBR** — Royal Library of Belgium 
  - ALTO - KBR format (No OLR ❌)
  - Wrapper: `kbrimporter.py` | Package: [`text_preparation/importers/kbr`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/kbr)
- 🇩🇪 **SUB** — Hamburg State Library
  - ALTO - SUB format (No OLR ❌)
  - Wrapper: `subimporter.py` | Package: [`text_preparation/importers/sub`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/sub)
- 🇨🇭 **SWA** — Schweizerisches Wirtschaftsarchiv 
  - ALTO - SWA format (No OLR ❌)
  - Wrapper: `swaimporter.py` | Package: [`text_preparation/importers/swa`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/swa)
- 🇨🇭 **FedGaz** — Swiss Federal Gazette
  - PDF-embedded OCR format converted to Tetml (No OLR ❌)
  - Wrapper: `fedgazimporter.py` | Package: [`text_preparation/importers/fedgaz`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/fedgaz)
- Coming soon 🕦
  - 🇦🇹 **ONB** — Austrian National Library
  - 🇳🇱 **KB** — National Library of the Netherlands
  - 🇩🇪 **SBB** - Berlin State Library

#### 📻 Radio - Radio bulletins (image-based) and Audio Broadcasts (audio based)
- 🇨🇭 **SWISSINFO** — Swissinfo radio bulletin format
  - PDF-embedded OCR originally, extracted, and converted to a custom format for import (No OLR ❌)
  - Wrapper: `swissinfoimporter.py` | Package: [`text_preparation/importers/swissinfo`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/swissinfo)
- 🇨🇭 **RTS** — Radio Télévision Suisse
  - AudioDoc ASR format
  - Wrapper: `rtsimporter.py` | Package: [`text_preparation/importers/rts`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/rts)
- 🇫🇷 **INA** — Institut National de l'Audiovisuel (France) 
  - Custom JSON-based format extracted from Whisper outputs
  - Wrapper: `inaimporter.py` | Package: [`text_preparation/importers/ina`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/importers/ina)

#### Importer CLI

Importers are executed through the format-specific wrapper scripts in `text_preparation/importer_scripts/`, which all call the generic importer engine in `text_preparation/importers/generic_importer.py` and `text_preparation/importers/core.py`.

Example command for an import of BNL data:

```bash
python -m text_preparation.importer_scripts.bnlimporter \
  --input-dir /path/to/BNL-source-data \
  --output-dir /path/to/canonical-output \
  --config-file /path/to/text_preparation/config/importer_config/import_BNL.json \
  --provider BNL \
  --chunk-size 10 \ 
  --log-file /path/to/log_file.log \
  --s3-bucket my-canonical-bucket \
  --clear \ # or --incremental
  --verbose # debug mode
```
Importer script options:

- `--input-dir`: directory containing the source data.
- `--output-dir`: directory to write canonical JSON output.
- `--temp-dir`: temporary directory for archive extraction.
- `--image-dirs`: optional image metadata directories (only for Olive format).
- `--config-file`: JSON config file for selective import.
- `--s3-bucket`: upload output to the given S3 bucket.
- `--scheduler`: Dask scheduler address.
- `--num-workers`: number of local Dask workers.
- `--clear`: remove the output directory before running.
- `--incremental`: skip already imported issues (presently in the output directory).
- `--dont-push-manifest`: do not push generated manifest to git.
- `--is-audio`: enable audio import mode.
- `--verbose`: enable detailed logging.

#### Importer Config file

The selection of the actual newspaper data to be imported can be controlled by means of a configuration file (JSON format). The path to this file is passed via the `--config-file` CLI parameter.

This JSON file contains three properties:

- `aliases`: a dictionary containing the media aliases to be imported (e.g. `GDL`);
- `exclude_aliases`: a list of the media aliases to be excluded;
- `year_only`: a boolean flag indicating whether date ranges are expressed by using years or more granular dates (in the format `YYYY/MM/DD`). When ingesting large amounts of data, this allows to organise data imports into batches or homogeneous collections.

Here is a simple configuration file:

```json
{
  "aliases": {
      "GDL": []
    },
  "exclude_aliases": [],
  "year_only": false
}
```

Here is a more complex configuration file (only contents for the decade 1950-1960 of `GDL` are processed):

```json
{
  "aliases": {
      "GDL": "1950/01/01-1960/12/31"
    },
  "exclude_aliases": [],
  "year_only": false
}
```

### 2. Rebuilder submodule

- 🔁 Overall logic: read canonical Impresso JSON from S3, join it with page/audio support data, rebuild content items, and write downstream output for Solr or Passim.
- 📂 Main folder: [`text_preparation/rebuilders/`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/rebuilders)
- 🚀 Main launcher: [`text_preparation/rebuilders/rebuilder.py`](https://github.com/impresso/impresso-text-acquisition/tree/master/text_preparation/rebuilders/rebuilder.py) .
- 🧪 Core code: 
    - core orchestrator: `rebuilder.py`
    - helper functions: `helpers.py`
    - paper-based content item specific reconstruction logic: `paper_rebuilders.py`
    - audio-based content item specific reconstruction logic: `audio_rebuilders.py`

#### Rebuilder CLI

The rebuilder reads canonical Impresso JSON from S3 and produces rebuilt files for downstream use.

Example command:

```bash
python -m text_preparation.rebuilders.rebuilder rebuild_articles \
  --input-bucket my-canonical-bucket \
  --output-dir /path/to/rebuilt-output \
  --filter-config /path/to/text_preparation/config/rebuilt_config/filter_config.json \
  --log-file /path/to/log_file.log \
  --format solr \
  --nworkers 32 \
  --temp-dir /path/to/temp_dir \
  --verbose \
  --compute-mft \ 
```

Common rebuilder options:

- `rebuild_articles`: rebuilds canonical issues into articles.
- `--input-bucket`: S3 bucket containing canonical JSON input.
- `--output-dir`: local directory for rebuilt output.
- `--output-bucket`: optional S3 bucket to upload rebuilt files.
- `--filter-config`: JSON file that defines issue batches to rebuild.
- `--format`: `solr` or `passim`.
- `--languages`: comma-separated languages to filter.
- `--nworkers`: number of local Dask workers.
- `--compute-mft`: compute the rebuilt manifest on the fly during processing.
- `--prev-manifest`: optional path to a previous manifest.
- `--git-repo`: local path to the impresso-text-acquisition repository.
- `--temp-dir`: temporary directory for repository cloning or tmp work.
- `--scheduler`: Dask scheduler address.
- `--clear`: remove output directory before rebuilding.
- `--verbose`: enable detailed logging.


### 3. Preprocessing notebooks and scripts

- 🛠 Overall logic: explore, analyze and prepare raw or messy source data before import, e.g. reorganize original files, extract OCR from PDFs, generate index metadata and format-specific preprocessing steps. Each script or notebook is customized for specific data and preprocessing tasks.
- 🚀 Main entry points:
  - `text_preparation/importer_scripts/preprocessing/bl_reorganize_original_data.py`
  - `text_preparation/importer_scripts/preprocessing/swissinfo_extract_ocr_from_pdfs.py`
  - Jupyter notebooks such as `bcul_preprocess_issues.ipynb`, `bnl_preprocess_issues.ipynb`, and `index_generator.ipynb`.

## About Impresso

### Impresso project

[Impresso - Media Monitoring of the Past](https://impresso-project.ch) is an interdisciplinary research project that aims to develop and consolidate tools for processing and exploring large collections of media archives across modalities, time, languages and national borders. The first project (2017-2021) was funded by the Swiss National Science Foundation under grant No. [CRSII5_173719](http://p3.snf.ch/project-173719) and the second project (2023-2027) by the SNSF under grant No. [CRSII5_213585](https://data.snf.ch/grants/grant/213585) and the Luxembourg National Research Fund under grant No. 17498891.

### Copyright

Copyright (C) 2024 The Impresso team.

### License

This project is available under the [GNU Affero General Public License v3 or later](https://github.com/impresso/impresso-pyindexation/blob/master/LICENSE).
