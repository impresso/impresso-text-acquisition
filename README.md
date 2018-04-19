# Impresso Text Importer

## Purpose

Import the data from Olive OCR XML files into a canonical JSON format defined by the Impresso project (see [documentation of schemas](./README_schemata.md)).

## Input data

A sample of the input data for this script can be found in [sample_data/](sample_data/) (data for Gazette de Lausanne (GDL), Feb 2-5 1900).

## Usage

Run the script sequentially:

    impresso-txt-importer --input-dir=text_importer/data/sample_data/ --output-dir=text_importer/data/out/ --temp-dir=text_importer/data/tmp/ --image-dir="/Volumes/project_impresso/images/" --filter="journal=IMP" --log-file=text_importer/data/import_test.log

or in parallel:

    impresso-txt-importer --input-dir=text_importer/data/sample_data/ --output-dir=text_importer/data/out/ --temp-dir=text_importer/data/tmp/ --image-dir="/Volumes/project_impresso/images/" --filter="journal=IMP" --log-file=text_importer/data/import_test.log --parallelize

For further info about the usage, see:

    impresso-txt-importer --help

## TODO

- [x] convert coordinates using Maud's code
- [x] add a sanity check: IDs of XML files in `Document.zip` should be found in `info.json`
- [x] define and implement `page.json` schema
- [x] revise and implement the `info.json` schema
- [x] discuss canonical naming of pages vs content items: e.g. `GDL-1900-01-02-a-i0051` and `GDL-1900-01-02-a-p0001.json`
- [x] merge token sequences not at the end of line (`<QID>` elements)
- [x] more robust handling of exceptions
- [x] parse `styleGallery.txt` files
- [x] (refactoring) move all olive-related functions into `importers/olive.py`
- [x] replace `partOf` field with canonical article ID
- [x] deal with `<QW>` elements when extracting box coordinates
