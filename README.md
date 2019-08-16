# Impresso Text Importer

## Purpose

Import the data from Olive OCR XML files into a canonical JSON format defined by the Impresso project (see [documentation of schemas](./README_schemata.md)).

## Input data

A sample of the input data for this script can be found in [sample_data/](sample_data/).

## Development settings

**Version**

`3.6`

**Documentation**

Python docstring style https://pythonhosted.org/an_example_pypi_project/sphinx.html

Sphinx configuration file (`docs/conf.py`) generated with:

    sphinx-quickstart --ext-githubpages

To compile the documentation

```bash
cd docs/
make html
```

To view locally:

Install `http-sever` (a node-js package):

    npm install http-server -g

Then:

    cd docs
    http-server

And you'll be able to browse it at <http://127.0.0.1:8080>.



**Testing**

Python pytest framework: https://pypi.org/project/pytest/

Tox: https://tox.readthedocs.io/en/latest/

**Passing arguments**

Doctopt: http://docopt.org/

**Style**

4 space indentation
