# Impresso Text Importer

## Purpose

Import the data from various OCR formats (Olive XML, Mets/Alto, etc.) into a canonical JSON format defined by the Impresso project (see [documentation of schemas](https://github.com/impresso/impresso-schemas)). A sample of the input data that this importer can deal with can be found in [sample_data/](sample_data/).

## Documentation

Code documentation, including a brief guide on how to write a custom importer, can be found at <https://impresso.github.io/impresso-text-acquisition/>. 

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

## License

The 'impresso - Media Monitoring of the Past' project is funded by the Swiss National Science Foundation (SNSF) under  grant number [CRSII5_173719](http://p3.snf.ch/project-173719) (Sinergia program). The project aims at developing tools to process and explore large-scale collections of historical newspapers, and at studying the impact of this new tooling on historical research practices. More information at https://impresso-project.ch.

Copyright (C) 2020  The *impresso* team (contributors to this program: Matteo Romanello, Maud Ehrmann, Alex Flückliger, Edoardo Tarek Höelzl).

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of merchantability or fitness for a particular purpose. See the [GNU Affero General Public License](https://github.com/impresso/impresso-text-acquisition/blob/master/LICENSE) for more details.

