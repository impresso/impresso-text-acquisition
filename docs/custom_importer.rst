Writing a new importer
=======================

TLDR;
-----

Writing a new importer is easy and entails implementing two
pieces of code:

1. implementing **functions to find the data** to import;
2. implementing from scratch **classes that handle the conversion into JSON** of your OCR format or adapt one of the existing importers.

**How should the code of a new text importer be structured?** We recommend to comply to the following structure:

- :mod:`<new_importer>/classes.py`: will contain implementations of abstract classes;
- :mod:`<new_importer>/detect.py` will contain functions to find the data to be imported;
- :mod:`<new_importer>/helpers.py` (optional) will contain ancillary functions;
- :mod:`<new_importer>/parsers.py` (optional) will contain functions/classes to parse the data.

Detect data to import
------------------------

- the importer needs to know which data should be imported
- information about the newspaper contents is often encoded as part of
  folder names etc., thus it needs to be extracted and made explicit
- add some sample data to ``text_importer/data/sample/<new_format>``


For example: :py:func:`~text_importer.importers.olive.detect.olive_detect_issues`

Mint canonical IDs
---------------------

See :ref:`Canonical identifiers`


Implement abstract classes
----------------------------

.. autoclass:: text_importer.importers.classes.NewspaperIssue
  :members:

.. autoclass:: text_importer.importers.classes.NewspaperPage
  :members:

Test
----

Create a new test file named ``test_<new_importer>_importer.py`` and add it to ``tests/importers/``.

This file should contain at the very minimum a test called :func:`test_import_issues`, which

- detects input data from ``text_importer/data/sample/<new_format>``
- writes any output to ``text_importer/data/out/``.
