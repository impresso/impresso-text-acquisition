Writing a new importer
=======================

TLDR;
-----

Writing a new importer is easy and entails implementing two
pieces of code:

1. implementing **functions to detect the data** to import;
2. implementing from scratch **classes that handle the conversion into JSON** of your OCR format or adapt one of the existing importers.

Once these two pieces of code are in place, they can be plugged into the functions defined in :mod:`text_importer.importers.generic_importer` so as to create a dedicated CLI script for your specific format.

For example, this is the content of ``oliveimporter.py``:

.. code-block:: python

  from text_importer.importers import generic_importer
  from text_importer.importers.olive.classes import OliveNewspaperIssue
  from text_importer.importers.olive.detect import (olive_detect_issues,
                                                    olive_select_issues)

  if __name__ == '__main__':
      generic_importer.main(
          OliveNewspaperIssue,
          olive_detect_issues,
          olive_select_issues
      )

**How should the code of a new text importer be structured?** We recommend to comply to the following structure:

- :mod:`text_importer.importers.<new_importer>.detect` will contain functions to find the data to be imported;
- :mod:`text_importer.importers.<new_importer>.helpers` (optional) will contain ancillary functions;
- :mod:`text_importer.importers.<new_importer>.parsers` (optional) will contain functions/classes to parse the data.
- :mod:`text_importer/scripts/<new_importer>.py`: will contain a CLI script to run the importer.

Detect data to import
------------------------

- the importer needs to know which data should be imported
- information about the newspaper contents is often encoded as part of
  folder names etc., thus it needs to be extracted and made explicit, by means
  of :ref:`Canonical identifiers`
- add some sample data to ``text_importer/data/sample/<new_format>``


For example: :py:func:`~text_importer.importers.olive.detect.olive_detect_issues`


Implement abstract classes
----------------------------

These two classes are passed to the the importer's generic command-line interface,
see :py:func:`text_importer.importers.generic_importer.main`


.. autoclass:: text_importer.importers.classes.NewspaperIssue
  :members:

.. autoclass:: text_importer.importers.classes.NewspaperPage
  :members:


Write an importer CLI script
------------------------------

This script imports passes the new :class:`NewspaperIssue` class, together with the-newly
defined *detect* functions, to the ``main()`` function of the generic importer CLI
:func:`text_importer.importers.generic_importer.main`.

Test
----

Create a new test file named ``test_<new_importer>_importer.py`` and add it to ``tests/importers/``.

This file should contain at the very minimum a test called :func:`test_import_issues`, which

- detects input data from ``text_importer/data/sample/<new_format>``
- writes any output to ``text_importer/data/out/``.
