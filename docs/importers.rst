Importers
=========

Available importers
-------------------

The *Impresso Importers* already support a number of formats (and flavours of standard formats), while a few others
are currently being developed.

The following importer CLI scripts are already available:

- :py:mod:`text_preparation.scripts.oliveimporter`: importer for the *Olive XML format*, used by
  `RERO <https://www.rero.ch/>`_ to encode and deliver the majority of its newspaper data.
- :py:mod:`text_preparation.scripts.reroimporter`: importer for the *Mets/ALTO flavor* used by `RERO <https://www.rero.ch/>`_
  to encode and deliver part of its data.
- :py:mod:`text_preparation.scripts.luximporter`: importer for the *Mets/ALTO flavor* used by the `Bibliothèque nationale de Luxembourg (BNL)
  <https://bnl.public.lu/>`_ to encode and deliver its newspaper data.
- :py:mod:`text_preparation.scripts.bnfimporter`: importer for the *Mets/ALTO flavor* used by the `Bibliothèque nationale de France (BNF)
  <https://www.bnf.fr/en/>`_ to encode and deliver its newspaper data.
- :py:mod:`text_preparation.scripts.bnfen_importer`: importer for the *Mets/ALTO flavor* used by the `Bibliothèque nationale de France (BNF)
  <https://www.bnf.fr/en/>`_  to encode and deliver its newspaper data for the Europeana collection.
- :py:mod:`text_preparation.scripts.bcul_importer`: importer for the *ABBYY format* used by the `Bibliothèque Cantonale Universitaire de Lausanne (BCUL)
  <https://www.bcu-lausanne.ch/en/>`_  to encode and deliver the newspaper data which is on the `Scriptorium interface <https://scriptorium.bcu-lausanne.ch/page/home>`_.
- :py:mod:`text_preparation.scripts.swaimporter`: *ALTO flavor* of the `Basel University Library`.
- :py:mod:`text_preparation.scripts.blimporter`: importer for the *Mets/ALTO flavor* used by the `British Library (BL) <https://www.bl.uk/>`_
  to encode and deliver its newspaper data.
- :py:mod:`text_preparation.scripts.tetml`: generic importer for the *TETML format*, produced by `PDFlib TET <https://www.pdflib.com/products/tet/overview/>`_.
- :py:mod:`text_preparation.scripts.fedgaz`: importer for the *TETML format* with separate metadata file and a heuristic article segmentation,
  used to parse the `Federal Gazette <https://www.admin.ch/gov/de/start/bundesrecht/bundesblatt.html>`_.


For further details on any of these implementations, please do refer to its documentation:

.. toctree::
   :maxdepth: 1

   importers/olive
   importers/mets-alto
   importers/lux
   importers/rero
   importers/swa
   importers/bl
   importers/bnf
   importers/bnf-en
   importers/bcul
   importers/tetml
   importers/fedgaz

Command-line interface
----------------------

.. note :: All importers share the same command-line interface; only a few options
  are import-specific (see documentation below).

.. automodule:: text_preparation.importers.generic_importer


Configuration file
------------------

The selection of the actual newspaper data to be imported can be controlled by
means of a configuration file (JSON format). The path to this file is passed via the ``--config_file=``
CLI parameter.

This JSON file contains three properties:

- ``newspapers``: a dictionary containing the newspaper IDs to be imported (e.g. GDL);
- ``exclude_newspapers``: a list of the newspaper IDs to be excluded;
- ``year_only``: a boolean flag indicating whether date ranges are expressed by using years
  or more granular dates (in the format ``YYYY/MM/DD``).

.. note::

    When ingesting large amounts of data, these configuration files can help you organise
    your data imports into batches or homogeneous collections.

Here is a simple configuration file:

.. code-block:: python

  {
    "newspapers": {
        "GDL": []
      },
    "exclude_newspapers": [],
    "year_only": false
  }

This is what a more complex config file looks like (only contents for the decade 1950-1960 of GDL are processed):


.. code-block:: python

  {
    "newspapers": {
        "GDL": "1950/01/01-1960/12/31"
      },
    "exclude_newspapers": [],
    "year_only": false
  }


Writing a new importer
----------------------

Writing a new importer is easy and entails implementing two
pieces of code:

1. implementing **functions to detect the data** to import;
2. implementing from scratch **classes that handle the conversion into JSON** of your OCR format or adapt one of the existing importers.

Once these two pieces of code are in place, they can be plugged into the functions defined in :mod:`text_preparation.importers.generic_importer` so as to create a dedicated CLI script for your specific format.

For example, this is the content of ``oliveimporter.py``:

.. code-block:: python

  from text_preparation.importers import generic_importer
  from text_preparation.importers.olive.classes import OliveNewspaperIssue
  from text_preparation.importers.olive.detect import (olive_detect_issues,
                                                    olive_select_issues)

  if __name__ == '__main__':
      generic_importer.main(
          OliveNewspaperIssue,
          olive_detect_issues,
          olive_select_issues
      )

**How should the code of a new text importer be structured?** We recommend to comply to the following structure:

- :mod:`text_preparation.importers.<new_importer>.detect` will contain functions to find the data to be imported;
- :mod:`text_preparation.importers.<new_importer>.helpers` (optional) will contain ancillary functions;
- :mod:`text_preparation.importers.<new_importer>.parsers` (optional) will contain functions/classes to parse the data.
- :mod:`text_preparation/scripts/<new_importer>.py`: will contain a CLI script to run the importer.

Detect data to import
~~~~~~~~~~~~~~~~~~~~~

- the importer needs to know which data should be imported
- information about the newspaper contents is often encoded as part of
  folder names etc., thus it needs to be extracted and made explicit, by means
  of :ref:`Canonical identifiers`
- add some sample data to ``text_preparation/data/sample/<new_format>``


For example: :py:func:`~text_preparation.importers.olive.detect.olive_detect_issues`


Implement abstract classes
~~~~~~~~~~~~~~~~~~~~~~~~~~

These two classes are passed to the the importer's generic command-line interface,
see :py:func:`text_preparation.importers.generic_importer.main`


.. autoclass:: text_preparation.importers.classes.NewspaperIssue
  :members:

.. autoclass:: text_preparation.importers.classes.NewspaperPage
  :members:


Write an importer CLI script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This script imports passes the new :class:`NewspaperIssue` class, together with the-newly
defined *detect* functions, to the ``main()`` function of the generic importer CLI
:func:`text_preparation.importers.generic_importer.main`.

Test
~~~~

Create a new test file named ``test_<new_importer>_importer.py`` and add it to ``tests/importers/``.

This file should contain at the very minimum a test called :func:`test_import_issues`, which

- detects input data from ``text_preparation/data/sample/<new_format>``
- writes any output to ``text_preparation/data/out/``.
