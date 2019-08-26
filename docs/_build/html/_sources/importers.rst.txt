TextImporter
============

Available importers
-------------------

The *Impresso TextImporter* already supports a number of formats (and flavours of standard formats), while a few others
are currently being developed.

The following importer CLI scripts are already available:

- :py:mod:`text_importer.scripts.oliveimporter`: importer for the *Olive XML format*, used by
  `RERO <https://www.rero.ch/>`_ to encode and deliver the majority of its newspaper data.
- :py:mod:`text_importer.scripts.reroimporter`: importer for the Mets/ALTO flavor used by `RERO <https://www.rero.ch/>`_
  to encode and deliver part of its data.
- :py:mod:`text_importer.scripts.luximporter`: importer for the Mets/ALTO flavor used by the `Bibliothèque nationale de Luxembourg (BNL)
  <https://bnl.public.lu/>`_ to encode and deliver its newspaper data.

For further details on any of these implementations, please do refer to its documentation:

.. toctree::
   :maxdepth: 1

   importers/olive
   importers/mets-alto
   importers/lux
   importers/rero

Command-line interface
----------------------

.. note :: All importers share the same command-line interface; only a few options
  are import-specific (see documentation below).

.. automodule:: text_importer.importers.generic_importer


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

.. code-block::

  {
    "newspapers": {
        "GDL": []
      },
    "exclude_newspapers": [],
    "year_only": false
  }

This is what a more complex config file looks like (only contents for the decade 1950-1960 of GDL are processed):


.. code-block::

  {
    "newspapers": {
        "GDL": "1950/01/01-1960/12/31"
      },
    "exclude_newspapers": [],
    "year_only": false
  }


Utilities
---------

.. automodule:: text_importer.utils
  :members:
