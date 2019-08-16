TextImporter
============

Available importers
-------------------


- :py:mod:`oliveimporter.py`: Olive XML OCR of `RERO <https://www.rero.ch/>`_
- :py:mod:`reroimporter.py`: Mets/ALTO flavor of `RERO <https://www.rero.ch/>`_
- :py:mod:`luximporter.py`: Mets/ALTO flavor of the Bibliotheque National du Luxembourg


.. toctree::
   :maxdepth: 1
   :caption: Importers' APIs:

   importers/olive
   importers/mets-alto
   importers/lux
   importers/rero

Command-line interface
----------------------

.. note :: All importers share the same command-line interface; only a few options
  are import-specific (see documentation below).

.. automodule:: text_importer.importers.generic_importer


Configuration files
-------------------

todo

Utilities
---------

.. automodule:: text_importer.utils
  :members:
