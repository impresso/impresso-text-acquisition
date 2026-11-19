BNF Mets/Alto importers
=======================

These importers extend the generic Mets/Alto importer. They handle the OCR newspaper data
provided by the BNF, which comes in three formats. Each format has its own canonical classes,
and they share an issue-index-based detection module:

- **BNF-OCR** (``no_olr``): titles without OLR, made of ALTO pages and an IIIF ``manifest.json``.
- **MP-OLR** (``mp_olr``): "Marché Presse" titles with OLR (METS + ALTO).
- **EN-OLR** (``en_olr``): "Europeana" titles with OLR (METS + ALTO).

See ``text_preparation/importers/bnf/README.md`` for the issue indices, the preprocessing
pipeline and the current ingestion status.

BNF-OCR (no OLR) classes
------------------------

.. automodule:: text_preparation.importers.bnf.no_olr.classes
  :members:

BNF-MP (Marché Presse OLR) classes
----------------------------------

.. automodule:: text_preparation.importers.bnf.mp_olr.classes
  :members:

BNF-EN (Europeana OLR) classes
------------------------------

.. automodule:: text_preparation.importers.bnf.en_olr.classes
  :members:

BNF Detect functions
--------------------

.. automodule:: text_preparation.importers.bnf.detect
  :members:

BNF Helper and Parser methods
-----------------------------

.. automodule:: text_preparation.importers.bnf.helpers
  :members:
.. automodule:: text_preparation.importers.bnf.parsers
  :members:
