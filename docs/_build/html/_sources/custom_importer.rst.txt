Write your own importer
=======================

TLDR;
-----

Writing a custom importer is easy and entails implementing two
pieces of code:

1. implementing functions to find the data that should be imported.
2. implementing classes that handle the data format you'd like to import.

**TODO**: Given an example.

**TODO**: How to structure the code.

Minting canonical IDs
---------------------

TBD

Detecting data to import
------------------------

- the importer needs to know which data should be imported
- information about the newspaper contents is often encoded as part of
  folder names etc., thus it needs to be extracted and made explicit

Subclassing abstract classes
----------------------------

.. autoclass:: text_importer.importers.classes.NewspaperIssue
  :members:

.. autoclass:: text_importer.importers.classes.NewspaperPage
  :members:
