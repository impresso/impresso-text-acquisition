Write your own importer
=======================

TLDR;
-----

To write a custom importer is easy and entails implementing two
pieces of code:

1. implementing functions to find the data that should be imported.
2. implementing classes that handle the data format you'd like to import.

**TODO**: Given an example.

**TODO**: How to structure the code.

Detecting data to import
------------------------

Subclassing abstract classes
----------------------------

.. autoclass:: text_importer.importers.classes.NewspaperIssue
  :members:

.. autoclass:: text_importer.importers.classes.NewspaperPage
  :members:
