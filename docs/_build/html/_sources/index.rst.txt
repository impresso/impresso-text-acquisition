.. Impresso TextImporter documentation master file, created by
   sphinx-quickstart on Mon Aug 12 14:50:13 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Impresso Text Preparation's documentation!
=====================================================

Impresso Text Preparation is a library resulting from the merge of the previous package "Text-Importer" and the "Text-Rebuilder" from Impresso-pycommons.
The goal for this merge was to regroup in one place all the code that was used as a first unified preparation for all data sources: creating the Impresso `canonical` and `rebuilt` formats from the data provided by partners.

This grouping means that there are two main modules to this library:
- Importers: first step of data processing, they convert OCR and OLR data (coming in a variety of formats - e.g. Olive XML, various flavors of Mets/Alto XML, etc.) into `Impresso's unified Canonical JSON format <https://github.com/impresso/impresso-schemas>`_ , which represents Newspaper issues and pages.
- Rebuilders: second step of the data processing where the content-items (articles, images, tables, headers etc) from the canonical format are extracted and "rebuilt" in preparation for the semantic augmentation and NLP processings that follow in the pipeline.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   architecture
   importers
   rebuilders
   utils


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
