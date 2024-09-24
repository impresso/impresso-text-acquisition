Overview
========

Data architecture
-----------------

`Impreso Text Preparation`, composed of the `Importer` and the `Rebuilder` is the main part of the data architecture defined in
the framework of the impresso project to store and process a large-scale
archive of historical newspapers. To understand the importer's logic
is worth touching upon the key points of the architecure into which it fits.

Canonical identifiers
*********************

Canonical identifiers are defined at the following levels:

1. newspaper issue
2. newspaper page
3. content item (e.g. article, advertisement, weather forecast, obituary, etc.)


Issue IDs
#########

- template: ``{newspaper_id}-{date.year}-{date.month}-{date.day}-{edition}``
- examples: ``GDL-1900-01-02-a``, ``luxzeit1858-1858-12-7-a``

Page IDs
########

- template: ``{newspaper_id}-{date.year}-{date.month}-{date.day}-{edition}-p{page_number}``
- examples: ``GDL-1900-01-02-a-p0004``, ``luxzeit1858-1858-12-7-a-p0002``


Content item IDs
################

- template: ``{newspaper_id}-{date.year}-{date.month}-{date.day}-{edition}-i{item_number}``
- examples: ``GDL-1900-01-02-a-i0048``, ``JDG-1901-01-01-a-i0031``

Some things to note about these templates:

- ``newspaper_id`` is an arbitrary string, not containing white spaces, unambiguously identifying a given newspaper
- ``page_number`` is a four-digits integer (zeroes are used for filling)
- ``edition``: in case of newspapers published multiple times per day, a lowercase letter is used to indicate the edition number: ``a`` for the first, ``b`` for the second, etc.
- ``item_number``: is a four-digits integer (zeroes are used for filling); **NB**: content item IDs are **expected to remain stable** across any two runs of the importer given the same input data.

Data packaging
**************

The JSON data produced by the ``Importer`` and ``Rebuilder`` are packaged into ``.bz2`` archives for efficient storage. Each archive consists of one JSON-line file, where each line contains a JSON document. The JSON schemas are described `here <https://github.com/impresso/impresso-schemas>`_.

In Impresso we use an S3 solution for distributed storage to store newspaper data and accessed them at processing time.

Issue data
##########

They are packaged **by newspaper and by year** (as they tend to be very small files). Each archive contains, one document per line, all issues of a newspaper that appeared in that year.

Examples: ``GDL-1900-issues.jsonl.bz2`` contains all issues of the *Gazette de Lausanne* published in 1900.

Page data
#########

They are packaged **by newspaper issue**. Each archive contains, one document per line, all JSON pages belonging to a given newspaper issue (edition).

Examples: ``GDL-1900-01-01-a-pages.jsonl.bz2`` contains all issues of the *Gazette de Lausanne* (= ``GDL``) published on January 1, 1900.

Rebuilt data
############

They are packaged **by newspaper and by year**. Each archive contains, one document per line, all JSON content-items belonging to a given newspaper and year.

Examples: ``GDL-1900.jsonl.bz2`` contains all rebuilt data of the *Gazette de Lausanne* (= ``GDL``) published in 1900.

Image data
**********

They are expected to be delivered via a dedicated IIIF endpoint, and typically stored in an image server. To each newspaper page corresponds an image file.

.. note ::

  In case the canonical ID of a page and the internal ID of its image differ, the content provider is expected to be able to provide a mapping of the two identifier systems.

Processing
----------

.. automodule:: text_preparation.importers.core
  :members:
