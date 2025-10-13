Overview
========

Data architecture
-----------------

`Impreso Text Preparation`, composed of the `Importer` and the `Rebuilder` is the main part of the data architecture defined in
the framework of the impresso project to store and process a large-scale
archive of historical newspapers and radio (broadcasts and bulletins). To understand the importer's logic
is worth touching upon the key points of the architecure into which it fits.

Canonical identifiers
*********************

Canonical identifiers are defined at the following levels:

1. newspaper or broadcast issue
2. newspaper page or audio record
3. paper-based content item (e.g. article, advertisement, weather forecast, obituary, etc.) or audio-record content item (audio broadcast emission)


Issue IDs
#########

- template: ``{media_alias}-{date.year}-{date.month}-{date.day}-{edition}``
- regex pattern: ``^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}$``
- examples: ``GDL-1900-01-02-a``, ``luxzeit1858-1858-12-7-a``

Page IDs
########

- template: ``{media_alias}-{date.year}-{date.month}-{date.day}-{edition}-p{page_number}``
- regex pattern: ``^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-p[0-9]{4}$``
- examples: ``GDL-1900-01-02-a-p0004``, ``luxzeit1858-1858-12-7-a-p0002``

Audio Records IDs
#################

- template: ``{media_alias}-{date.year}-{date.month}-{date.day}-{edition}-r{record_number}``
- regex pattern: ``^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-r[0-9]{4}$``
- examples: ``GDL-1900-01-02-a-p0004``, ``luxzeit1858-1858-12-7-a-p0002``

Content item IDs
################

- template: ``{media_alias}-{date.year}-{date.month}-{date.day}-{edition}-i{item_number}``
- regex pattern: ``^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-i[0-9]{4}$``
- examples: ``GDL-1900-01-02-a-i0048``, ``JDG-1901-01-01-a-i0031``

Some things to note about these templates:

- ``media_alias`` is an arbitrary string, not containing white spaces, unambiguously identifying a given media title
- ``page_number`` is a four-digits integer (zeroes are used for filling)
- ``record_number`` is a four-digits integer (zeroes are used for filling). In most cases a broadcast only has one MP3 recording, but is here for conformity.
- ``edition``: in case of newspapers published multiple times per day, a lowercase letter is used to indicate the edition number: ``a`` for the first, ``b`` for the second, etc.
- ``item_number``: is a four-digits integer (zeroes are used for filling); **NB**: content item IDs are **expected to remain stable** across any two runs of the importer given the same input data.

Data packaging
**************

The JSON data produced by the ``Importer`` and ``Rebuilder`` are packaged into ``.bz2`` archives for efficient storage. Each archive consists of one JSON-line file, where each line contains a JSON document. The JSON schemas are described `here <https://github.com/impresso/impresso-schemas>`_.

In Impresso we use an S3 solution for distributed storage to store newspaper data and accessed them at processing time.

Issue data
##########

They are packaged **by media title and by year** (as they tend to be very small files). Each archive contains, one document per line, all issues of a media title that appeared in that year.

Examples: ``GDL-1900-issues.jsonl.bz2`` contains all issues of the *Gazette de Lausanne* published in 1900.

Page or Audio Record data
#########################

They are packaged **by issue**. Each archive contains, one document per line, all JSON pages belonging to a given issue (edition).

Examples: 
- ``GDL-1900-01-01-a-pages.jsonl.bz2`` contains all issues of the *Gazette de Lausanne* (= ``GDL``) published on January 1, 1900.
- ``RDN-1950-01-12-a-audios.jsonl.bz2`` contains all issues of *La ronde des nations* (= ``RDN``) published on January 12, 1950.

Rebuilt data
############

They are packaged **by media title and by year**. Each archive contains, one document per line, all JSON content-items belonging to a given title and year.

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
