FedGaz TETML importer
=======================

This importer was developed to parse the OCR document data of the Federal Gazette in the TETML format, produced by PDFlib TET, complemented with separate metadata file.

The metadata, which is not contained in the TETML dat,atext_importer.importers.tetml.classes.TetmlNewspaperPage is looked up in a complementary dataset located in the top folder of a source and is named `metadata.tsv`.
The dataset provides the following columns: `article_docid`, `issue_date,` `article_title`, `volume_language`, `canonical_page_first`, `canonical_page_last`, `pruned`.
Moreover, the name of the tetml file needs to correspond with the `article_docid` in the metadata as it is used as lookup id (e.g.,`10000032.word.tetml`).

Additonaly, the importer performs an heuristic article segmentation for documents that end on the same page where a new document already begins.
This in-page segmentation are indicated with the attribute `pruned`. When the attribute is set to false, the content of the last overlapping page is assigned to the subsequent article.
Otherwise, a fuzzy search on the title is performed to draw the correct boundary and reassign the content to respective article.

Custom classes
--------------

.. automodule:: text_importer.importers.fedgaz.classes
  :members:
