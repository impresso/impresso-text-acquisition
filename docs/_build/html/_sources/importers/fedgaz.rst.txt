FedGaz TETML importer
=======================

This importer is an adapted version of the generic TETML importer to parse the Federal Gazette data, which is complemented by an additional metadata file besides the document files in the TETML format.

The separate metadata file is used to look up additional information for documents not provided by the respective TETML files. The file needs to be located in the top folder of the input directory and is named `metadata.tsv`. Moreover, the dataset provides the following columns: `article_docid`, `issue_date,` `article_title`, `volume_language`, `canonical_page_first`, `canonical_page_last`, `pruned`.
Notably, the tetml file's name needs to correspond with the `article_docid` of the metadata as it is used as a key to look up other information (e.g., `10000032.word.tetml`).

By default, the importer assumes that an article starts on a new page. Practically, there are many cases of in-page segmentations (i.e., an article starts on the same page where the previous ends). Thus, the FedGaz importer also performs a heuristic article segmentation for documents that share the page with the subsequent articles, indicated by the attribute `pruned`.
Unless the attribute is set to `True`, the content of the shared page is automatically assigned to the subsequent article, limiting an article to its last full page. However, in case of an indicated pruning, the importer performs a fuzzy search to locate the subsequent article title on its starting page. If successful, the procedure sets the article boundary at the matching position and reassigns the content accordingly.



Custom classes
--------------

.. automodule:: text_importer.importers.fedgaz.classes
  :members:
