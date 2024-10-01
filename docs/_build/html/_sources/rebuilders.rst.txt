Rebuilders
==========

Once the canonical data has been generated, it is rebuilt into two variants of the `rebuilt` format:
- ``Solr Rebuilder``: Returns the base format for all subsequent text proessing steps, keeping only the text data and important information (line breaks, regions etc).
- ``Passim Rebuilder``: Returns the base format for the text-reuse processing, which is done with a software called Passim. 
This format is very similar to the solr rebuilt on all counts, but simply has slightly different properties and property names.

Both of these are generated with the `text_preparation.rebuilders.rebuilder` module, and one can select which format to produce with the `format` parameter.

Rebuild functions
-----------------

A set of functions to transform JSON files in **impresso's canonical format** into a number of JSON-based formats for different purposes.

.. automodule:: text_preparation.rebuilders.rebuilder
   :members:
   :undoc-members:
   :show-inheritance:

Helpers
-------

.. automodule:: text_preparation.rebuilders.helpers
   :members:
   :undoc-members:
   :show-inheritance:

Config file example
-------------------

(from file: `text_preparation.config.rebuilt_cofig.cluster.json`)::

    [{"GDL": [1948, 1999]}, {"GDL": [1900, 1948]}, {"GDL": [1850, 1900]}, {"schmiede": [1916, 1920]}]

Several newspaper titles can be added to the same configuration file.
If there is a newspaper title that contains a very large number of data (many issues and/or many years), 
it is advised to separate this processing into parts as is shown above with `GDL` to reduce the memory and computing needs, 
as well as ensure minimal outputs are lost if the process stops early due to an error.

Running using Runai
-------------------

Members of Impresso and EPFL can use the computing platform Runai to produce the rebuilt data. 
Indications to run data in this way are available [here](https://github.com/impresso/impresso-infrastructure).