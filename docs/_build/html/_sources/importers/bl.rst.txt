British Library importers
=========================

The British Library shared data in several different formats with us.
We will be processing them one at a time based on the numbr of media titles they cover.
All importers will share their detect functions but have unique classes, which will act as submodules of the "bl" module.

BL Detect functions
-------------------

.. automodule:: text_preparation.importers.bl.detect
  :members:


1. BL OmniPage Custom classes
-----------------------------

The first BL OCR format, which we call "OmniPage" is based on the METS/ALTO standard.
This importer extends the generic Mets/Alto importer.
Almost 300 media titles from the BL are in this format.

.. automodule:: text_preparation.importers.bl.omni.classes
  :members:


