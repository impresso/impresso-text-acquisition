import logging
import os
import json
import string
from datetime import date
from collections import namedtuple

from dask import bag as db

from text_preparation.importers.detect import _apply_datefilter
from text_preparation.importers.bcul.helpers import parse_date, find_mit_file

METADATA_FILENAME = "ina_metadata.json"

INAIssueDir = namedtuple(
    "IssueDirectory", ["alias", "date", "edition", "path", "metadata_file"]
)
"""A light-weight data structure to represent a radio audio broadcast issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of bulletins published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    alias (str): Bulletin alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = INAIssueDir(
    alias='SOC_CJ', 
    date=datetime.date(1940, 07, 22), 
    edition='a', 
    path='./SOC_CJ/1940/07/22/a', 
)
"""
