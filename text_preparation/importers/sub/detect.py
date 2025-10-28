"""This module contains helper functions to find SUB OCR data to import."""

import logging
import os
import json
import string
from collections import namedtuple

from text_preparation.importers.detect import _apply_datefilter

SubIssueDir = namedtuple("IssueDirectory", ["provider", "alias", "date", "edition", "path"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "SUB"
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    mit_file_type (str): Type of mit file for this issue (json or xml).

>>> from datetime import date
>>> i = BculIssueDir(
    provider='SUB',
    alias='hamb_echo', 
    date=datetime.date(1919, 02, 19), 
    edition='a', 
    path='./SUB/Hamburger_Echo/1919/02/19/Morgenausgabe'
)
"""


def dir2issue(path: str) -> SubIssueDir | None:
    # TODO define for SUB case
    pass


def detect_issues(base_dir: str) -> list[SubIssueDir]:
    # TODO define for SUB case
    pass


def select_issues(base_dir: str, config: dict) -> list[SubIssueDir] | None:
    # TODO define for SUB case
    pass
