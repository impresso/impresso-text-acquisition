"""This module contains functions to detect Tetml OCR data to be imported."""

from collections import namedtuple
from typing import List

from impresso_essentials.utils import IssueDir

from text_preparation.importers.detect import (
    detect_issues,
    select_issues,
)

TetmlIssueDir = namedtuple("TetmlIssueDirectory", ["alias", "date", "edition", "path"])

"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

.. note ::

    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

:param str alias: Newspaper alias
:param datetime.date date: Publication date
:param str edition: Edition of the newspaper issue ('a', 'b', 'c', etc.)
:param str path: Path to the directory containing OCR data

>>> from datetime import date
>>> i = TetmlIssueDir('GDL', date(1900,1,1), 'a', './GDL-1900-01-01/')
"""


def tetml_detect_issues(
    base_dir: str, alias_filter: set = None, exclude: bool = False
) -> List[TetmlIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that RERO used to
    organize the dump of Tetml OCR data.

    :param str base_dir: Path to the base directory of newspaper data.
    :param set alias_filter: IDs of newspapers to consider.
    :param bool exclude: Whether ``alias_filter`` should determine exclusion.
    :return: List of `TetmlIssueDir` instances, to be imported.
    """

    issues = detect_issues(base_dir, alias_filter=alias_filter, exclude=exclude)

    return [TetmlIssueDir(x.alias, x.date, x.edition, x.path) for x in issues]


def tetml_select_issues(base_dir: str, config: dict) -> List[TetmlIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`tetml_detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :return: List of `TetmlIssueDir` instances, to be imported.
    """
    issues = select_issues(config, base_dir)

    return [TetmlIssueDir(x.alias, x.date, x.edition, x.path) for x in issues]
