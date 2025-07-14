"""This module contains functions to detect Olive OCR data to be imported."""

from collections import namedtuple
from typing import Any

from impresso_essentials.utils import IssueDir
from text_preparation.importers.detect import (
    detect_issues,
    select_issues,
)

OliveIssueDir = namedtuple("OliveIssueDirectory", ["journal", "date", "edition", "path"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    journal (str): Newspaper ID.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = OliveIssueDir('GDL', date(1900,1,1), 'a', './GDL-1900-01-01/')
"""


def dir2olivedir(issue_dir: IssueDir) -> OliveIssueDir:
    """Helper function that injects access rights info into an ``IssueDir``.

    Note:
        This function is called internally by :func:`olive_detect_issues`.

    Args:
        issue_dir (IssueDir): Input ``IssueDir`` object.
        access_rights (dict[str, dict[str, str]]): Access rights information.

    Returns:
        OliveIssueDir: New ``OliveIssueDir`` object.
    """

    return OliveIssueDir(issue_dir.journal, issue_dir.date, issue_dir.edition, issue_dir.path)


def olive_detect_issues(
    base_dir: str,
    alias_filter: set | None = None,
    exclude: bool = False,
) -> list[OliveIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that RERO used to
    organize the dump of Olive OCR data.
    TODO remove access rights, and potentially identify if the function is needed
    or if it should be replaced by importers.detect.detect_issues

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        alias_filter (set | None, optional): IDs of newspapers to consider.
            Defaults to None.
        exclude (bool, optional): Whether ``alias_filter`` should determine
            exclusion. Defaults to False.

    Returns:
        list[OliveIssueDir]: List of `OliveIssueDir` instances, to be imported.
    """
    return [
        dir2olivedir(x) for x in detect_issues(base_dir, alias_filter=alias_filter, exclude=exclude)
    ]


def olive_select_issues(base_dir: str, config: dict[str, Any]) -> list[OliveIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`olive_detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.
    TODO potentially identify if the function is needed
    or if it should be replaced by importers.detect.select_issues

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict[str, Any]): Config dictionary for filtering.

    Returns:
        list[OliveIssueDir]: List of `OliveIssueDir` instances, to be imported.
    """
    return [dir2olivedir(x) for x in select_issues(config, base_dir)]
