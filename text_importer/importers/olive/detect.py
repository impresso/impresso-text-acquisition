"""This module contains functions to detect Olive OCR data to be imported.
"""

import json
from collections import namedtuple
from typing import Any

from impresso_commons.path.path_fs import (IssueDir, detect_issues,
                                           select_issues)

from text_importer.utils import get_access_right

OliveIssueDir = namedtuple(
    "OliveIssueDirectory", [
        'journal',
        'date',
        'edition',
        'path',
        'rights'
    ]
)
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
    rights (str): Access rights on the data (open, closed, etc.).

>>> from datetime import date
>>> i = OliveIssueDir('GDL', date(1900,1,1), 'a', './GDL-1900-01-01/', 'open')
"""


def dir2olivedir(
    issue_dir: IssueDir, access_rights: dict[str, dict[str, str]]
) -> OliveIssueDir:
    """Helper function that injects access rights info into an ``IssueDir``.

    Note:
        This function is called internally by :func:`olive_detect_issues`.

    Args:
        issue_dir (IssueDir): Input ``IssueDir`` object.
        access_rights (dict[str, dict[str, str]]): Access rights information.

    Returns:
        OliveIssueDir: New ``OliveIssueDir`` object.
    """
    ar = get_access_right(issue_dir.journal, issue_dir.date, access_rights)
    return OliveIssueDir(
        issue_dir.journal,
        issue_dir.date,
        issue_dir.edition,
        issue_dir.path,
        rights=ar
    )


def olive_detect_issues(
    base_dir: str,
    access_rights: str,
    journal_filter: set | None = None,
    exclude: bool = False
) -> list[OliveIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that RERO used to
    organize the dump of Olive OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        access_rights (str): Path to ``access_rights.json`` file.
        journal_filter (set | None, optional): IDs of newspapers to consider. 
            Defaults to None.
        exclude (bool, optional): Whether ``journal_filter`` should determine
            exclusion. Defaults to False.

    Returns:
        list[OliveIssueDir]: List of `OliveIssueDir` instances, to be imported.
    """
    with open(access_rights, 'r') as f:
        access_rights_dict = json.load(f)

    issues = detect_issues(
        base_dir,
        journal_filter=journal_filter,
        exclude=exclude
    )

    return [dir2olivedir(x, access_rights_dict) for x in issues]


def olive_select_issues(
    base_dir: str,
    config: dict[str, Any],
    access_rights: str
) -> list[OliveIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`olive_detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict[str, Any]): Config dictionary for filtering.
        access_rights (str): Path to ``access_rights.json`` file.

    Returns:
        list[OliveIssueDir]: List of `OliveIssueDir` instances, to be imported.
    """
    with open(access_rights, 'r') as f:
        access_rights_dict = json.load(f)

    issues = select_issues(config, base_dir)

    return [dir2olivedir(x, access_rights_dict) for x in issues]
