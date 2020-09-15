import json
from collections import namedtuple
from typing import List

from impresso_commons.path.path_fs import IssueDir, detect_issues, select_issues

from text_importer.utils import get_access_right

TetmlIssueDir = namedtuple("TetmlIssueDirectory", ["journal", "date", "edition", "path", "rights"])

"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

.. note ::

    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

:param str journal: Newspaper ID
:param datetime.date date: Publication date
:param str edition: Edition of the newspaper issue ('a', 'b', 'c', etc.)
:param str path: Path to the directory containing OCR data
:param str rights: Access rights on the data (open, closed, etc.)

>>> from datetime import date
>>> i = TetmlIssueDir('GDL', date(1900,1,1), 'a', './GDL-1900-01-01/', 'open')
"""


def dir2tetmldir(issue_dir: IssueDir, access_rights: dict) -> TetmlIssueDir:
    """Helper function that injects access rights info into an ``IssueDir``.

    .. note ::
        This function is called internally by :func:`tetml_detect_issues`.

    :param IssueDir issue_dir: Input ``IssueDir`` object.
    :param dict access_rights: Access rights information.
    :return: New ``TetmlIssueDir`` object.
    """
    ar = get_access_right(issue_dir.journal, issue_dir.date, access_rights)

    return TetmlIssueDir(
        issue_dir.journal, issue_dir.date, issue_dir.edition, issue_dir.path, rights=ar
    )


def tetml_detect_issues(
    base_dir: str, access_rights: str, journal_filter: set = None, exclude: bool = False
) -> List[TetmlIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that RERO used to
    organize the dump of Tetml OCR data.

    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Path to ``access_rights.json`` file.
    :param set journal_filter: IDs of newspapers to consider.
    :param bool exclude: Whether ``journal_filter`` should determine exclusion.
    :return: List of `TetmlIssueDir` instances, to be imported.
    """

    with open(access_rights, "r") as f:
        access_rights_dict = json.load(f)

    issues = detect_issues(base_dir, journal_filter=journal_filter, exclude=exclude)

    return [dir2tetmldir(x, access_rights_dict) for x in issues]


def tetml_select_issues(base_dir: str, config: dict, access_rights: str) -> List[TetmlIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`tetml_detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :param str access_rights: Path to ``access_rights.json`` file.
    :return: List of `TetmlIssueDir` instances, to be imported.
    """
    with open(access_rights, "r") as f:
        access_rights_dict = json.load(f)

    issues = select_issues(config, base_dir)

    return [dir2tetmldir(x, access_rights_dict) for x in issues]
