"""Objects and functions to detect Olive data in the RERO dump."""

import json
from collections import namedtuple
from typing import List

from impresso_commons.path.path_fs import (IssueDir, detect_issues,
                                           select_issues)

from text_importer.utils import get_access_right

OliveIssueDir = namedtuple(
        "IssueDirectory", [
                'journal',
                'date',
                'edition',
                'path',
                'rights'
                ]
        )
"""test"""


def dir2olivedir(issue_dir: IssueDir, access_rights: dict) -> OliveIssueDir:
    """Short summary.

    :param IssueDir issue_dir: Description of parameter `issue_dir`.
    :param dict access_rights: Description of parameter `access_rights`.
    :return: Description of returned object.
    :rtype: OliveIssueDir

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
        journal_filter: set = None,
        exclude: bool = False
        ) -> List[OliveIssueDir]:
    """Short summary.

    :param str base_dir: Description of parameter `base_dir`.
    :param str access_rights: Description of parameter `access_rights`.
    :param set journal_filter: Description of parameter `journal_filter`.
    :param bool exclude: Description of parameter `exclude`.
    :return: Description of returned object.
    :rtype: List[OliveIssueDir]

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
        config: dict,
        access_rights: str
        ) -> List[OliveIssueDir]:
    """Short summary.

    :param str base_dir: Description of parameter `base_dir`.
    :param dict config: Description of parameter `config`.
    :param dict access_rights: Description of parameter `access_rights_dict`.
    :return: Description of returned object.
    :rtype: List[OliveIssueDir]

    """
    with open(access_rights, 'r') as f:
        access_rights_dict = json.load(f)

    issues = select_issues(config, base_dir)

    return [dir2olivedir(x, access_rights_dict) for x in issues]
