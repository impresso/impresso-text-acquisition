import json
import logging
import os
from collections import namedtuple
from datetime import date
from typing import List

from dask import bag as db

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {
        1: 'a',
        2: 'b',
        3: 'c',
        4: 'd',
        5: 'e'
        }

LuxIssueDir = namedtuple(
        "IssueDirectory", [
                'journal',
                'date',
                'edition',
                'path',
                'rights'
                ]
        )


def dir2issue(path: str) -> LuxIssueDir:
    """Create a LuxIssueDir object from a directory."""
    issue_dir = os.path.basename(path)
    local_id = issue_dir.split('_')[2]
    issue_date = issue_dir.split('_')[3]
    year, month, day = issue_date.split('-')
    rights = 'open_public' if 'public_domain' in path else 'closed'
    
    if len(issue_dir.split('_')) == 4:
        edition = 'a'
    elif len(issue_dir.split('_')) == 5:
        edition = issue_dir.split('_')[4]
        edition = EDITIONS_MAPPINGS[int(edition)]
    
    return LuxIssueDir(
            local_id,
            date(int(year), int(month), int(day)),
            edition,
            path,
            rights
            )


def detect_issues(base_dir: str, access_rights: str = None) -> List[LuxIssueDir]:
    """Parse a directory structure and detect newspaper issues to be imported.

    :param access_rights:
    :param base_dir: the root of the directory structure
    :type base_dir: LuxIssueDir
    :return: list of `LuxIssueDir` instances
    :rtype: list
    """
    dir_path, dirs, files = next(os.walk(base_dir))
    batches_dirs = [os.path.join(dir_path, dir) for dir in dirs]
    issue_dirs = [
            os.path.join(batch_dir, dir)
            for batch_dir in batches_dirs
            for dir in os.listdir(batch_dir)
            if 'newspaper' in dir
            ]
    return [
            dir2issue(_dir)
            for _dir in issue_dirs
            ]


def select_issues(input_dir: str, config: dict, access_rights: str) -> List[LuxIssueDir]:
    """
    
    :param input_dir:
    :param config:
    :param access_rights:
    :return:
    """
    issues = detect_issues(input_dir)
    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag \
        .filter(lambda i: i.journal in config['newspapers'].keys()) \
        .compute()
    
    logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                    len(selected_issues),
                    selected_issues
                    )
            )
    return selected_issues
