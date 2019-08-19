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
>>> i = LuxIssueDir('armeteufel', date(1904,1,17), 'a', './protected_027/1497608_newspaper_armeteufel_1904-01-17/', 'protected')
"""


def dir2issue(path: str) -> LuxIssueDir:
    """Create a `LuxIssueDir` object from a directory path.
    
    .. note ::
        This function is called internally by :func:`detect_issues`
        
    :param path: Path of issue.
    :return: New ``LuxIssueDir`` object
    """
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
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNL used to
    organize the dump of Mets/Alto OCR data.

    :param base_dir: Path to the base directory of newspaper data.
    :param access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `LuxIssueDir` instances, to be imported.
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


def select_issues(base_dir: str, config: dict, access_rights: str) -> List[LuxIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.
    
    :param base_dir: Path to the base directory of newspaper data.
    :param config: Config dictionary for filtering
    :param access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `LuxIssueDir` instances, to be imported.
    """
    issues = detect_issues(base_dir)
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
