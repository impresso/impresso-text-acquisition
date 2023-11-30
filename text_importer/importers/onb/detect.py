"""This module contains helper functions to find the ONB OCR data to import.
"""

import json
import logging
import os
from collections import namedtuple
from datetime import date
from pathlib import Path

from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter

from text_importer.utils import get_access_right

logger = logging.getLogger(__name__)

OnbIssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path',
        'rights',
        'pages'
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
>>> i = OnbIssueDir('nwb', date(1874,01,06), 'a', './ANNO/nwb/1874/01/06', 'open')
>>> i = OnbIssueDir(
        journal='nwb', 
        date=date(1874,01,06), 
        edition='a', path='./ANNO/nwb/1874/01/06',
        rights='open_public', 
        pages=[(
            'nwb-1874-01-06-a-p0001', 
            '00000001.xml'
        ), ...]
    )
"""

def dir2issue(path: str, access_rights: dict) -> OnbIssueDir:
    """Create a `OnbIssueDir` from a directory (ONB format).

    Args:
        path (str): Path of issue.
        access_rights (dict): dictionary for access rights.

    Returns:
        OnbIssueDir: New `OnbIssueDir` object matching the path and rights.
    """
    split_path = path.split('/')
    journal = split_path[-4]
    issue_date = date.fromisoformat('-'.join(split_path[-3:]))
    edition = 'a'
    issue_id = '-'.join(split_path[-4:] + [edition])

    pages = []
    for file in os.listdir(path):
        if '.xml' in file:
            p_number = int(Path(file).stem)
            p_id = f"{issue_id}-p{str(p_number).zfill(4)}"
            pages.append((p_id, os.path.basename(file)))

    return OnbIssueDir(journal=journal, 
                       date=issue_date,
                       edition=edition, 
                       path=path,
                       rights=access_rights,
                       pages = pages)


def detect_issues(base_dir: str, access_rights: str) -> list[OnbIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that ONB used to
    organize the dump of Alto OCR data.
    
    The access rights information is not in place yet, but needs
    to be specified by the content provider (ONB).

    TODO: Add the directory structure of ONB OCR data dumps.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        access_rights (str): Path to ``access_rights.json`` file.

    Returns:
        list[SwaIssueDir]: list of ``OnbIssueDir`` instances, to be imported.
    """
    # For now, only ANNO titles are in mets/alto format
    anno_path = os.path.join(base_dir, 'ANNO')

    dir_path, dirs, files = next(os.walk(anno_path))
    journal_dirs = [os.path.join(dir_path, j_dir) for j_dir in dirs]
    # iteratively 
    issues_dirs = [
        os.path.join(journal, year, month, day)
        for journal in journal_dirs
        for year in os.listdir(journal)
        for month in os.listdir(os.path.join(journal, year))
        for day in os.listdir(os.path.join(journal, year, month))
    ]
    
    #with open(access_rights, 'r') as f:
    #    ar_dict = json.load(f)
    ar_dict = {}

    return [dir2issue(_dir, ar_dict) for _dir in issues_dirs]