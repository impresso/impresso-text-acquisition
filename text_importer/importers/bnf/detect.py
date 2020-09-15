import json
import logging
import os
import string
from collections import namedtuple
from typing import List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter
from text_importer.importers.bnf.helpers import get_journal_name, parse_date
from text_importer.importers.mets_alto.mets import get_dmd_sec
from text_importer.utils import get_access_right

logger = logging.getLogger(__name__)

BnfIssueDir = namedtuple(
        "IssueDirectory", [
            'journal',
            'date',
            'edition',
            'path',
            'rights',
            'secondary_date'
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
:param str path: Path to the archive containing OCR and OLR data
:param str rights: Access rights on the data (open, closed, etc.)

>>> from datetime import date
>>> i = BnfIssueDir(journal='Marie-Claire', date=datetime.date(1938, 3, 11), edition='a', path='./BNF/files/4701034.zip', rights='open_public')
"""

DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d"]
DATE_SEPARATORS = ["/", "-"]
"""
In BNF data, dates can be given in two different formats (separated with `-` or `/`). Also, an issue can have two dates,
separated by either `-` or `/`
"""


def get_id(issue):
    return "{}-{}-{:02}-{:02}-{}".format(issue.journal, issue.date.year, issue.date.month,
                                         issue.date.day, issue.edition)


def get_number(issue):
    return issue.path.split('/')[-1]


def assign_editions(issues):
    issues = sorted(issues, key=lambda x: x[1])
    new_issues = []
    for index, (i, n) in enumerate(issues):
        i = BnfIssueDir(journal=i.journal,
                        date=i.date,
                        edition=string.ascii_lowercase[index],
                        path=i.path,
                        rights=i.rights,
                        secondary_date=i.secondary_date)
        new_issues.append((i, n))
    return new_issues


def dir2issue(issue_path: str, access_rights_dict: dict) -> BnfIssueDir:
    """ Creates a `BnfIssueDir` object from an archive path
    
    .. note ::
        This function is called internally by `detect_issues`
    
    :param str issue_path: The path of the issue within the archive
    :param dict access_rights_dict:
    :return: New ``BnfIssueDir`` object
    """
    manifest_file = os.path.join(issue_path, "manifest.xml")
    
    issue = None
    if os.path.isfile(manifest_file):
        with open(manifest_file) as f:
            manifest = BeautifulSoup(f, "xml")
        
        try:
            issue_info = get_dmd_sec(manifest, 2)  # Issue info is in dmdSec of id 2
            journal = get_journal_name(issue_path)
            np_date, secondary_date = parse_date(issue_info.find("date").contents[0], DATE_FORMATS, DATE_SEPARATORS)
            edition = "a"
            rights = get_access_right(journal, np_date, access_rights_dict)
            issue = BnfIssueDir(journal=journal, date=np_date, edition=edition, path=issue_path, rights=rights,
                                secondary_date=secondary_date)
        except ValueError as e:
            logger.info(e)
            logger.error(f"Could not parse issue at {issue_path}")
    else:
        logger.error(f"Could not find manifest in {issue_path}")
    return issue


def detect_issues(base_dir: str, access_rights: str = None):
    """ Detects BNF issues to import within the filesystem
    
    This function the directory structure used by BNF (one subdir by journal)
    
    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfIssueDir` instances, to be imported.
    """
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs if not _dir.startswith("2")]
    issue_dirs = [
        os.path.join(journal, _dir)
        for journal in journal_dirs
        for _dir in os.listdir(journal)
        ]
    
    with open(access_rights, 'r') as f:
        access_rights_dict = json.load(f)
    issue_dirs = [dir2issue(_dir, access_rights_dict) for _dir in issue_dirs]
    initial_length = len(issue_dirs)
    issue_dirs = [i for i in issue_dirs if i is not None]
    
    df = pd.DataFrame([{"issue": i, "id": get_id(i), "number": get_number(i)} for i in issue_dirs])
    vals = df.groupby('id').apply(lambda x: x[['issue', 'number']].values.tolist()).values
    
    issue_dirs = [i if len(i) == 1 else assign_editions(i) for i in vals]
    issue_dirs = [j[0] for i in issue_dirs for j in i]
    
    logger.info(f"Removed {initial_length - len(issue_dirs)} problematic issues")
    return [i for i in issue_dirs if i is not None]


def select_issues(base_dir: str, config: dict, access_rights: str) -> Optional[List[BnfIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfIssueDir` instances, to be imported.
    """
    
    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config["newspapers"]
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] is missing in the config file.")
        return
    
    issues = detect_issues(base_dir, access_rights)
    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag \
        .filter(lambda i: (len(filter_dict) == 0 or i.journal in filter_dict.keys()) and i.journal not in exclude_list) \
        .compute()
    
    exclude_flag = False if not exclude_list else True
    filtered_issues = _apply_datefilter(filter_dict, selected_issues,
                                        year_only=year_flag) if not exclude_flag else selected_issues
    logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                    len(filtered_issues),
                    filtered_issues
                    )
            )
    return filtered_issues
