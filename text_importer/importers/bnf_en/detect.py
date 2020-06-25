import json
import logging
import os
from collections import namedtuple
from datetime import datetime
from typing import List, Optional

from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter
import requests
from text_importer.utils import get_access_right
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {
    1: 'a',
    2: 'b',
    3: 'c',
    4: 'd',
    5: 'e'
    }

BnfEnIssueDir = namedtuple(
        "IssueDirectory", [
            'journal',
            'date',
            'edition',
            'path',
            'rights',
            'ark_link'
            ]
        )
"""A light-weight data structure to represent a newspaper issue in BNF Europeana

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
>>> i = BnfEnIssueDir('BLB', date(1845,12,28), 'a', './Le-Gaulois/18820208_1', 'open')
"""

EXCEL_FILE = "liste-arks.xls"
sheet_translation = {
    'Ouest-Eclair': 'ouesteclair',
    'JDPL': 'jdpl',
    'Matin': 'lematin',
    'Petit Parisien': 'lepetitparisien',
    'PJI': 'lepji',
    'Gaulois': 'legaulois'
    }

OECAEN_API = 'http://gallica.bnf.fr/ark:/12148/cb41193642z/date'


def get_id(journal: str, date: datetime.date, edition: str):
    return "{}-{}-{:02}-{:02}-{}".format(journal, date.year, date.month, date.day, edition)


def parse_dir(_dir: str, journal: str) -> str:
    """ Parses a directory and returns the corresponding ID (used in the context of constructing ark links)
    
    :param str _dir: The directory (in Windows FS)
    :param str journal:  Journal name to construct ID
    :return: issue id
    """
    date_edition = _dir.split('\\')[-1].split('_')
    if len(date_edition) == 1:
        edition = 'a'
        date = date_edition[0]
    else:
        date = date_edition[0]
        edition = EDITIONS_MAPPINGS[int(date_edition[1])]
    year, month, day = date[:4], date[4:6], date[6:8]
    return "{}-{}-{}-{}-{}".format(journal, year, month, day, edition)


def construct_ark_links(excel_file: str) -> dict:
    """ Given the path of the excel file, creates a dict[str, str] mapping id to ark_link
    
    :param str excel_file:
    :return: dict issue_id -> ark_link
    """
    xls = pd.ExcelFile(excel_file)
    dfs = []
    for sheet in xls.sheet_names:
        s = pd.read_excel(xls, sheet)
        journal = sheet_translation[sheet]
        s['id'] = s['CHEMIN_COMPLET'].apply(lambda x: parse_dir(x, journal))
        dfs.append(s)
    df = pd.concat(dfs)
    
    return dict(df[['id', 'ARK_DOCNUM']].values)


def get_oe_caen_ark_link(date: datetime.date) -> str:
    r = requests.get(OECAEN_API + "{}{:02}{:02}".format(date.year, date.month, date.day))
    obj = BeautifulSoup(r.text, features='lxml')
    return obj.findAll("meta", {'property': 'og:url'})[0].get('content')


def get_ark_link(ark_links, journal, date, edition):
    _id = get_id(journal, date, edition)
    if _id in ark_links:
        return ark_links[_id]
    else:
        assert journal == "oecaen" or journal == "oerennes"
        link = get_oe_caen_ark_link(date)
        return "/".join(link.split('/')[3:])


def dir2issue(path: str, access_rights: dict, ark_links: dict) -> BnfEnIssueDir:
    """Create a `BnfEnIssueDir` object from a directory path.

    .. note ::
        This function is called internally by :func:`detect_issues`

    :param str path: Path of issue.
    :return: New ``BnfEnIssueDir`` object
    """
    journal, issue = path.split('/')[-2:]
    
    date, edition = issue.split('_')[:2]
    date = datetime.strptime(date, '%Y%m%d').date()
    journal = journal.lower().replace('-', '').strip()
    edition = EDITIONS_MAPPINGS[int(edition)]
    
    #get_ark_link(ark_links, journal, date, edition)
    return BnfEnIssueDir(journal=journal, date=date, edition=edition, path=path,
                         rights="open-public", ark_link=None)


def detect_issues(base_dir: str, access_rights: str) -> List[BnfEnIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNF-EN used to
    organize the dump of Mets/Alto OCR data.

    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfEnIssueDir` instances, to be imported.
    """
    
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    issue_dirs = [
        os.path.join(journal, _dir)
        for journal in journal_dirs
        for _dir in os.listdir(journal)
        ]
    # Open ark links document and parse it
    #ark_links = construct_ark_links(os.path.join(base_dir, EXCEL_FILE))
    return [dir2issue(_dir, None, None) for _dir in issue_dirs]


def select_issues(base_dir: str, config: dict, access_rights: str) -> Optional[List[BnfEnIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfEnIssueDir` instances, to be imported.
    """
    
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
