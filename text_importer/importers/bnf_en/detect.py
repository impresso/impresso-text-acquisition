"""This module contains helper functions to find BNF-EN OCR data to import.
"""
import logging
import os
from collections import namedtuple
from datetime import datetime, timedelta
from string import ascii_lowercase
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

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
    ark_link (str): Unique IIIF identifier associated with this issue.

>>> from datetime import date
>>> i = BnfEnIssueDir('BLB', date(1845,12,28), 'a', './Le-Gaulois/18820208_1', 'open')
"""

API_JOURNAL_URL = "https://gallica.bnf.fr/services/Issues?ark={ark}/date"
API_ISSUE_URL = "https://gallica.bnf.fr/services/Issues?ark={ark}/date&date={year}"
IIIF_URL = "https://gallica.bnf.fr/iiif/ark:/12148/{issue_id}"

API_MAPPING = {
    "oerennes": "cb32830550k",
    "oecaen": "cb41193642z",
    "lematin": "cb328123058",
    "lepji": "cb32836564q",
    "jdpl": "cb39294634r",
    "legaulois": "cb32779904b",
    "lepetitparisien": "cb34419111x",
}


def get_api_id(
    journal: str, api_issue: tuple[str, datetime.date], edition: str
) -> str:
    """Construct an ID given a journal name, date and edition.

    Args:
        journal (str): Journal name
        api_issue (tuple[str, datetime.date]): Tuple of information fetched 
            from the Gallica API.
        edition (str): Edition of the issue.

    Returns:
        str: Canonical issue Id composed of journal name, date and edition.
    """
    date = api_issue[1]
    return "{}-{}-{:02}-{:02}-{}".format(journal, date.year, date.month, 
                                         date.day, ascii_lowercase[edition])


def get_issues_iiif_arks(journal_ark: tuple[str, str]) -> list[tuple[str, str]]:
    """Given a journal name and Ark, fetch its issues' Ark in the Gallica API.

    Each fo the Europeana journals have a journal-level Ark id, as well as 
    issue-level IIIF Ark ids that can be fetched from the Gallica API using 
    the journal Ark. 
    The API also provides the day of the year for the corresponding issue.
    Using both information, this function recreates all the issue canonical
    for each collection and maps them to their respective issue IIIF Ark ids.

    Args:
        journal_ark (tuple[str, str]): Pair of journal and associated Ark id.

    Returns:
        list[tuple[str, str]]: Pairs of issue canonical Ids and IIIF Ark Ids.
    """
    journal, ark = journal_ark
    
    def get_date(dayofyear: str, year: int) -> datetime.date:
        """Return the date corresponding to a day of year.

        Args:
            dayofyear (str): Numbered day in a year.
            year (int): Year in question.

        Returns:
            datetime.date: Date corresponding to the day of year.
        """
        start_date = datetime(year=year, month=1, day=1)
        return start_date + timedelta(days=int(dayofyear) - 1)
    
    print("Fetching for {}".format(journal))
    r = requests.get(API_JOURNAL_URL.format(ark=ark))
    years = BeautifulSoup(r.content, "lxml").findAll("year")
    years = [int(x.contents[0]) for x in years]
    
    links = []
    for year in tqdm(years):
        # API requrest
        url = API_ISSUE_URL.format(ark=API_MAPPING[journal], year=year)
        r = requests.get(url)
        api_issues = BeautifulSoup(r.content, "lxml").findAll("issue")
        # Parse dates and editions
        api_issues = [
            (i.get("ark"), get_date(i.get("dayofyear"), year)) 
            for i in api_issues
        ]
        
        editions = []
        for i, issue in enumerate(api_issues):
            if i == 0:
                editions.append(0)
            else:
                previous_same = api_issues[i - 1][1] == issue[1]
                if previous_same:
                    editions.append(editions[-1] + 1)
                else:
                    editions.append(0)
        
        api_issues = [
            (get_api_id(journal, i, edition), i[0]) 
            for i, edition in zip(api_issues, editions)
        ]
        links += api_issues
    return links


def construct_iiif_arks() -> dict[str, str]:
    """Fetch the IIIF ark ids for each issue and map them to each other.

    Returns:
        dict[str, str]: Mapping from issue canonical id to IIIF Ark id.
    """
    with Pool(4) as p:
        results = p.map(get_issues_iiif_arks, list(API_MAPPING.items()))
    
    iiif_arks = []
    for i in results:
        iiif_arks += i
    return dict(iiif_arks)


def get_id(journal: str, date: datetime.date, edition: str) -> str:
    """Construct the canonical issue ID given the necessary information.

    Args:
        journal (str): Journal name.
        date (datetime.date): Publication date.
        edition (str): Edition of the issue.

    Returns:
        str: Resulting issue canonical Id.
    """
    return "{}-{}-{:02}-{:02}-{}".format(journal, date.year, date.month, 
                                         date.day, edition)


def parse_dir(_dir: str, journal: str) -> str:
    """Parse a directory and return the corresponding ID.

    Args:
        _dir (str): The directory (in Windows FS).
        journal (str): Journal name to construct ID.

    Returns:
        str: Issue canonical id.
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


def dir2issue(
    path: str, access_rights: dict, iiif_arks: dict[str, str]
) -> Optional[BnfEnIssueDir]:
    """Create a `BnfEnIssueDir` object from a directory path.

    Note:
        This function is called internally by :func:`detect_issues`.

    Args:
        path (str): Path of issue.
        access_rights (dict): Access rights (for conformity).
        iiif_arks (dict): Mapping from issue canonical ids to iiif ark ids. 

    Returns:
        Optional[BnfEnIssueDir]: `BnfEnIssueDir` for given issue if the ark id 
            was found on the Gallica API, None otherwise.
    """
    journal, issue = path.split('/')[-2:]
    
    date, edition = issue.split('_')[:2]
    date = datetime.strptime(date, '%Y%m%d').date()
    journal = journal.lower().replace('-', '').strip()
    edition = EDITIONS_MAPPINGS[int(edition)]
    
    id_ = get_id(journal, date, edition)
    
    if id_ not in iiif_arks:
        return None
    
    return BnfEnIssueDir(journal=journal, date=date, edition=edition, path=path,
                         rights="open-public", ark_link=iiif_arks[id_])


def detect_issues(base_dir: str, access_rights: str) -> List[BnfEnIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNF-EN used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        access_rights (str): Not used for this importer (kept for conformity).

    Returns:
        List[BnfEnIssueDir]: List of `BnfEnIssueDir` instances to import.
    """
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    issue_dirs = [
        os.path.join(journal, _dir)
        for journal in journal_dirs
        for _dir in os.listdir(journal)
    ]
    
    iiif_arks = construct_iiif_arks()
    issue_dirs = [dir2issue(_dir, None, iiif_arks) for _dir in issue_dirs]
    
    initial_length = len(issue_dirs)
    issue_dirs = [i for i in issue_dirs if i is not None]
    logger.info(f"Removed {initial_length-len(issue_dirs)} problematic issues")

    return issue_dirs


def select_issues(
    base_dir: str, config: dict, access_rights: str
) -> Optional[List[BnfEnIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.
        access_rights (str): Not used for this importer (kept for conformity).

    Returns:
        Optional[List[BnfEnIssueDir]]: `BnfEnIssueDir` instances to import.
    """
    try:
        filter_dict = config["newspapers"]
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] "
                        "is missing in the config file.")
        return
    
    issues = detect_issues(base_dir, access_rights)
    issue_bag = db.from_sequence(issues)
    selected_issues = (
        issue_bag.filter(
            lambda i: (
                len(filter_dict) == 0 or i.journal in filter_dict.keys()
            ) and i.journal not in exclude_list
        ).compute()
    )
    
    exclude_flag = False if not exclude_list else True
    filtered_issues = _apply_datefilter(
        filter_dict, selected_issues, year_only=year_flag
    ) if not exclude_flag else selected_issues
    logger.info(f"{len(filtered_issues)} newspaper issues remained "
                f"after applying filter: {filtered_issues}")
    
    return filtered_issues
