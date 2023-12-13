"""This module contains helper functions to find BL OCR data to import.
"""
import logging
import os
from collections import namedtuple
from datetime import date
from typing import List, Optional
import zipfile

from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter
from glob import glob
from text_importer.utils import get_access_right

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {
    1: 'a',
    2: 'b',
    3: 'c',
    4: 'd',
    5: 'e'
    }

BlIssueDir = namedtuple(
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
>>> i = BlIssueDir(
    journal='0002088', 
    date=datetime.date(1832, 11, 23), 
    edition='a', 
    path='./BL/BLIP_20190920_01.zip', 
    rights='open_public'
)
"""

BL_ACCESS_RIGHTS = "closed"


def _get_single_subdir(_dir: str) -> Optional[str]:
    """Check if the given dir only has one directory and return its basename.

    Args:
        _dir (str): Directory to check.

    Returns:
        Optional[str]: Subdirectory's basename if it's unique, None otherwise.
    """
    sub_dirs = [x for x in os.listdir(_dir) if os.path.isdir(os.path.join(_dir, x))]
    
    if len(sub_dirs) == 0:
        logger.warning(f"Could not find issue in BLIP: {_dir}")
        return None
    elif len(sub_dirs) > 1:
        logger.warning(f"Found more than one issue in BLIP: {_dir}")
        return None
    return sub_dirs[0]


def _get_journal_name(issue_path: str, blip_id: str) -> Optional[str]:
    """Find the Journal name from within the Mets file.

    For BL, the journal name is not present in the directory structure.
    The BLIP Id is needed to fetch the right section. The BLIP ID is usually
    the top-level directory where the issue is located.

    Args:
        issue_path (str): Path to issue directory
        blip_id (str): BLIP ID of the issue.

    Returns:
        Optional[str]: The name of the journal, or None if not found.
    """
    mets_file = [
        os.path.join(issue_path, f)
        for f in os.listdir(issue_path)
        if 'mets.xml' in f.lower()
    ]
    if len(mets_file) == 0:
        logger.critical(f"Could not find METS file in {issue_path}")
        return None
    
    mets_file = mets_file[0]
    
    with open(mets_file, 'r', encoding="utf-8") as f:
        raw_xml = f.read()
    
    mets_doc = BeautifulSoup(raw_xml, 'xml')
    
    dmd_sec = [
        x for x in mets_doc.findAll('dmdSec') 
        if x.get('ID') and blip_id in x.get('ID')
    ]
    if len(dmd_sec) != 1:
        logger.critical(f"Could not get journal name for {issue_path}")
        return None
    
    contents = dmd_sec[0].find('title').contents
    if len(contents) != 1:
        logger.critical(f"Could not get journal name for {issue_path}")
        return None
    
    title = contents[0]
    acronym = [x[0] for x in title.split(" ")]

    return "".join(acronym)


def _extract_all(archive_dir: str, destination: str) -> None:
    """Extract all zip files in `archive_dir` into `destination`.

    Args:
        archive_dir (str): Directory containing all archives to extract.
        destination (str): Destination directory.
    """
    
    archive_files = glob(os.path.join(archive_dir, "*.zip"))
    logger.info(f"Found {len(archive_files)} files to extract")
    
    for archive in archive_files:
        with zipfile.ZipFile(archive, 'r') as zip_ref:
            zip_ref.extractall(destination)


def dir2issue(path: str) -> Optional[BlIssueDir]: 
    """Given the BLIP directory of an issue, create the `BlIssueDir` object.
    
    TODO: update handling of rights and edition with full data.

    Args:
        path (str): The BLIP directory path

    Returns:
        Optional[BlIssueDir]: The corresponding Issue
    """
    split = path.split('/')
    journal, year, month_day = split[-3], int(split[-2]), split[-1]
    month, day = int(month_day[:2]), int(month_day[2:])
    
    return BlIssueDir(journal=journal, date=date(year, month, day), 
                      edition='a', path=path, rights=BL_ACCESS_RIGHTS)


def detect_issues(
    base_dir: str, access_rights: str, tmp_dir: str
) -> list[BlIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that the BL used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data, 
            this directory should contain `zip` files.
        access_rights (str): Not used for this imported, but argument is 
            kept for normality.
        tmp_dir (str): Temporary directory to unzip archives.

    Returns:
        list[BlIssueDir]: List of `BlIssueDir` instances to import.
    """
    # Extract all zips to tmp_dir
    _extract_all(base_dir, tmp_dir)
    
    # base_dir becomes extracted archives dir
    base_dir = tmp_dir
    
    # Get all BLIP dirs (named with NLP ID)
    blip_dirs = [
        x for x in os.listdir(base_dir) 
        if os.path.isdir(os.path.join(base_dir, x))
    ]
    issues = []
    
    for blip in blip_dirs:
        blip_path = os.path.join(base_dir, blip)
        dir_path, journal_dirs, files = next(os.walk(blip_path))
        
        # First iterate on all journals in BLIP dir
        for journal in journal_dirs:
            journal_path = os.path.join(blip_path, journal)
            dir_path, year_dirs, files = next(os.walk(journal_path))
            
            # Then on years
            for year in year_dirs:
                year_path = os.path.join(journal_path, year)
                dir_path, month_day_dirs, files = next(os.walk(year_path))
                # Then on each issue
                for month_day in month_day_dirs:
                    path = os.path.join(year_path, month_day)
                    issues.append(dir2issue(path))
    
    return issues


def select_issues(
    base_dir: str, config: dict, access_rights: str, tmp_dir: str
) -> Optional[list[BlIssueDir]]:
    """SDetect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.
        access_rights (str): Path to ``access_rights.json`` file.
        tmp_dir (str): Temporary directory to unzip archives.

    Returns:
        Optional[list[BlIssueDir]]: List of `BlIssueDir` instances to import.
    """
    
    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config["newspapers"]
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] "
                        "is missing in the config file.")
        return
    
    issues = detect_issues(base_dir, access_rights, tmp_dir)
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
