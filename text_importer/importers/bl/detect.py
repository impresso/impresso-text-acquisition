import json
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

import codecs
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
BL_ACCESS_RIGHTS = "closed"


def _get_single_subdir(_dir: str) -> Optional[str]:
    """Checks if the given dir only has one directory and returns its basename, returns None otherwise.
    
    :param str _dir: Directory to check
    :return: str The basename of the single directory
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
    """ Finds the Journal name from within the mets file, since for BL, it is not in the directory structure.
    The BLIP Id is needed to fetch the right section.
    
    :param str issue_path: Path to issue directory
    :param str blip_id: BLIP ID of the issue (usually the top-level directory where it is located)
    :return: str : The name of the journal, or None if not found
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
    
    with codecs.open(mets_file, 'r', "utf-8") as f:
        raw_xml = f.read()
    
    mets_doc = BeautifulSoup(raw_xml, 'xml')
    
    dmd_sec = [x for x in mets_doc.findAll('dmdSec') if x.get('ID') and blip_id in x.get('ID')]
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


def _extract_all(archive_dir: str, destination: str):
    """
    Extracts all zip files in `archive_dir` into `destination`
    :param str archive_dir: Directory containing all archives to extract
    :param str destination: Destination directory
    :return:
    """
    
    archive_files = glob(os.path.join(archive_dir, "*.zip"))
    logger.info(f"Found {len(archive_files)} files to extract")
    
    for archive in archive_files:
        with zipfile.ZipFile(archive, 'r') as zip_ref:
            zip_ref.extractall(destination)


def dir2issue(path: str) -> Optional[BlIssueDir]:  # TODO: ask about rights and edition
    """
    Given the BLIP directory of an issue, this function returns the corresponding IssueDirectory.
    
    :param str blip_dir: The BLIP directory path
    :return: BlIssueDir The corresponding Issue
    """
    split = path.split('/')
    journal, year, month_day = split[-3], int(split[-2]), split[-1]
    month, day = int(month_day[:2]), int(month_day[2:])
    
    return BlIssueDir(journal=journal, date=date(year, month, day), edition='a', path=path,
                      rights=BL_ACCESS_RIGHTS)


def detect_issues(base_dir: str, access_rights: str, tmp_dir: str) -> List[BlIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that the BL used to
    organize the dump of Mets/Alto OCR data.

    :param str base_dir: Path to the base directory of newspaper data. (For BL this dir should contain `zip` files)
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :param str tmp_dir: Temporary directory to unzip archives
    :return: List of `BlIssueDir` instances, to be imported.
    """
    # Extract all zips to tmp_dir
    _extract_all(base_dir, tmp_dir)
    
    # base_dir becomes extracted archives dir
    base_dir = tmp_dir
    
    # Get all BLIP dirs (named with NLP ID)
    blip_dirs = [x for x in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, x))]
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


def select_issues(base_dir: str, config: dict, access_rights: str) -> Optional[List[BlIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :param str access_rights: Path to ``access_rights.json`` file.
    :return: List of `BlIssueDir` instances, to be imported.
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
