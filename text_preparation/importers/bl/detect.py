"""This module contains helper functions to find BL OCR data to import."""

import logging
import os
from collections import namedtuple
from datetime import date
import zipfile

from glob import glob
from dask import bag as db
from text_preparation.importers.detect import _apply_datefilter

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}
# TODO remove since it's been copied to original/BL
BL_SAMPLE_DIR = "../text_preparation/data/sample_data/BL/"
BL_OCR_FILE = "BL_ocr_formats.json"
# add here the file with the mapping from issue to working and alternative titles

BlIssueDir = namedtuple("IssueDirectory", ["provider", "alias", "date", "edition", "path", "nlp"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "BL"
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    nlp (str): BL internal NLP for this issue (eg. '0002088')

>>> from datetime import date
>>> i = BlIssueDir(
    provider='BL',
    alias='LSGA', 
    date=datetime.date(1832, 11, 23), 
    edition='a', 
    path='./BL/LSGA/0002088/1832/11/23', 
    nlp='0002088'
)
"""


def _get_single_subdir(_dir: str) -> str | None:
    """Check if the given dir only has one directory and return its basename.

    Args:
        _dir (str): Directory to check.

    Returns:
        str | None: Subdirectory's basename if it's unique, None otherwise.
    """
    sub_dirs = [x for x in os.listdir(_dir) if os.path.isdir(os.path.join(_dir, x))]

    if len(sub_dirs) == 0:
        logger.warning("Could not find issue in BLIP: %s", _dir)
        return None
    if len(sub_dirs) > 1:
        logger.warning("Found more than one issue in BLIP: %s", _dir)
        return None
    return sub_dirs[0]


def _get_journal_name(issue_path: str, blip_id: str) -> str | None:
    """Find the Journal name from within the Mets file.

    For BL, the journal name is not present in the directory structure.
    The BLIP Id is needed to fetch the right section. The BLIP ID is usually
    the top-level directory where the issue is located.

    Args:
        issue_path (str): Path to issue directory
        blip_id (str): BLIP ID of the issue.

    Returns:
        str | None: The name of the journal, or None if not found.
    """
    mets_file = [
        os.path.join(issue_path, f) for f in os.listdir(issue_path) if "mets.xml" in f.lower()
    ]
    if len(mets_file) == 0:
        logger.critical("Could not find METS file in %s", issue_path)
        return None

    mets_file = mets_file[0]

    with open(mets_file, "r", encoding="utf-8") as f:
        raw_xml = f.read()

    mets_doc = BeautifulSoup(raw_xml, "xml")

    dmd_sec = [x for x in mets_doc.findAll("dmdSec") if x.get("ID") and blip_id in x.get("ID")]
    if len(dmd_sec) != 1:
        logger.critical("Could not get journal name for %s", issue_path)
        return None

    contents = dmd_sec[0].find("title").contents
    if len(contents) != 1:
        logger.critical("Could not get journal name for %s", issue_path)
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
    logger.info("Found %s files to extract", len(archive_files))

    for archive in archive_files:
        with zipfile.ZipFile(archive, "r") as zip_ref:
            zip_ref.extractall(destination)


def dir2issue(path: str) -> BlIssueDir | None:
    """Given the directory of an issue, create the `BlIssueDir` object.

    Args:
        path (str): The issue directory path

    Returns:
        Optional[BlIssueDir]: The corresponding Issue
    """
    split = path.split("/")
    alias, nlp, year, month, day = (
        split[-5],
        split[-4],
        int(split[-3]),
        int(split[-2]),
        int(split[-1]),
    )

    return BlIssueDir(
        provider="BL", alias=alias, date=date(year, month, day), edition="a", path=path, nlp=nlp
    )


def detect_issues(
    base_dir: str, bl_ocr_formats: str | None = BL_OCR_FILE, format: str = "OmniPage-NLP"
) -> list[BlIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that the BL used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to the BL aliases.
        tmp_dir (str): Temporary directory to unzip archives.

    Returns:
        list[BlIssueDir]: List of `BlIssueDir` instances to import.
    """
    # Extract all zips to tmp_dir
    # TODO choose the BL format file corresponding to the correct format
    # _extract_all(base_dir, tmp_dir)

    # get the list of issues from the processed OCR formats file.
    # This will substantially reduce the initial processing time for detecting issues.
    # base_dir becomes extracted archives dir

    # Get all BLIP dirs (named with NLP ID)
    blip_dirs = [x for x in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, x))]
    issues = []

    for blip in blip_dirs:
        blip_path = os.path.join(base_dir, blip)
        dir_path, journal_dirs, files = next(os.walk(blip_path))

        # First iterate on all journals in BLIP dir
        for journal in journal_dirs:
            journal_path = os.path.join(blip_path, journal)
            _, year_dirs, _ = next(os.walk(journal_path))

            # Then on years
            for year in year_dirs:
                year_path = os.path.join(journal_path, year)
                _, month_day_dirs, _ = next(os.walk(year_path))
                # Then on each issue
                for month_day in month_day_dirs:
                    path = os.path.join(year_path, month_day)
                    issues.append(dir2issue(path))

    return issues


def select_issues(base_dir: str, config: dict, tmp_dir: str) -> list[BlIssueDir] | None:
    """SDetect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.
    TODO add NLP

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.
        tmp_dir (str): Temporary directory to unzip archives.

    Returns:
        list[BlIssueDir] | None: List of `BlIssueDir` instances to import.
    """

    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config["titles"]
        exclude_list = config["exclude_titles"]
        year_flag = config["year_only"]

    except KeyError:
        logger.critical(
            "The key [titles|exclude_titles|year_only] " "is missing in the config file."
        )
        return None

    issues = detect_issues(base_dir, tmp_dir)
    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag.filter(
        lambda i: (len(filter_dict) == 0 or i.alias in filter_dict.keys())
        and i.alias not in exclude_list
    ).compute()

    exclude_flag = False if not exclude_list else True
    filtered_issues = (
        _apply_datefilter(filter_dict, selected_issues, year_only=year_flag)
        if not exclude_flag
        else selected_issues
    )
    logger.info(
        "%s newspaper issues remained after applying filter: %s",
        len(filtered_issues),
        filtered_issues,
    )

    return filtered_issues
