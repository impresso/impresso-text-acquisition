"""This module contains helper functions to find BCUL OCR data to import."""

import logging
import os
import json
import string
from collections import namedtuple

from dask import bag as db

from text_preparation.importers.detect import _apply_datefilter
from text_preparation.importers.bcul.helpers import parse_date, find_mit_file

logger = logging.getLogger(__name__)

BculIssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path", "mit_file_type"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    mit_file_type (str): Type of mit file for this issue (json or xml).

>>> from datetime import date
>>> i = BculIssueDir(
    alias='FAL', 
    date=datetime.date(1762, 12, 07), 
    edition='a', 
    path='./BCUL/46165', 
    mit_file_type:'json'
)
"""

# issues that lead to HTTP response 404. Skipping them altogether.
# These issues are often dublicates of issues for which the API works
# In addition, it was found that some issues were listed with wrong dates.
ALIASES_FILEPATH = "../data/sample_data/BCUL/access_rights_and_aliases.json"
FAULTY_ISSUES = [
    "127626",
    "127627",
    "127628",
    "127629",
    "127630",
    "127631",
    "127625",
    "287371",
    "287365",
    "287373",
    "287545",
    "287530",
    "287477",
]
CORRECT_ISSUE_DATES = {
    "170463": "08",
    "170468": "09",
    "170466": "11",
}


def dir2issue(path: str, journal_info: dict[str, str]) -> BculIssueDir | None:
    """Create a `BculIssueDir` object from a directory.

    Note:
        This function is called internally by `detect_issues`

    Args:
        path (str): The path of the issue.
        access_rights (dict): Dictionary for access rights.

    Returns:
        BculIssueDir | None: New `BculIssueDir` object.
    """
    mit_file = find_mit_file(path)
    if mit_file is None:
        logger.error("Could not find MIT file in %s", path)
        return None

    if not mit_file.endswith(journal_info["file_type"]):
        logger.warning(
            "Found mit file %s does not correspond to mit file type %s",
            os.path.join(path, mit_file),
            journal_info["file_type"],
        )
        # override the mit file type if the extension of the file found does not match
        journal_info["file_type"] = mit_file.split(".")[-1]

    date = parse_date(mit_file)

    # check if multiple issues are at this date:
    day_dir = os.path.dirname(path)
    day_editions = list(os.listdir(day_dir))
    day_editions = [
        str(i)
        for i in os.listdir(day_dir)
        if i not in FAULTY_ISSUES and i not in CORRECT_ISSUE_DATES and i != ".DS_Store"
    ]

    if len(day_editions) > 1 and os.path.basename(path) not in CORRECT_ISSUE_DATES:
        # if multiple issues exist for a given day, find the correct edition
        logger.info("Multiple issues for %s, finding the edition", day_dir)
        # exclude incorrect issues from the list
        index = sorted(day_editions).index(os.path.basename(path))
        edition = string.ascii_lowercase[index]
    else:
        edition = "a"

    return BculIssueDir(
        alias=journal_info["alias"],
        date=date,
        edition=edition,
        path=path,
        mit_file_type=journal_info["file_type"],
    )


def detect_issues(base_dir: str) -> list[BculIssueDir]:
    """Detect BCUL newspaper issues to import within the filesystem.

    This function expects the directory structure that BCUL used to
    organize the dump of Abbyy files.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[BculIssueDir]: List of `BCULIssueDir` instances, to be imported.
    """
    # open and read bcul_alias.json file
    with open(ALIASES_FILEPATH, "rb") as f:
        alias_mapping = json.load(f)

    dir_path, dirs, files = next(os.walk(base_dir))

    journal_dirs = [
        os.path.join(dir_path, _dir)
        for _dir in dirs
        if _dir not in ["OLD", "wrong_BCUL", ".DS_Store"] and _dir in alias_mapping
    ]

    if "La_Veveysanne__La_Patrie" in os.listdir(dir_path):
        # for the case of 'La_Veveysanne__La_Patrie' add them also
        vvs_pat_base_dir = os.path.join(dir_path, "La_Veveysanne__La_Patrie")
        vvs_pat_dirs = [
            os.path.join(vvs_pat_base_dir, _dir)
            for _dir in os.listdir(vvs_pat_base_dir)
            if ".DS_Store" not in _dir and _dir in alias_mapping
        ]
        journal_dirs.extend(vvs_pat_dirs)

    issue_dirs = []
    for journal in journal_dirs:
        logger.info("Detecting issues for %s.", journal)
        for dir_path, dirs, files in os.walk(journal):
            title = journal.split("/")[-1]
            # check if we are in the directory of a (valid) issue
            if (
                len(files) > 1
                and "solr" not in dir_path
                and os.path.basename(dir_path) not in FAULTY_ISSUES
            ):
                issue_dirs.append(dir2issue(dir_path, alias_mapping[title]))

    return issue_dirs


def select_issues(base_dir: str, config: dict) -> list[BculIssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[BculIssueDir] | None: List of `BculIssueDir` to import.
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
        return

    issues = detect_issues(base_dir)
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
