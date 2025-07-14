"""This module contains helper functions to find SWISSINFO OCR data to be imported."""

import logging
import os
from datetime import date
from collections import namedtuple

from dask import bag as db

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

METADATA_FILENAME = "SOC_rb_metadata.json"

SwissInfoIssueDir = namedtuple(
    "IssueDirectory", ["alias", "date", "edition", "path", "metadata_file"]
)
"""A light-weight data structure to represent a radio bulletin issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of bulletins published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    alias (str): Bulletin alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = SwissInfoIssueDir(
    alias='SOC_CJ', 
    date=datetime.date(1940, 07, 22), 
    edition='a', 
    path='./SOC_CJ/1940/07/22/a', 
    metadata_file='../data/sample_data/SWISSINFO/bulletins_metadata.json'
)
"""


def dir2issue(path: str, metadata_file_path: str) -> SwissInfoIssueDir | None:
    """Create a `SwissInfoIssueDir` object from a directory.

    Note:
        This function is called internally by `detect_issues`

    Args:
        path (str): The path of the issue.
        access_rights (dict): Dictionary for access rights.

    Returns:
        SwissInfoIssueDir | None: New `SwissInfoIssueDir` object.
    """
    split_path = path.split("/")
    alias = split_path[-5]
    issue_date = date.fromisoformat("-".join(split_path[-4:-1]))
    edition = split_path[-1]

    return SwissInfoIssueDir(
        alias=alias,
        date=issue_date,
        edition=edition,
        path=path,
        metadata_file=metadata_file_path,
    )


def detect_issues(base_dir: str) -> list[SwissInfoIssueDir]:
    """Detect SWISSINFO Radio bulletins to import within the filesystem.

    This function expects the directory structure that we created for Swissinfo.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        access_rights (str): unused argument kept for conformity for now.

    Returns:
        list[SwissInfoIssueDir]: List of `SwissInfoIssueDir` instances, to be imported.
    """

    swissinfo_path = os.path.join(base_dir, "WW2-SOC-bulletins-json")
    metadata_file_path = os.path.join(swissinfo_path, METADATA_FILENAME)

    dir_path, dirs, _ = next(os.walk(swissinfo_path))

    journal_dirs = [os.path.join(dir_path, j_dir) for j_dir in dirs]
    # iteratively
    issues_dirs = [
        os.path.join(alias, year, month, day, edition)
        for alias in journal_dirs
        for year in os.listdir(alias)
        for month in os.listdir(os.path.join(alias, year))
        for day in os.listdir(os.path.join(alias, year, month))
        for edition in os.listdir(os.path.join(alias, year, month, day))
    ]

    return [dir2issue(_dir, metadata_file_path) for _dir in issues_dirs]


def select_issues(base_dir: str, config: dict) -> list[SwissInfoIssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[SwissInfoIssueDir] | None: List of `SwissInfoIssueDir` to import.
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
