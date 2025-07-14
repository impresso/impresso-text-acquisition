"""This module contains helper functions to find INA ASR data to import."""

import logging
import os
import json
from datetime import datetime
from collections import namedtuple

from dask import bag as db

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

METADATA_FILENAME = "ina_metadata.json"

INAIssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path", "metadata_file"])
"""A light-weight data structure to represent a radio audio broadcast issue.

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
>>> i = INAIssueDir(
    alias='SOC_CJ', 
    date=datetime.date(1940, 07, 22), 
    edition='a', 
    path='./SOC_CJ/1940/07/22/a', 
)
"""


def dir2issue(path: str, metadata_file_path: str) -> INAIssueDir | None:
    """Create a `INAIssueDir` object from a directory.

    Note:
        This function is called internally by `detect_issues`

    Args:
        path (str): The path of the issue.
        access_rights (dict): Dictionary for access rights.

    Returns:
        INAIssueDir | None: New `INAIssueDir` object.
    """
    issue_dir_key = os.path.basename(path)

    with open(metadata_file_path, "r", encoding="utf-8") as f:
        metadata_json = json.load(f)

    issue_metadata = metadata_json[issue_dir_key]
    alias = issue_metadata["Alias Collection"]
    # issue_date = issue_metadata["Date d'enregistrement"]
    issue_date = datetime.strptime(issue_metadata["Date d'enregistrement"], "%d/%m/%Y").date()
    # TODO update once we have more info and context
    edition = "a"

    return INAIssueDir(
        alias=alias,
        date=issue_date,
        edition=edition,
        path=path,
        metadata_file=metadata_file_path,
    )


def detect_issues(base_dir: str) -> list[INAIssueDir]:
    """Detect INA Radio broadcasts to import within the filesystem.

    This function expects the directory structure that we created for Swissinfo.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[INAIssueDir]: List of `INAIssueDir` instances, to be imported.
    """

    metadata_file_path = os.path.join(base_dir, METADATA_FILENAME)

    with open(metadata_file_path, "r", encoding="utf-8") as f:
        metadata_json = json.load(f)

    dir_path, dirs, _ = next(os.walk(base_dir))

    journal_dirs = [os.path.join(dir_path, j_dir) for j_dir in dirs if j_dir in metadata_json]

    """
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
    """

    # issue_dirs = [dir2issue(_dir, metadata_file_path) for _dir in journal_dirs]
    # print(f"issue_dirs = {issue_dirs}")
    # return issue_dirs
    return [dir2issue(_dir, metadata_file_path) for _dir in journal_dirs]


def select_issues(base_dir: str, config: dict) -> list[INAIssueDir] | None:
    """Detect selectively issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[INAIssueDir] | None: List of `INAIssueDir` to import.
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
    print(f"SELECT issues = {issues}, filtered_issues = {filtered_issues}")

    return filtered_issues
