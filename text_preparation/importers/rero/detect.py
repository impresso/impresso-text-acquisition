"""This module contains helper functions to find RERO OCR data to be imported."""

import logging
import os
from collections import namedtuple
from datetime import datetime

from dask import bag as db

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}

Rero2IssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path"])
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

>>> from datetime import date
>>> i = Rero2IssueDir('BLB', date(1845,12,28), 'a', './BLB/data/BLB/18451228_01')
"""


def dir2issue(path: str) -> Rero2IssueDir:
    """Create a `Rero2IssueDir` from a directory (RERO format).

    Args:
        path (str): Path of issue.
        access_rights (dict): dictionary for access rights.

    Returns:
        Rero2IssueDir: New `Rero2IssueDir` object matching the path and rights.
    """
    alias, issue = path.split("/")[-2:]
    date, edition = issue.split("_")[:2]
    date = datetime.strptime(date, "%Y%m%d").date()

    edition = EDITIONS_MAPPINGS[int(edition)]

    return Rero2IssueDir(
        alias=alias,
        date=date,
        edition=edition,
        path=path,
    )


def detect_issues(base_dir: str, data_dir: str = "data") -> list[Rero2IssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that RERO used to
    organize the dump of Mets/Alto OCR data.

    TODO: add info on the file structure.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        data_dir (str, optional): Directory where data is stored
            (usually `data/`). Defaults to 'data'.

    Returns:
        list[Rero2IssueDir]: list of `Rero2IssueDir` instances, to be imported.
    """
    dir_path, dirs, _ = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    journal_dirs = [
        os.path.join(alias, _dir, _dir2)
        for alias in journal_dirs
        for _dir in os.listdir(alias)
        if _dir == data_dir
        for _dir2 in os.listdir(os.path.join(alias, _dir))
    ]

    issues_dirs = [
        os.path.join(j_dir, l)
        for j_dir in journal_dirs
        for l in os.listdir(j_dir)
        if ".DS_Store" not in l
    ]

    return [dir2issue(_dir) for _dir in issues_dirs]


def select_issues(base_dir: str, config: dict) -> list[Rero2IssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[Rero2IssueDir] | None: `Rero2IssueDir` instances to be imported.
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
