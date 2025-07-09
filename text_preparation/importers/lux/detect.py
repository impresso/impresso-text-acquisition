"""This module contains helper functions to find BNL OCR data to be imported."""

import logging
import os
from collections import namedtuple
from datetime import date

from dask import bag as db
from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}

LuxIssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path"])
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
>>> i = LuxIssueDir('armeteufel', date(1904,1,17), 'a', './protected_027/1497608_newspaper_armeteufel_1904-01-17/')
"""


def dir2issue(path: str) -> LuxIssueDir:
    """Create a `LuxIssueDir` from a directory (BNL format).

    Called internally by :func:`detect_issues`.

    Args:
        path (str): Path of issue.

    Returns:
        Rero2IssueDir: New `LuxIssueDir` object matching the path and rights.
    """
    issue_dir = os.path.basename(path)
    local_id = issue_dir.split("_")[2]
    issue_date = issue_dir.split("_")[3]
    year, month, day = issue_date.split("-")

    if len(issue_dir.split("_")) == 4:
        edition = "a"
    elif len(issue_dir.split("_")) == 5:
        edition = issue_dir.split("_")[4]
        edition = EDITIONS_MAPPINGS[int(edition)]

    return LuxIssueDir(local_id, date(int(year), int(month), int(day)), edition, path)


def detect_issues(base_dir: str) -> list[LuxIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNL used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[LuxIssueDir]: List of `LuxIssueDir` instances, to be imported.
    """
    dir_path, dirs, _ = next(os.walk(base_dir))
    batches_dirs = [os.path.join(dir_path, dir) for dir in dirs]
    issue_dirs = [
        os.path.join(batch_dir, dir)
        for batch_dir in batches_dirs
        for dir in os.listdir(batch_dir)
        if "newspaper" in dir
    ]
    return [dir2issue(_dir) for _dir in issue_dirs]


def select_issues(base_dir: str, config: dict) -> list[LuxIssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[LuxIssueDir] | None: List of `LuxIssueDir` instances to import.
    """
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
    msg = (
        f"{len(filtered_issues)} newspaper issues remained "
        f"after applying filter: {filtered_issues}"
    )
    logger.info(msg)
    return filtered_issues
