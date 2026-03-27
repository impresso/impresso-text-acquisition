"""This module contains helper functions to find BNL OCR data to be imported."""

import logging
import json
from collections import namedtuple
from datetime import date

from dask import bag as db
from text_preparation.importers.detect import _apply_datefilter
from text_preparation.utils import edition_num_to_code

logger = logging.getLogger(__name__)

BASE_DIR = "/mnt/project_impresso/original"

JSON_FILE = "../data/sample_data/BNL/issues_to_ingest.json"

LuxIssueDir = namedtuple("IssueDirectory", ["provider", "alias", "date", "edition", "path"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "BNL"
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = LuxIssueDir('BNL','armeteufel', date(1904,1,17), 'a', './protected_027/1497608_newspaper_armeteufel_1904-01-17/')
"""


def entry2issue(alias: str, year: str, month: str, entry: dict, base_dir: str) -> LuxIssueDir:
    """
    Convert a hierarchical JSON entry into a LuxIssueDir.

    entry example:
      { "day": "15", "edition": "01", "local_path": "..._01" }
    """
    y = int(year)
    m = int(month)
    d = int(entry["day"])

    edition = entry["edition"]

    return LuxIssueDir(
        provider="BNL",
        alias=alias,
        date=date(y, m, d),
        edition=edition,
        path=base_dir + entry["local_path"],
    )


def detect_issues(base_dir: str) -> list[LuxIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNL used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[LuxIssueDir]: List of `LuxIssueDir` instances, to be imported.
    """
    # 1. Ensure base_dir is what we expect
    if base_dir != BASE_DIR:
        raise ValueError(f"BNL importer expects base_dir='{BASE_DIR}', got '{base_dir}'")
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    issues: list[LuxIssueDir] = []

    for alias, years in data.items():
        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:
                    issue = entry2issue(alias, year, month, entry, base_dir)
                    issues.append(issue)

    return issues


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
