"""This module contains helper functions to find SUB OCR data to import."""

import logging
import os
from collections import namedtuple
from datetime import date
import json

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

JSON_FILE = "../data/issue_indices/issue_index.sub.json"

SubIssueDir = namedtuple("IssueDirectory", ["provider", "alias", "date", "edition", "path"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "SUB"
    alias (str): Newspaper alias.
    date (datetime.date): Publication date of issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = SubIssueDir(
    provider='SUB',
    alias='hamb_echo', 
    date=date(1888, 2, 1), 
    edition='a', 
    path='./SUB/hamb_echo/1888/02/01/Abend-Ausgabe'
)
"""


"""def dir2issue(path: str) -> SubIssueDir | None:
    ""Convert a directory path into a SubIssueDir object.

    Expected directory structure:
    [base]/[alias]/[yyyy]/[mm]/[dd]/[edition_name]

    The edition mapping works as follows:
    - Directories with prefixes like "A1-", "A2-", "A3-" are mapped to 'a', 'b', 'c', etc.
    - When multiple directories exist for the same date without edition prefixes,
      they are assigned editions 'a', 'b', 'c' based on alphabetical order

    Args:
        path (str): The issue directory path

    Returns:
        SubIssueDir | None: The corresponding Issue, or None if path is invalid
    ""
    try:
        parts = path.rstrip("/").split("/")

        # Extract components from path
        # Expecting: .../alias/yyyy/mm/dd/edition_name
        if len(parts) < 5:
            logger.warning(f"Path too short to parse: {path}")
            return None

        edition_name = parts[-1]
        day = int(parts[-2])
        month = int(parts[-3])
        year = int(parts[-4])
        alias = parts[-5]

        # Map edition names to lowercase letters
        # Handle explicit edition naming like A1-, A2-, A3- etc.
        if edition_name.startswith(("A1-", "A2-", "A3-", "A4-", "A5-", "A6-", "A7-", "A8-", "A9-")):
            # Extract the number from A1-, A2-, etc. and map to a, b, c...
            edition_number = int(edition_name.split("-")[0][1:])  # Extract 1, 2, 3, etc.
            edition = chr(96 + edition_number)  # Map 1 -> 'a', 2 -> 'b', etc.
        else:
            # For paths without explicit edition prefixes, we need to check if there are
            # multiple directories for the same date. This requires checking sibling directories.
            # For now, default to 'a' - the calling code can adjust if multiple issues per day are detected
            edition = "a"

        return SubIssueDir(
            provider="SUB", alias=alias, date=date(year, month, day), edition=edition, path=path
        )
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse issue directory {path}: {e}")
        return None
"""


def entry2issue(alias: str, year: str, month: str, entry: dict, base_dir: str) -> SubIssueDir:
    """
    Convert a hierarchical JSON entry into a SubIssueDir.

    entry example:
      { "day": "15", "edition": "01", "local_path": "..._01" }
    """
    y = int(year)
    m = int(month)
    d = int(entry["day"])

    edition = entry["edition"]

    return SubIssueDir(
        provider="SUB",
        alias=alias,
        date=date(y, m, d),
        edition=edition,
        path=base_dir + entry["local_path"][0],
    )


def detect_issues(
    base_dir: str, alias_filter: list[str] | None = None, exclude_list: list[str] | None = None
) -> list[SubIssueDir]:
    """Detect SUB issues to import within the filesystem.

    Traverses the directory structure looking for METS XML files that indicate
    a valid issue directory. Handles multiple issues per day by assigning editions
    'a', 'b', 'c', etc. based on alphabetical order of directory names.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to newspaper aliases.
        alias_filter (list[str] | None, optional): Aliases to consider. Defaults to None.
        exclude_list (list[str] | None, optional): Aliases to exclude. Defaults to None.

    Returns:
        list[SubIssueDir]: List of `SubIssueDir` instances to import.
    """
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    issues: list[SubIssueDir] = []

    # Apply alias filters early
    if alias_filter:
        kept_data = {a: d for a, d in data.items() if a in alias_filter}
    if exclude_list:
        kept_data = {a: d for a, d in data.items() if a not in exclude_list}

    for alias, years in kept_data.items():
        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:
                    issue = entry2issue(alias, year, month, entry, base_dir)
                    issues.append(issue)

    return issues


def select_issues(base_dir: str, config: dict) -> list[SubIssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See the configuration documentation for details on filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to newspaper aliases.
        config (dict): Configuration dictionary containing 'titles', 'exclude_titles',
            and 'year_only' keys for filtering.

    Returns:
        list[SubIssueDir] | None: List of `SubIssueDir` instances to import.
    """
    try:
        filter_dict = config.get("titles", {})
        exclude_list = config.get("exclude_titles", [])
        year_flag = config.get("year_only", False)
    except KeyError as e:
        logger.critical(f"Missing required key in config file: {e}")
        return None

    alias_filter = list(filter_dict.keys()) if filter_dict else None

    selected_issues = detect_issues(base_dir, alias_filter, exclude_list)

    # Apply date filtering if we have a filter dict
    if filter_dict and not exclude_list:
        filtered_issues = _apply_datefilter(filter_dict, selected_issues, year_only=year_flag)
    else:
        filtered_issues = selected_issues

    logger.info(f"{len(filtered_issues)} newspaper issues remained after applying filter")

    return filtered_issues
