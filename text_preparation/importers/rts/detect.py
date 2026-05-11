"""This module contains helper functions to find INA ASR data to import."""

import logging
import os
import json
from datetime import datetime, date
from collections import namedtuple
from typing import Any

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

JSON_FILE = "../data/issue_indices/issue_index.rts.json"
METADATA_FILE = "../data/sample_data/RTS/issue_metadata.rts.json"

RTSIssueDir = namedtuple(
    "IssueDirectory", ["provider", "alias", "date", "edition", "path", "issue_metadata"]
)
"""A light-weight data structure to represent a radio audio broadcast issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of bulletins published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "RTS"
    alias (str): Bulletin alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = RTSIssueDir(
    provider='RTS',
    alias='ana_media', 
    date=datetime.date(1996, 11, 11), 
    edition='a', 
    path='./', 
    issue_metadata={...}
)
"""


"""def dir2issue(path: str, metadata_file_path: str) -> RTSIssueDir | None:
    Create a `RTSIssueDir` object from a directory.

    Note:
        This function is called internally by `detect_issues`

    Args:
        path (str): The path of the issue.
        access_rights (dict): Dictionary for access rights.

    Returns:
        RTSIssueDir | None: New `RTSIssueDir` object.
    
    issue_dir_key = os.path.basename(path)

    with open(metadata_file_path, "r", encoding="utf-8") as f:
        metadata_json = json.load(f)

    issue_metadata = metadata_json[issue_dir_key]
    alias = issue_metadata["Alias Collection"]
    # issue_date = issue_metadata["Date d'enregistrement"]
    issue_date = datetime.strptime(issue_metadata["Date d'enregistrement"], "%d/%m/%Y").date()
    # TODO update once we have more info and context
    edition = "a"

    return RTSIssueDir(
        provider="RTS",
        alias=alias,
        date=issue_date,
        edition=edition,
        path=path,
        metadata_file=metadata_file_path,
    )"""


def entry2issue(
    alias: str, year: str, month: str, entry: dict, base_dir: str, alias_issues: dict[str, Any]
) -> RTSIssueDir:
    """
    Convert a hierarchical JSON entry into a RTSIssueDir.

    entry example:
      { "day": "15", "edition": "01", "local_path": ["[...].mp3"]}
    """
    y = int(year)
    m = int(month)
    d = int(entry["day"])

    edition = entry["edition"]

    issue_id = f"{alias}-{year}-{month}-{d}-{edition}"

    return RTSIssueDir(
        provider="RTS",
        alias=alias,
        date=date(y, m, d),
        edition=edition,
        path=base_dir,
        issue_metadata=alias_issues[issue_id],
    )


def detect_issues(
    base_dir: str, alias_filter: list[str] | None = None, exclude_list: list[str] | None = None
) -> list[RTSIssueDir]:
    """Detect INA Radio broadcasts to import within the filesystem.

    This function expects the directory structure that we created for Swissinfo.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[RTSIssueDir]: List of `RTSIssueDir` instances, to be imported.
    """
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        issues_metadata = json.load(f)
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        issues_data = json.load(f)

    issues: list[RTSIssueDir] = []

    # Apply alias filters early
    if alias_filter:
        kept_data = {a: d for a, d in issues_data.items() if a in alias_filter}
    if exclude_list:
        kept_data = {a: d for a, d in issues_data.items() if a not in exclude_list}

    for alias, years in kept_data.items():
        alias_issues = issues_metadata[alias]
        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:
                    issue = entry2issue(alias, year, month, entry, base_dir, alias_issues)
                    issues.append(issue)

    return issues


def select_issues(base_dir: str, config: dict) -> list[RTSIssueDir] | None:
    """Detect selectively issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Note:
        For RTS, the basedir is the original dir, each issue_metadata holds
        the necessary parts to the specific audio and xml files.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[RTSIssueDir] | None: List of `RTSIssueDir` to import.
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

    logger.info(
        f"{len(filtered_issues)} newspaper issues remained after applying filter ({len(selected_issues)} before.)"
    )

    return filtered_issues

    print(f"SELECT issues = {issues}, filtered_issues = {filtered_issues}")

    return filtered_issues
