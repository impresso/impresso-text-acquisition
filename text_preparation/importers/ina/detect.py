"""Helper functions for detecting and selecting INA ASR broadcast data to import."""

import logging
import os
import json
from datetime import datetime, date
from collections import namedtuple
from typing import Any

from ast import literal_eval

from dask import bag as db

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

JSON_FILE = "../data/issue_indices/issue_index.ina.json"
METADATA_FILE = "../data/sample_data/INA/issues_metadata.ina.json"
FAILED_COPIES_FILE = "../data/sample_data/INA/failed_audio_copies.jsonl"
# these four issues have faulty json files in their formatting. As a result we will ignore them during ingestion
ADDITIONAL_FAULTY_ISSUES = [
    "TrParis-1951-06-13-a",
    "TrParis-1951-05-16-d",
    "TrParis-1951-05-16-a",
    "InterSoir-1995-01-03-b",
]

INAIssueDir = namedtuple(
    "IssueDirectory", ["provider", "alias", "date", "edition", "path", "issue_metadata"]
)
"""Lightweight data structure representing a single INA radio broadcast issue.

Can be used to locate data in the filesystem or to build canonical identifiers
for the issue and its audio records.

Note:
    When multiple broadcasts are published on the same day, a lowercase letter
    indicates the edition: ``'a'`` for the first, ``'b'`` for the second, etc.

Attributes:
    provider (str): Data provider, always ``"INA"`` for this importer.
    alias (str): Unique broadcast title alias.
    date (datetime.date): Broadcast date of the issue.
    edition (str): Edition letter (``'a'``, ``'b'``, ``'c'``, …).
    path (str): Path to the directory containing the issue's data files.
    issue_metadata (dict[str, Any]): Provider-supplied metadata for the issue.

Example:
    >>> from datetime import date
    >>> i = INAIssueDir(
    ...     provider='INA',
    ...     alias='SOC_CJ',
    ...     date=date(1940, 7, 22),
    ...     edition='a',
    ...     path='./SOC_CJ/1940/07/22/a',
    ...     issue_metadata={},
    ... )
"""


"""def dir2issue(path: str, metadata_file_path: str) -> INAIssueDir | None:
    Create a `INAIssueDir` object from a directory.

    Note:
        This function is called internally by `detect_issues`

    Args:
        path (str): The path of the issue.
        access_rights (dict): Dictionary for access rights.

    Returns:
        INAIssueDir | None: New `INAIssueDir` object.
    
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
        provider="INA",
        alias=alias,
        date=issue_date,
        edition=edition,
        path=path,
        metadata_file=metadata_file_path,
    )"""


def entry2issue(
    alias: str, year: str, month: str, entry: dict, base_dir: str, alias_issues: dict[str, Any]
) -> INAIssueDir:
    """Convert a hierarchical JSON index entry into an :class:`INAIssueDir`.

    Args:
        alias (str): Broadcast title alias (e.g. ``"SOC_CJ"``).
        year (str): Four-digit year string (e.g. ``"1940"``).
        month (str): Two-digit month string (e.g. ``"07"``).
        entry (dict): Single issue entry from the index, containing at minimum
            ``"day"`` and ``"edition"`` keys (e.g.
            ``{"day": "15", "edition": "01", "local_path": ["...mp3"]}``).
        base_dir (str): Absolute path to the root data directory for this alias.
        alias_issues (dict[str, Any]): Mapping of issue IDs to their provider
            metadata for the given alias, keyed by canonical issue ID.

    Returns:
        INAIssueDir: Populated named tuple for the issue.
    """
    y = int(year)
    m = int(month)
    d = entry["day"]

    edition = entry["edition"]

    issue_id = f"{alias}-{year}-{month}-{d}-{edition}"

    return INAIssueDir(
        provider="INA",
        alias=alias,
        date=date(y, m, int(d)),
        edition=edition,
        path=base_dir,
        issue_metadata=alias_issues[issue_id],
    )


def detect_issues(
    base_dir: str, alias_filter: list[str] | None = None, exclude_list: list[str] | None = None
) -> list[INAIssueDir]:
    """Detect INA radio broadcast issues available for import within the filesystem.

    Reads the issue index and metadata files, filters out known-faulty entries,
    and returns one :class:`INAIssueDir` per importable issue.

    Args:
        base_dir (str): Path to the root directory of the INA broadcast data.
        alias_filter (list[str] | None): If provided, only issues whose alias is
            in this list are included. Defaults to ``None`` (no filter).
        exclude_list (list[str] | None): If provided, issues whose alias appears
            in this list are excluded. Defaults to ``None`` (no exclusions).

    Returns:
        list[INAIssueDir]: Issue instances ready for import.
    """
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        issues_metadata = json.load(f)
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        issues_data = json.load(f)

    with open(FAILED_COPIES_FILE, "r", encoding="utf-8") as f:
        failed_copies_list = list(f)

    issues_to_ignore = [json.loads(l)["issue_id"] for l in failed_copies_list]

    issues: list[INAIssueDir] = []

    msg = f"INSIDE INA DETECT ISSUES: alias_filter: {alias_filter}"
    print(msg)

    # Apply alias filters early
    if alias_filter:
        kept_data = {a: d for a, d in issues_data.items() if a in alias_filter}
    if exclude_list:
        kept_data = {a: d for a, d in issues_data.items() if a not in exclude_list}
    else:
        kept_data = issues_data

    for alias, years in kept_data.items():
        alias_issues = issues_metadata[alias]
        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:

                    issue_id = f"{alias}-{year}-{month}-{entry['day']}-{entry['edition']}"
                    if (
                        issue_id not in issues_to_ignore
                        and issue_id not in ADDITIONAL_FAULTY_ISSUES
                    ):
                        issue = entry2issue(alias, year, month, entry, base_dir, alias_issues)
                        issues.append(issue)
                    else:
                        # don't process the issues which have faulty audio files
                        msg = f"{issue_id} - This issue is in the list of faulty audio files - skipping."
                        print(msg)
                        logger.info(msg)

    return issues


def select_issues(base_dir: str, config: dict) -> list[INAIssueDir] | None:
    """Detect and filter issues to import according to a configuration dictionary.

    Behaves like :func:`detect_issues` but applies additional filtering rules
    specified in ``config``. See the importer configuration documentation for
    the supported keys and filtering semantics.

    Note:
        For INA, ``base_dir`` points to the original data root; each issue's
        metadata contains the relative paths to its audio and transcript files.

    Args:
        base_dir (str): Path to the root directory of the INA broadcast data.
        config (dict): Filtering configuration. Recognised keys:

            - ``"aliases"`` (dict): Mapping of alias to date range(s) to include.
            - ``"exclude_aliases"`` (list[str]): Aliases to exclude.
            - ``"year_only"`` (bool): If ``True``, filter by year only (ignore
              month/day). Defaults to ``False``.

    Returns:
        list[INAIssueDir] | None: Filtered list of issue instances to import,
        or ``None`` if a required configuration key is missing.
    """

    try:
        filter_dict = config.get("aliases", {})
        exclude_list = config.get("exclude_aliases", [])
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
