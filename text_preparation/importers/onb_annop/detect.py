"""This module contains helper functions to find the ONB ANNOP OCR data to import."""

import logging
import os
from collections import namedtuple
from datetime import date
from pathlib import Path

from dask import bag as db
from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

OnbIssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path", "pages"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    alias (str): Newspaper ID.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    rights (str): Access rights on the data (open, closed, etc.).

>>> from datetime import date
>>> i = OnbIssueDir(
        alias='nwb', 
        date=date(1874,01,06), 
        edition='a', path='./ANNO/nwb/1874/01/06',
        pages=[(
            'nwb-1874-01-06-a-p0001', 
            '00000001.xml'
        ), ...]
    )
"""


def dir2issue(path: str) -> OnbIssueDir:
    """Create a `OnbIssueDir` from a directory (ONB format).

    Args:
        path (str): Path of issue.

    Returns:
        OnbIssueDir: New `OnbIssueDir` object matching the path and rights.
    """
    split_path = path.split("/")
    alias = split_path[-4]
    issue_date = date.fromisoformat("-".join(split_path[-3:]))
    edition = "a"
    issue_id = "-".join(split_path[-4:] + [edition])

    pages = []
    for file in os.listdir(path):
        if ".xml" in file:
            p_number = int(Path(file).stem)
            p_id = f"{issue_id}-p{str(p_number).zfill(4)}"
            pages.append((p_id, os.path.basename(file)))

    # sort the pages by number
    pages.sort(key=lambda x: int(x[1].replace(".xml", "")))

    return OnbIssueDir(
        alias=alias,
        date=issue_date,
        edition=edition,
        path=path,
        pages=pages,
    )


def detect_issues(base_dir: str) -> list[OnbIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that ONB used to
    organize the dump of Alto OCR data.

    The access rights information is not in place yet, but needs
    to be specified by the content provider (ONB).

    TODO: Add the directory structure of ONB OCR data dumps.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[SwaIssueDir]: list of ``OnbIssueDir`` instances, to be imported.
    """
    # For now, only ANNO titles are in mets/alto format
    anno_path = os.path.join(base_dir, "ANNO")

    dir_path, dirs, _ = next(os.walk(anno_path))
    journal_dirs = [os.path.join(dir_path, j_dir) for j_dir in dirs]
    # iteratively
    issues_dirs = [
        os.path.join(alias, year, month, day)
        for alias in journal_dirs
        for year in os.listdir(alias)
        for month in os.listdir(os.path.join(alias, year))
        for day in os.listdir(os.path.join(alias, year, month))
    ]

    return [dir2issue(_dir) for _dir in issues_dirs]


def select_issues(base_dir: str, config: dict) -> list[OnbIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Note:
        The access rights information is not in place yet, but needs
        to be specified by the content provider (ONB).

    TODO: `select_issues` has a lot of code reuse among importers, move to
    `utils.py` or something similar.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[OnbIssueDir]: list of ``OnbIssueDir`` instances, to be imported.
    """
    try:
        filter_dict = config.get("titles")
        exclude_list = config["exclude_titles"]
        year_flag = config["year_only"]
    except KeyError:
        logger.critical("The key [titles|exclude_titles|year_only] is missing in the config file.")
        return []

    exclude_flag = False if not exclude_list else True
    msg = (
        f"got filter_dict: {filter_dict}, "
        f"\nexclude_list: {exclude_list}, "
        f"\nyear_flag: {year_flag}"
        f"\nexclude_flag: {exclude_flag}"
    )
    logger.debug(msg)

    filter_titles = set(filter_dict.keys()) if not exclude_flag else set(exclude_list)
    msg = f"got filter_titles: {filter_titles}, " f"with exclude flag: {exclude_flag}"
    logger.debug(msg)

    issues = detect_issues(base_dir)

    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag.filter(
        lambda i: (len(filter_dict) == 0 or i.alias in filter_dict.keys())
        and i.alias not in exclude_list
    ).compute()

    exclude_flag = bool(exclude_list)
    filtered_issues = (
        _apply_datefilter(filter_dict, selected_issues, year_only=year_flag)
        if not exclude_flag
        else selected_issues
    )

    msg = (
        f"{len(filtered_issues)} newspaper issues remained after applying filter: {filtered_issues}"
    )
    logger.info(msg)

    return filtered_issues
