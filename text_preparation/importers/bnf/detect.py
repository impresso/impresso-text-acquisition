"""This module contains helper functions to find BNF OCR data to import."""

import json
import logging
import os
import string
from collections import namedtuple
from typing import List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from dask import bag as db
from text_preparation.importers.bnf.helpers import get_journal_name, parse_date
from text_preparation.importers.mets_alto.mets import get_dmd_sec
from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

BnfIssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path", "secondary_date"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Note:
    In BNF data, dates can be given in two different formats (separated 
    with `-` or `/`). Also, an issue can have two dates, separated by 
    either `-` or `/`.

Args:
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    secondary_date (datetime.date): Secondary publication date or issue.

>>> from datetime import date
>>> i = BnfIssueDir(
    alias='Marie-Claire', 
    date=datetime.date(1938, 3, 11), 
    edition='a', 
    path='./BNF/files/4701034.zip', 
    secondary_date = None
)
"""

DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d"]
DATE_SEPARATORS = ["/", "-"]


def get_id(issue: BnfIssueDir) -> str:
    """Return an issue's canonical ID given its IssueDir.

    Args:
        issue (BnfIssueDir): IssueDir of issue.

    Returns:
        str: Canonical ID of issue.
    """
    return "{}-{}-{:02}-{:02}-{}".format(
        issue.alias, issue.date.year, issue.date.month, issue.date.day, issue.edition
    )


def get_number(issue: BnfIssueDir) -> str:
    """Return an issue's original identifying number given its IssueDir.

    Args:
        issue (BnfIssueDir): IssueDir of issue.

    Returns:
        str: Identifying number in BNF's original file structure.
    """
    return issue.path.split("/")[-1]


def assign_editions(issues: list[BnfIssueDir]) -> list[BnfIssueDir]:
    """Assign updated edition numbers to each issue of a given day.

    TFor BNF, the issues are not organized by date or edition in the file
    system. Hence, when multiple issues exist for a given day, an indexing
    must be applied to assign edition numbers.

    Args:
        issues (list[BnfIssueDir]): List of issues for a given day.

    Returns:
        list[BnfIssueDir]: List of issues with updated editions.
    """
    issues = sorted(issues, key=lambda x: x[1])
    new_issues = []
    for index, (i, n) in enumerate(issues):
        i = BnfIssueDir(
            alias=i.alias,
            date=i.date,
            edition=string.ascii_lowercase[index],
            path=i.path,
            secondary_date=i.secondary_date,
        )
        new_issues.append((i, n))
    return new_issues


def dir2issue(issue_path: str) -> BnfIssueDir:
    """Create a `BnfIssueDir` object from an archive path.

    Note:
        This function is called internally by `detect_issues`

    Args:
        issue_path (str): The path of the issue within the archive.

    Returns:
        BnfIssueDir: New `BnfIssueDir` object
    """
    manifest_file = os.path.join(issue_path, "manifest.xml")

    issue = None
    if os.path.isfile(manifest_file):
        with open(manifest_file, encoding="utf-8") as f:
            manifest = BeautifulSoup(f, "xml")

        try:
            # Issue info is in dmdSec of id "DMD.2"
            issue_info = get_dmd_sec(manifest, "DMD.2")
            alias = get_journal_name(issue_path)
            np_date, secondary_date = parse_date(
                issue_info.find("date").contents[0], DATE_FORMATS, DATE_SEPARATORS
            )
            edition = "a"
            issue = BnfIssueDir(
                alias=alias,
                date=np_date,
                edition=edition,
                path=issue_path,
                secondary_date=secondary_date,
            )
        except ValueError as e:
            logger.info(e)
            logger.error("Could not parse issue at %s", issue_path)
    else:
        logger.error("Could not find manifest in %s", issue_path)
    return issue


def detect_issues(base_dir: str) -> list[BnfIssueDir]:
    """Detect BNF issues to import within the filesystem

    This function the directory structure used by BNF (one subdir by journal).

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[BnfIssueDir]: List of `BnfIssueDir` instances, to be imported.
    """
    dir_path, dirs, _ = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs if not _dir.startswith("2")]
    issue_dirs = [
        os.path.join(journal, _dir) for journal in journal_dirs for _dir in os.listdir(journal)
    ]

    issue_dirs = [dir2issue(_dir) for _dir in issue_dirs]
    initial_length = len(issue_dirs)
    issue_dirs = [i for i in issue_dirs if i is not None]

    df = pd.DataFrame([{"issue": i, "id": get_id(i), "number": get_number(i)} for i in issue_dirs])
    vals = df.groupby("id").apply(lambda x: x[["issue", "number"]].values.tolist()).values
    # TODO -> keep info of number in issue for legacy
    issue_dirs = [i if len(i) == 1 else assign_editions(i) for i in vals]
    issue_dirs = [j[0] for i in issue_dirs for j in i]

    logger.info("Removed %s problematic issues", initial_length - len(issue_dirs))
    return [i for i in issue_dirs if i is not None]


def select_issues(base_dir: str, config: dict) -> Optional[List[BnfIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        Optional[List[BnfIssueDir]]: List of `BnfIssueDir` instances, to be imported.
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
