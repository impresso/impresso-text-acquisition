"""This module contains helper functions to find BL OCR data to import."""

import logging
import os
import json
from collections import namedtuple
from datetime import date

from text_preparation.importers.detect import _apply_datefilter

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}
# BL_SAMPLE_DIR = "../text_preparation/data/sample_data/BL/"
BL_OCR_FILE = "BL_ocr_formats.json"
BL_FORMAT_SPECIFIC_FILE = "BL_{ocr_format}_issues.json"
POSSIBLE_FORMATS = ["OmniPage-NLP", "BL-ALIAS", "Nuance-NLP", "ABBYY-ALIAS", "ABBYY-NLP"]
# add here the file with the mapping from issue to working and alternative titles

BlIssueDir = namedtuple("IssueDirectory", ["provider", "alias", "date", "edition", "path", "nlp"])
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "BL"
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    nlp (str): BL internal NLP for this issue (eg. '0002088')

>>> from datetime import date
>>> i = BlIssueDir(
    provider='BL',
    alias='LSGA', 
    date=datetime.date(1832, 11, 23), 
    edition='a', 
    path='./BL/LSGA/0002088/1832/11/23', 
    nlp='0002088'
)
"""


def dir2issue(path: str) -> BlIssueDir | None:
    """Given the directory of an issue, create the `BlIssueDir` object.

    Args:
        path (str): The issue directory path

    Returns:
        Optional[BlIssueDir]: The corresponding Issue
    """
    split = path.split("/")
    alias, nlp, year, month, day = (
        split[-5],
        split[-4],
        int(split[-3]),
        int(split[-2]),
        int(split[-1]),
    )

    # TODO fix this!!! there are some issues which have multiple editions!!!
    return BlIssueDir(
        provider="BL", alias=alias, date=date(year, month, day), edition="a", path=path, nlp=nlp
    )


def detect_issues(
    base_dir: str,
    ocr_format: str = "OmniPage-NLP",
    bl_issues_for_format: str | None = BL_FORMAT_SPECIFIC_FILE,
    alias_filter: list[str] | None = None,
    exclude_list: list[str] | None = None,
) -> list[BlIssueDir]:
    """Detect BL issues to import within the filesystem.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to the BL aliases.
        ocr_format (str, optional): BL OCR format which is to be processed. Defaults to "OmniPage-NLP".
        bl_issues_for_format (str | None, optional): Name of the file which contains the list of issues
            for the given OCR format. Defaults to BL_FORMAT_SPECIFIC_FILE.
        alias_filter (list[str] | None, optional): Aliases to consider. Defaults to None.
        exclude_list (list[str] | None, optional): Aliases to exclude. Defaults to None.

    Returns:
        list[BlIssueDir]: List of `BlIssueDir` instances to import.
    """
    # Fin the file with the BL issues for the wanted format
    ocr_format_filepath = os.path.join(base_dir, bl_issues_for_format.format(ocr_format=ocr_format))

    with open(ocr_format_filepath, "r", encoding="utf-8") as fin:
        issues_for_format = json.load(fin)

    all_issues = []
    for alias, issues_of_alias in issues_for_format.items():

        if (alias_filter and alias not in alias_filter) or (exclude_list and alias in exclude_list):
            # if any of the filters are defined and the current alias should not be processed, skip
            msg = f"Skipping {alias} - based on config filters."
            logger.debug(msg)
            continue

        issue_paths = [dir2issue(path) for path in list(issues_of_alias["priority_issues"].keys())]
        msg = f"{alias} - Found {len(issue_paths)} issues"
        logger.debug(msg)
        all_issues.extend(issue_paths)

    return all_issues


def select_issues(
    base_dir: str,
    config: dict,
    ocr_format: str = "OmniPage-NLP",
    bl_issues_for_format: str | None = BL_FORMAT_SPECIFIC_FILE,
) -> list[BlIssueDir] | None:
    """SDetect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to the BL aliases.
        ocr_format (str, optional): BL OCR format which is to be processed. Defaults to "OmniPage-NLP".
        bl_issues_for_format (str | None, optional): Name of the file which contains the list of issues
            for the given OCR format. Defaults to BL_FORMAT_SPECIFIC_FILE.

    Returns:
        list[BlIssueDir] | None: List of `BlIssueDir` instances to import.
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
        return None

    alias_filter = list(filter_dict.keys())

    selected_issues = detect_issues(
        base_dir, ocr_format, bl_issues_for_format, alias_filter, exclude_list
    )

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
