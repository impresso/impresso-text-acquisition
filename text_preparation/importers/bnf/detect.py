"""This module contains helper functions to find BNF OCR data to import."""

import logging
import os
import json
import string
from collections import namedtuple
from typing import List, Optional
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup
from dask import bag as db
from text_preparation.importers.bnf.helpers import get_journal_name, parse_date
from text_preparation.importers.mets_alto.mets import get_dmd_sec
from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

BnfIssueDir = namedtuple(
    "IssueDirectory",
    [
        "provider",
        "alias",
        "date",
        "edition",
        "path",
        "ark_id",
        "title_ark",
        "batch",
        "secondary_date",
    ],
)
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
    provider (str): Provider for this alias, here always "BNF"
    alias (str): Newspaper alias.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    secondary_date (datetime.date): Secondary publication date or issue.

>>> from datetime import date
>>> i = BnfIssueDir(
    provider='BNF',
    alias='Marie-Claire', 
    date=datetime.date(1925, 10, 01), 
    edition='a', 
    path="BNF/abendland/1925/10/01/a", 
    ark_id="bpt6k47732053",
    title_ark="cb343488519",
    secondary_date = None,
)
"""

DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d"]
DATE_SEPARATORS = ["/", "-"]

FORMATS = {"BNF-OCR": "ocr", "MP-OLR": "mp", "EN-OLR": "en"}
JSON_FILE = "../data/issue_indices/issue_index.bnf_{format}.json"
ALIAS_TO_ARKS_FILE = "../data/sample_data/BNF_API/alias_to_ark.json"
FORMAT = None

# Listing issue ark_ids which had incomplete dates and were approximated to the 1st of the month/year
INEXACT_DATE_ISSUES = {
    "bpt6k6357584w",
    "bpt6k6505631k",
    "bpt6k64182864",
    "bpt6k6460684k",
    "bpt6k6465476w",
    "bpt6k6446747z",
    "bpt6k6463830n",
    "bpt6k201227m",
    "bpt6k2012280",
    "bpt6k201229c",
    "bpt6k2012309",
    "bpt6k201231p",
    "bpt6k2012322",
    "bpt6k201233f",
    "bpt6k201234t",
    "bpt6k2012356",
    "bpt6k201236k",
    "bpt6k201237z",
    "bpt6k201238b",
    "bpt6k201239q",
    "bpt6k201240n",
    "bpt6k9815051w",
    "bpt6k104618h",
    "bpt6k104619w",
    "bpt6k2012445",
    "bpt6k1046216",
    "bpt6k104622k",
    "bpt6k104623z",
    "bpt6k104624b",
    "bpt6k104625q",
    "bpt6k1046263",
    "bpt6k104627g",
    "bpt6k104628v",
    "bpt6k98146507",
    "bpt6k1066182",
    "bpt6k106619f",
    "bpt6k106620c",
    "bpt6k106621r",
    "bpt6k1066224",
    "bpt6k106624w",
    "bpt6k1066258",
    "bpt6k106626n",
    "bpt6k1066271",
    "bpt6k106628d",
    "bpt6k106629s",
    "bpt6k106630q",
    "bpt6k1066313",
    "bpt6k106632g",
    "bpt6k106633v",
    "bpt6k1066347",
    "bpt6k106635m",
    "bpt6k4766447s",
    "bpt6k4773164m",
    "bd6t550851129",
    "bpt6k4140780k",
    "bpt6k70359657",
    "bpt6k97427650",
    "bpt6k4715134f",
    "bpt6k5497472g",
    "bpt6k7631174n",
    "bpt6k7631177w",
    "bpt6k76311752",
    "bpt6k7631176g",
    "bpt6k46741905",
    "bpt6k97425659",
    "bpt6k9742578x",
    "bpt6k115388h",
    "bpt6k115392k",
    "bpt6k115393z",
    "bpt6k115394b",
    "bpt6k67132s",
    "bpt6k671334",
    "bpt6k67134g",
    "bpt6k67135t",
    "bpt6k67140d",
    "bpt6k67141r",
    "bpt6k671423",
    "bpt6k67143f",
    "bpt6k67144s",
    "bpt6k671454",
    "bpt6k67146g",
    "bpt6k7053456g",
    "bpt6k7053457w",
    "bpt6k70534589",
    "bd6t54196152f",
    "bd6t54182736w",
    "bd6t54180388m",
    "bd6t542051480",
    "bpt6k56009224",
    "bpt6k106239w",
    "bpt6k56873886",
    "bpt6k5687175w",
    "bpt6k56873923",
    "bpt6k106240t",
    "bpt6k5696170s",
    "bpt6k5727872n",
    "bpt6k5727852w",
    "bpt6k57278784",
    "bpt6k5727866x",
    "bpt6k106242k",
    "bpt6k56885975",
    "bpt6k106243z",
    "bpt6k56962704",
    "bpt6k106248v",
    "bpt6k5696212p",
    "bpt6k5696296c",
    "bpt6k5416020v",
    "bpt6k1062505",
    "bpt6k5690493z",
    "bpt6k106251j",
    "bpt6k5690516h",
    "bpt6k106252x",
    "bpt6k1062539",
    "bpt6k5696077b",
    "bpt6k1062552",
    "bpt6k106256f",
    "bpt6k106257t",
    "bpt6k4766753j",
    "bpt6k4766754z",
    "bpt6k4766755c",
    "bpt6k6517037w",
    "bpt6k63508054",
    "bpt6k6257708z",
    "bpt6k6262047c",
    "bpt6k6257711f",
    "bpt6k6257714p",
    "bpt6k6247942d",
    "bpt6k6215666b",
    "bpt6k6215667r",
}


def set_json_file(format):
    global FORMAT, JSON_FILE

    FORMAT = format
    JSON_FILE = JSON_FILE.format(format=FORMATS[FORMAT])


def entry2issue(
    alias: str, year: str, month: str, entry: dict, base_dir: str, title_ark=None
) -> BnfIssueDir:
    """
    Convert a hierarchical JSON entry into a BnfIssueDir.

    entry example:
      { "day": "15", "edition": "01", "local_path": "..._01" }
    """

    y = int(year)
    m = int(month)
    d = int(entry["day"])

    edition = entry["edition"]

    if entry["ark_id"] in INEXACT_DATE_ISSUES:
        # for incomplete dates, use the secondary date to notify that the date is not exact
        sec_date = year if month == 1 and d == 1 else "-".join([year, month])
    else:
        sec_date = entry.get("secondary_date", None)

    return BnfIssueDir(
        provider="BNF",
        alias=alias,
        date=date(y, m, d),
        edition=edition,
        path=os.path.join(base_dir, entry["local_path"][0]),
        ark_id=entry["ark_id"],
        secondary_date=sec_date,
        batch=entry["batch"],  # batch info will help with knowing the issue directory structure
        title_ark=title_ark,
    )


def detect_issues(
    base_dir: str, alias_filter: list[str] | None = None, exclude_list: list[str] | None = None
) -> list[BnfIssueDir]:
    """Detect BNF issues to import within the filesystem.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to newspaper aliases.
        alias_filter (list[str] | None, optional): Aliases to consider. Defaults to None.
        exclude_list (list[str] | None, optional): Aliases to exclude. Defaults to None.

    Returns:
        list[BnfIssueDir]: List of `BnfIssueDir` instances to import.
    """
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        issues_data = json.load(f)

    with open(ALIAS_TO_ARKS_FILE, "r", encoding="utf-8") as fin:
        arks_per_alias = json.load(fin)

    issues: list[BnfIssueDir] = []

    # Apply alias filters early
    kept_data = issues_data
    if alias_filter:
        kept_data = {a: d for a, d in kept_data.items() if a in alias_filter}
    if exclude_list:
        kept_data = {a: d for a, d in kept_data.items() if a not in exclude_list}

    for alias, years in kept_data.items():
        for year, months in years.items():
            for month, entries in months.items():
                for entry in entries:
                    issue = entry2issue(alias, year, month, entry, base_dir, arks_per_alias[alias])
                    issues.append(issue)

    return issues


def select_issues(base_dir: str, config: dict) -> list[BnfIssueDir] | None:
    """Detect selectively newspaper issues to import.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to newspaper aliases.
        config (dict): Configuration dictionary containing 'aliases', 'exclude_aliases',
            and 'year_only' keys for filtering.

    Returns:
        list[BnfIssueDir] | None: List of `BnfIssueDir` instances to import.
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

    logger.info(f"{len(filtered_issues)} newspaper issues remained after applying filter")

    return filtered_issues
