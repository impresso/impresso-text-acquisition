"""This module contains helper functions to find SWA OCR data to be imported."""

import json
import logging
import os
from collections import namedtuple
from datetime import date
from typing import Optional

import dask.bag as db
import pandas as pd

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

SwaIssueDir = namedtuple(
    "IssueDirectory",
    [
        "alias",
        "date",
        "edition",
        "path",
        "pages",
    ],
)
"""A light-weight data structure to represent a SWA newspaper issue.

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
    pages (list): list of tuples (page_canonical_id, alto_path), alto_path is
        the path from within the archive.

>>> from datetime import date
>>> i = IssueDirectory(
        alias='arbeitgeber', 
        date=datetime.date(1908, 7, 4), 
        edition='a', 
        path='./SWA/impresso_ocr/schwar_000059110_DSV01_1908.zip',
        pages=[(
            'arbeitgeber-1908-07-04-a-p0001', 
            'schwar_000059110_DSV01_1908/ocr/schwar_000059110_DSV01_1908_alto/BAU_1_000059110_1908_0001.xml'
        ), ...]
    )
"""


def _get_csv_file(directory: str) -> str:
    """Detect and return the path to the CSV file in the given directory.

    In SWA format, a mapping from the original IDs to the impresso ones is
    required, and provided in a single CSV file.

    Args:
        directory (str): directory in which the CSV file should be.

    Raises:
        ValueError: No CSV file was found in the directory.
        ValueError: Multiple CSV files were found in the directory.

    Returns:
        str: Path of the the CSV file found.
    """
    files = os.listdir(directory)
    files = [f for f in files if f.endswith(".csv")]
    if len(files) == 0:
        raise ValueError(f"Could not find csv file in {directory}")
    elif len(files) > 1:
        raise ValueError(f"Found multiple csv files in {directory}")

    return os.path.join(directory, files[0])


def _parse_csv_apply(part: pd.DataFrame) -> pd.Series:
    """Helper to parse csv document, meant to be used on a grouped partition.

    For all the pages in the current issue, matches the pages' canonical ID to
    the corresponding XML file.

    Note:
        This function is called internally by :func:`detect_issues`.

    Args:
        part (pd.DataFrame): Partition for current issue.

    Returns:
        pd.Series: Pages and archives for the issue.
    """
    res = []
    archives = set()
    for i, x in part.iterrows():
        res.append((x.identifier_impresso, x.full_xml_path))
        archives.add(x.goobi_name + ".zip")
    return pd.Series({"pages": res, "archives": archives})


def _get_issuedir(row: pd.Series, journal_root: str) -> Optional[SwaIssueDir]:
    """Create a ``SwaIssueDir`` from a row of the CSV file.

    Each row of the CSV file contains the issue manifest ID, used to derive
    the issue canonical ID, and the path the XML files of the issue's pages.

    Note:
        This function is called internally by :func:`detect_issues`.

    Args:
        row (pd.Series): Row of the CSV file  to an issue.
        journal_root (str): Path to archive for current issue.
        access_rights (dict): access rights of the issue's journal.

    Returns:
        Optional[SwaIssueDir]: New `SwaIssueDir` object for the issue.
    """
    if len(row.archives) > 1:
        logger.debug("Issue %s has more than one archive %s", row.manifest_id, row.archives)

    archive = os.path.join(journal_root, list(row.archives)[0])

    if os.path.isfile(archive):
        split = row.manifest_id.split("-")[:-1]
        if len(split) == 5:
            try:
                alias, year, mo, day, edition = split
                pub_date = date(int(year), int(mo), int(day))
                return SwaIssueDir(
                    alias,
                    pub_date,
                    edition,
                    path=archive,
                    pages=row.pages,
                )
            except ValueError as e:
                logger.debug("Issue %s does not have a regular name: %s", row.manifest_id, e)
        else:
            logger.debug("Issue %s does not have regular name", row.manifest_id)
    else:
        logger.debug("Issue %s does not have archive", row.manifest_id)
    return None


def detect_issues(base_dir: str) -> list[SwaIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that SWA used to
    organize the dump of Alto OCR data.

    The access rights information is not in place yet, but needs
    to be specified by the content provider (SWA).

    TODO: Add the directory structure of SWA OCR data dumps.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[SwaIssueDir]: list of ``SwaIssueDir`` instances, to be imported.
    """
    dir_path, dirs, _ = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    journal_dirs = [(d, _get_csv_file(d)) for d in journal_dirs]

    results = []
    for journal_root, csv_file in journal_dirs:
        if os.path.isfile(csv_file):
            df = pd.read_csv(csv_file).groupby("manifest_id").apply(_parse_csv_apply).reset_index()
            result = df.apply(lambda r: _get_issuedir(r, journal_root), axis=1)
            result = result[~result.isna()].values.tolist()
            results += result
        else:
            logger.warning("Could not find csv file %s", csv_file)

    return results


def select_issues(base_dir: str, config: dict) -> list[SwaIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

        The access rights information is not in place yet, but needs
        to be specified by the content provider (SWA).

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.
        access_rights (str): Path to ``access_rights.json`` file.

    Returns:
        list[SwaIssueDir]: list of ``SwaIssueDir`` instances, to be imported.
    """
    try:
        filter_dict = config.get("titles")
        exclude_list = config["exclude_titles"]
        year_flag = config["year_only"]
    except KeyError:
        logger.critical(
            "The key [titles|exclude_titles|year_only] " "is missing in the config file."
        )
        return []

    exclude_flag = False if not exclude_list else True
    msg = (
        f"got filter_dict: {filter_dict}, "
        f"\nexclude_list: {exclude_list}, "
        f"\nyear_flag: {year_flag}"
        f"\nexclude_flag: {exclude_flag}"
    )
    logger.debug(msg)

    filter_titles = set(filter_dict.keys()) if not exclude_list else set(exclude_list)
    logger.debug(
        "got filter_titles: %s, with exclude flag: %s",
        filter_titles,
        exclude_flag,
    )

    issues = detect_issues(base_dir)

    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag.filter(
        lambda i: (len(filter_dict) == 0 or i.alias in filter_dict.keys())
        and i.alias not in exclude_list
    ).compute()

    logger.info(
        "%s newspaper issues remained after applying filter: %s",
        len(selected_issues),
        selected_issues,
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

    return selected_issues
