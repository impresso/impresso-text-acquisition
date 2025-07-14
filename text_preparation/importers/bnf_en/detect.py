"""This module contains helper functions to find BNF-EN OCR data to import."""

import logging
import os
from collections import namedtuple
from datetime import datetime, timedelta
from string import ascii_lowercase
from multiprocessing import Pool
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from dask import bag as db
from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}

BnfEnIssueDir = namedtuple("IssueDirectory", ["alias", "date", "edition", "path", "ark_link"])
"""A light-weight data structure to represent a newspaper issue in BNF Europeana

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
    ark_link (str): Unique IIIF identifier associated with this issue.

>>> from datetime import date
>>> i = BnfEnIssueDir('BLB', date(1845,12,28), 'a', './Le-Gaulois/18820208_1')
"""

API_JOURNAL_URL = "https://gallica.bnf.fr/services/Issues?ark={ark}/date"
API_ISSUE_URL = "https://gallica.bnf.fr/services/Issues?ark={ark}/date&date={year}"
IIIF_URL = "https://gallica.bnf.fr/iiif/ark:/12148/{issue_id}"

API_MAPPING = {
    "oerennes": "cb32830550k",
    "oecaen": "cb41193642z",
    "lematin": "cb328123058",
    "lepji": "cb32836564q",
    "jdpl": "cb39294634r",
    "legaulois": "cb32779904b",
    "lepetitparisien": "cb34419111x",
}


def get_api_id(alias: str, api_issue: tuple[str, datetime.date], edition: str) -> str:
    """Construct an ID given a journal alias, date and edition.

    Args:
        alias (str): Journal alias
        api_issue (tuple[str, datetime.date]): Tuple of information fetched
            from the Gallica API.
        edition (str): Edition of the issue.

    Returns:
        str: Canonical issue Id composed of alias, date and edition.
    """
    date = api_issue[1]
    return "{}-{}-{:02}-{:02}-{}".format(
        alias, date.year, date.month, date.day, ascii_lowercase[edition]
    )


def fix_api_year_mismatch(
    alias: str, year: int, api_issues: list[Tag], last_i: list[Tag | None]
) -> tuple[list[Tag], list[Tag | None]]:
    """Modify proivded list of issues fetched from the API to fix some issues present.

    Indeed, the API currently wronly stores the issues for december 31st of some years,
    with some issues being shifted from one year.
    This is not the case for all years, and the correct issue can be present or not.
    This function aims to rectify this issue and fetch the correct IIIF ark IDs.

    Args:
        alias (str): Alias of the journal currently under processing.
        year (int): Year for which the API was queried.
        api_issues (list[Tag]): List of issues as returned from the API.
        last_i (list[Tag | None]): Last december 31st issue entry, returned for the wrong year.

    Returns:
        tuple[list[Tag], list[Tag | None]: Corrected issue list and next december 31st
            issue(s) if the error was present again, None otherwise.
    """
    curr_last_i = last_i
    # TODO check if correct
    next_last_i = curr_last_i

    # if there is indeed a mismatch in the year
    if str(year - 1) in api_issues[-1].getText():
        if "31 décembre" not in api_issues[-1].getText():
            logger.warning(
                "%s-%s: Mismatch in year for another day!!: %s",
                alias,
                year,
                api_issues[-1],
            )
            next_last_i = [None]
        else:
            # it can happen that there are 2 issues on Dec 31st:
            if str(year - 1) in api_issues[-2].getText():
                # save the last 2 issues
                msg = f"{alias}-{year}: Saving 2 editions for Dec 31st {curr_last_i}"
                logger.info(msg)
                num_to_replace = 2
                next_last_i = api_issues[-num_to_replace:]
            else:
                # store this api_issue for the following year
                num_to_replace = 1
                next_last_i = [api_issues[-num_to_replace]]
            # sanity check that the previously stored value corresponds to the correct year
            if curr_last_i[0] is None:
                msg = (
                    f"{alias}-{year}: No previously stored Dec 31s value since "
                    f"it's the last available year, removing the {num_to_replace} incorrect issue."
                )
                logger.info(msg)
                # if ark is not available: delete the wrong last issue
                api_issues = api_issues[:num_to_replace]
            elif all(str(year) in i.getText() for i in curr_last_i):
                # remove the number of issues of the wrong year
                api_issues = api_issues[:-num_to_replace]
                # replace the final issue by the one with the correct year
                api_issues.extend(curr_last_i)
                msg = f"{alias}-{year}: Setting the value of api_issues[:-{num_to_replace}] to {curr_last_i}"
                logger.debug(msg)
            else:
                msg = (
                    f"{alias}-{year}: The previously stored dec 31st issue does "
                    f"not correspond to this year {curr_last_i}"
                )
                logger.info(msg)
    elif all(str(year) in i.getText() for i in curr_last_i):
        # if the last stored value corresponds to this year and december 31st is missing, add it
        if (
            all("31 décembre" in i.getText() for i in curr_last_i)
            and "31 décembre" not in api_issues[-1].getText()
        ):
            api_issues.extend(curr_last_i)
            msg = f"{alias}-{year}: Appending {curr_last_i} to api_issues."
            logger.info(msg)
            next_last_i = [None]
        # if it's not missing but corresponds to another day, log it
        else:
            msg = f"{alias}-{year}: api_issues[-1] corresponding to another day than the previous one: {api_issues[-1].getText()}"
            logger.warning(msg)

    return api_issues, next_last_i


def get_issues_iiif_arks(journal_ark: tuple[str, str]) -> list[tuple[str, str]]:
    """Given a journal alias and Ark, fetch its issues' Ark in the Gallica API.

    Each fo the Europeana journals have a journal-level Ark id, as well as
    issue-level IIIF Ark ids that can be fetched from the Gallica API using
    the journal Ark.
    The API also provides the day of the year for the corresponding issue.
    Using both information, this function recreates all the issue canonical
    for each collection and maps them to their respective issue IIIF Ark ids.

    Args:
        journal_ark (tuple[str, str]): Pair of journal alias and associated Ark id.

    Returns:
        list[tuple[str, str]]: Pairs of issue canonical Ids and IIIF Ark Ids.
    """
    alias, ark = journal_ark

    def get_date(dayofyear: str, year: int) -> datetime.date:
        """Return the date corresponding to a day of year.

        Args:
            dayofyear (str): Numbered day in a year.
            year (int): Year in question.

        Returns:
            datetime.date: Date corresponding to the day of year.
        """
        start_date = datetime(year=year, month=1, day=1)
        return start_date + timedelta(days=int(dayofyear) - 1)

    print(f"Fetching for {alias}")
    r = requests.get(API_JOURNAL_URL.format(ark=ark), timeout=60)
    years = BeautifulSoup(r.content, "lxml").findAll("year")
    years = [int(x.contents[0]) for x in years]

    links = []
    next_year_last_i = [None]

    # start with the last year
    for year in tqdm(years[::-1]):
        # API requrest
        url = API_ISSUE_URL.format(ark=API_MAPPING[alias], year=year)
        r = requests.get(url, timeout=60)
        api_issues = BeautifulSoup(r.content, "lxml").findAll("issue")

        # fix the problem stemming from the API with dec. 31st being of following year
        if str(year - 1) in api_issues[-1].getText() or (
            next_year_last_i[0] is not None
            and all(str(year) in i.getText() for i in next_year_last_i)
        ):
            logger.debug(
                "%s-%s: api_issues[-1].getText(): %s, next_year_last_i: %s",
                alias,
                year,
                api_issues[-1].getText(),
                next_year_last_i,
            )
            api_issues, next_year_last_i = fix_api_year_mismatch(
                alias, year, api_issues, next_year_last_i
            )
        else:
            # reset the value since it won't be valid anymore
            next_year_last_i = [None]

        # Parse dates and editions
        api_issues = [(i.get("ark"), get_date(i.get("dayofyear"), year)) for i in api_issues]

        editions = []
        for i, issue in enumerate(api_issues):
            if i == 0:
                editions.append(0)
            else:
                previous_same = api_issues[i - 1][1] == issue[1]
                if previous_same:
                    editions.append(editions[-1] + 1)
                else:
                    editions.append(0)

        api_issues = [
            (get_api_id(alias, i, edition), i[0]) for i, edition in zip(api_issues, editions)
        ]
        links += api_issues[::-1]
    # flip the resulting links since they were fetched from end to start
    return links[::-1]


def construct_iiif_arks() -> dict[str, str]:
    """Fetch the IIIF ark ids for each issue and map them to each other.

    Returns:
        dict[str, str]: Mapping from issue canonical id to IIIF Ark id.
    """
    with Pool(4) as p:
        results = p.map(get_issues_iiif_arks, list(API_MAPPING.items()))

    iiif_arks = []
    for i in results:
        iiif_arks += i
    return dict(iiif_arks)


def get_id(alias: str, date: datetime.date, edition: str) -> str:
    """Construct the canonical issue ID given the necessary information.

    Args:
        alias (str): Journal alias.
        date (datetime.date): Publication date.
        edition (str): Edition of the issue.

    Returns:
        str: Resulting issue canonical Id.
    """
    return "{}-{}-{:02}-{:02}-{}".format(alias, date.year, date.month, date.day, edition)


def parse_dir(_dir: str, alias: str) -> str:
    """Parse a directory and return the corresponding ID.

    Args:
        _dir (str): The directory (in Windows FS).
        alias (str): Journal alias to construct ID.

    Returns:
        str: Issue canonical id.
    """
    date_edition = _dir.split("\\")[-1].split("_")
    if len(date_edition) == 1:
        edition = "a"
        date = date_edition[0]
    else:
        date = date_edition[0]
        edition = EDITIONS_MAPPINGS[int(date_edition[1])]
    year, month, day = date[:4], date[4:6], date[6:8]
    return "{}-{}-{}-{}-{}".format(alias, year, month, day, edition)


def dir2issue(path: str, iiif_arks: dict[str, str]) -> BnfEnIssueDir | None:
    """Create a `BnfEnIssueDir` object from a directory path.

    Note:
        This function is called internally by :func:`detect_issues`.

    Args:
        path (str): Path of issue.
        access_rights (dict): Access rights (for conformity).
        iiif_arks (dict): Mapping from issue canonical ids to iiif ark ids.

    Returns:
        BnfEnIssueDir | None: `BnfEnIssueDir` for given issue if the ark id
            was found on the Gallica API, None otherwise.
    """
    alias, issue = path.split("/")[-2:]

    date, edition = issue.split("_")[:2]
    date = datetime.strptime(date, "%Y%m%d").date()
    alias = alias.lower().replace("-", "").strip()
    edition = EDITIONS_MAPPINGS[int(edition)]

    id_ = get_id(alias, date, edition)

    if id_ not in iiif_arks:
        return None

    return BnfEnIssueDir(
        alias=alias,
        date=date,
        edition=edition,
        path=path,
        ark_link=iiif_arks[id_],
    )


def detect_issues(base_dir: str) -> list[BnfEnIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNF-EN used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data.

    Returns:
        list[BnfEnIssueDir]: List of `BnfEnIssueDir` instances to import.
    """
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    issue_dirs = [os.path.join(alias, _dir) for alias in journal_dirs for _dir in os.listdir(alias)]

    iiif_arks = construct_iiif_arks()
    issue_dirs = [dir2issue(_dir, iiif_arks) for _dir in issue_dirs]

    initial_length = len(issue_dirs)
    issue_dirs = [i for i in issue_dirs if i is not None]
    logger.info("Removed %s problematic issues", initial_length - len(issue_dirs))

    return issue_dirs


def select_issues(base_dir: str, config: dict) -> list[BnfEnIssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.

    Returns:
        list[BnfEnIssueDir] | None: `BnfEnIssueDir` instances to import.
    """
    try:
        filter_dict = config["titles"]
        exclude_list = config["exclude_titles"]
        year_flag = config["year_only"]

    except KeyError:
        logger.critical(
            "The key [titles|exclude_titles|year_only] " "is missing in the config file."
        )
        return None

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
