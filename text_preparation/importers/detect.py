"""Code for parsing impresso's canonical directory structures."""

import os
import logging
from typing import Any
from datetime import date, datetime
from impresso_essentials.utils import IssueDir, KNOWN_JOURNALS

logger = logging.getLogger(__name__)


def _apply_datefilter(
    filter_dict: dict[str, Any], issues: list[IssueDir], year_only: bool
) -> list[IssueDir]:
    """Apply the provided date-filter when selecting the issues to import.

    Args:
        filter_dict (dict[str, Any]): Dates to consider for each title.
        issues (list[IssueDir]): List of detected issues.
        year_only (bool): Whether to filter only based on the year basis.

    Returns:
        list[IssueDir]: List of filtered issues based on their dates.
    """
    filtered_issues = []

    for newspaper, dates in filter_dict.items():
        # date filter is a range
        if isinstance(dates, str):
            start, end = dates.split("-")
            start = datetime.strptime(start, "%Y/%m/%d").date()
            end = datetime.strptime(end, "%Y/%m/%d").date()

            if year_only:
                filtered_issues += [
                    i
                    for i in issues
                    if i.journal == newspaper and start.year <= i.date.year <= end.year
                ]
            else:
                filtered_issues += [
                    i
                    for i in issues
                    if i.journal == newspaper and start <= i.date <= end
                ]

        # date filter is not a range
        elif isinstance(dates, list):
            if not dates:
                filtered_issues += [i for i in issues if i.journal == newspaper]
            else:
                filter_date = [
                    (
                        datetime.strptime(d, "%Y/%m/%d").date().year
                        if year_only
                        else datetime.strptime(d, "%Y/%m/%d").date()
                    )
                    for d in dates
                ]

                if year_only:
                    filtered_issues += [
                        i
                        for i in issues
                        if i.journal == newspaper and i.date.year in filter_date
                    ]
                else:
                    filtered_issues += [
                        i
                        for i in issues
                        if i.journal == newspaper and i.date in filter_date
                    ]

    return filtered_issues


def select_issues(config_dict, inp_dir):
    """Reads a configuration file and select newspapers/issues to consider
    See config.example.md for explanations.

    Usage example:
        if config_file and os.path.isfile(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                issues = select_issues(config, inp_dir)
            else:
                issues = detect_issues(inp_dir)

    :param config_dict: dict of newspaper filter parameters
    :type config_dict: dict
    :param inp_dir: base dit where to get the issues from
    :type inp_dir: str
    """
    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config_dict.get("newspapers")
        exclude_list = config_dict["exclude_newspapers"]
        year_flag = config_dict["year_only"]
    except KeyError:
        logger.critical(
            "The key [newspapers|exclude_newspapers|year_only] is missing in the config file."
        )
        return
    exclude_flag = False if not exclude_list else True
    msg = (
        f"got filter_dict: {filter_dict}, "
        f"\nexclude_list: {exclude_list}, "
        f"\nyear_flag: {year_flag}"
        f"\nexclude_flag: {exclude_flag}"
    )
    logger.debug(msg)

    # detect issues to be imported
    if filter_dict or exclude_list:
        msg = "Calling `detect_issues` with no filter_dict or exclude_list!!"
        logger.error(msg)
        raise AttributeError(msg)
    else:
        filter_newspapers = (
            set(filter_dict.keys()) if not exclude_list else set(exclude_list)
        )
        logger.debug(
            "got filter_newspapers: %s, with exclude flag: %s",
            filter_newspapers,
            exclude_flag,
        )
        issues = detect_issues(
            inp_dir, journal_filter=filter_newspapers, exclude=exclude_flag
        )

        # apply date filter if not exclusion mode
        filtered_issues = (
            _apply_datefilter(filter_dict, issues, year_only=year_flag)
            if not exclude_flag
            else issues
        )
        return filtered_issues


def detect_issues(
    base_dir: str,
    journal_filter: list[str] | None = None,
    exclude: bool = False,
    w_edition: bool = False,
) -> list[IssueDir]:
    """Parse a directory structure and detect newspaper issues to be imported.

    Note:
        Invalid directories are skipped, and a warning message is logged.

    Note:
        This function can be used to identify issues to import within a directory and
        to identify already imported issues, based on the parameters.
        When identifing already imported issues `journal_filter` and `exclude` don't
        need to be specified but `w_edition` should be set to True.

    Args:
        base_dir (str): The root of the directory structure.
        journal_filter (list[str] | None, optional): List of newspaper to filter
            (positive or negative). Defaults to None.
        exclude (bool, optional): Whether journal_filter is positive or negative.
            Defaults to False.
        w_edition (bool, optional): Whether to include the editions in the search.
            Defaults to False.

    Returns:
        list[IssueDir]: Detected issues present in the `base_dir` or child dirs.
    """
    detected_issues = []
    dir_path, dirs, files = next(os.walk(base_dir))
    # workaround to deal with journal-level folders like: 01_GDL, 02_GDL
    if journal_filter is None:
        journal_dirs = [d for d in dirs if d.split("_")[-1] in KNOWN_JOURNALS]
    else:
        if not exclude:
            filtrd_journals = list(set(KNOWN_JOURNALS).intersection(journal_filter))
        else:
            filtrd_journals = list(set(KNOWN_JOURNALS).difference(journal_filter))
        journal_dirs = [d for d in dirs if d.split("_")[-1] in filtrd_journals]

    for journal in journal_dirs:
        journal_path = os.path.join(base_dir, journal)
        journal = journal.split("_")[-1] if "_" in journal else journal
        _, year_dirs, _ = next(os.walk(journal_path))

        for year in year_dirs:
            year_path = os.path.join(journal_path, year)
            _, month_dirs, _ = next(os.walk(year_path))

            for month in month_dirs:
                month_path = os.path.join(year_path, month)
                _, day_dirs, _ = next(os.walk(month_path))

                for day in day_dirs:
                    day_path = os.path.join(month_path, day)

                    # if including the edition in the issueDir path.
                    if w_edition:
                        _, edition_dirs, _ = next(os.walk(day_path))
                        for edition in edition_dirs:
                            edition_path = os.path.join(day_path, edition)
                            try:
                                detected_issue = IssueDir(
                                    journal,
                                    date(int(year), int(month), int(day)),
                                    edition,
                                    edition_path,
                                )
                                print("Found an issue: %s", str(detected_issue))
                                detected_issues.append(detected_issue)
                            except ValueError:
                                print(
                                    "Path %s is not a valid issue directory", day_path
                                )
                    else:
                        try:
                            # concerning `edition="a"`: for now, no cases of newspapers
                            # published more than once a day in Olive format (but it
                            # may come later on)
                            # TODO correct in the future when working with dir structures like this
                            detected_issue = IssueDir(
                                journal,
                                date(int(year), int(month), int(day)),
                                "a",
                                day_path,
                            )
                            print("Found an issue: %s", str(detected_issue))
                            detected_issues.append(detected_issue)
                        except ValueError:
                            print("Path %s is not a valid issue directory", day_path)
    return detected_issues


def get_access_right(
    journal: str, _date: date, access_rights: dict[str, dict[str, str]]
) -> str:
    """Fetch the access rights for a specific journal and publication date.

    Note:
        With the new approach to access rights management, the access rights
        won't be in the canonical data anymore.

    Args:
        journal (str): Journal name.
        _date (date): Publication date of the journal
        access_rights (dict[str, dict[str, str]]): Access rights for various
            journals.

    Returns:
        str: Access rights for specific journal issue.
    """
    rights = access_rights[journal]
    if rights["time"] == "all":
        return rights["access-right"].replace("-", "_")

    # TODO: this should rather be a custom exception
    logger.warning("Access right not defined for %s-%s", journal, _date)

    return "undefined"
