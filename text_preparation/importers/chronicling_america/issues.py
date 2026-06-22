"""Issue identity helpers and date-range filtering."""

from __future__ import annotations

import os
from datetime import date

from text_preparation.importers.chronicling_america.manifest import batch_version
from text_preparation.importers.chronicling_america.models import IssueInfo, TitleSpec


def issue_in_date_range(issue_date: str, title: TitleSpec) -> bool:
    parsed = date.fromisoformat(issue_date)
    if title.start_date and parsed < title.start_date:
        return False
    if title.end_date and parsed > title.end_date:
        return False
    return True


def issue_dir_name_from_date_edition(issue_date: str, edition: str) -> str:
    year, month, day = issue_date.split("-")
    ed_num = int(edition.replace("ed-", ""))
    return f"{year}{month}{day}{ed_num:02d}"


def issue_info_from_tarball_key(
    batch: str,
    issue_key: str,
    alias_by_lccn: dict[str, str],
) -> IssueInfo:
    lccn, issue_date, edition = issue_key.split("/", 2)
    return IssueInfo(
        batch=batch,
        lccn=lccn,
        alias=alias_by_lccn[lccn],
        date=issue_date,
        edition=edition,
        issue_dir_name=issue_dir_name_from_date_edition(issue_date, edition),
        url="",
    )


def issues_from_tarball_extraction(
    extracted: dict[str, dict[int, bytes]],
    batch: str,
    titles: list[TitleSpec],
    alias_by_lccn: dict[str, str],
) -> list[IssueInfo]:
    title_by_lccn = {title.lccn: title for title in titles}
    issues: list[IssueInfo] = []
    for issue_key in sorted(extracted):
        lccn = issue_key.split("/", 1)[0]
        title = title_by_lccn.get(lccn)
        if title is None:
            continue
        issue = issue_info_from_tarball_key(batch, issue_key, alias_by_lccn)
        if issue_in_date_range(issue.date, title):
            issues.append(issue)
    return issues


def dedupe_issues(issues: list[IssueInfo]) -> list[IssueInfo]:
    best: dict[str, IssueInfo] = {}
    for issue in issues:
        key = f"{issue.lccn}/{issue.date}/{issue.edition}"
        current = best.get(key)
        if current is None:
            best[key] = issue
            continue
        if batch_version(issue.batch) > batch_version(current.batch):
            best[key] = issue
    return sorted(best.values(), key=lambda i: (i.alias, i.date, i.edition))


def issue_local_dir(output_dir: str, issue: IssueInfo) -> str:
    year, month, day = issue.date.split("-")
    return os.path.join(output_dir, issue.alias, year, month, day, issue.edition)


def issue_state_key(issue: IssueInfo) -> str:
    return f"{issue.alias}/{issue.date}/{issue.edition}"
