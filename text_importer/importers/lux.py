"""Importer for the newspapers data of the Luxembourg National Library"""

from collections import namedtuple
from datetime import date
import os


LuxIssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path',
        'rights'
    ]
)


def dir2issue(path):
    issue_dir = os.path.basename(path)
    local_id = issue_dir.split('_')[2]
    issue_date = issue_dir.split('_')[3]
    year, month, day = issue_date.split('-')
    rights = 'o' if 'public_domain' in path else 'c'

    if len(issue_dir.split('_')) == 4:
        edition = 'a'
    elif len(issue_dir.split('_')) == 5:
        edition = issue_dir.split('_')[4]

    return LuxIssueDir(
        local_id,
        date(int(year), int(month), int(day)),
        edition,
        path,
        rights
    )


def detect_issues(base_dir):
    """TODO."""
    dir_path, dirs, files = next(os.walk(base_dir))
    batches_dirs = [os.path.join(dir_path, dir) for dir in dirs]
    issue_dirs = [
        os.path.join(batch_dir, dir)
        for batch_dir in batches_dirs
        for dir in os.listdir(batch_dir)
        if 'newspaper' in dir
    ]
    return [
        dir2issue(dir)
        for dir in issue_dirs
    ]
