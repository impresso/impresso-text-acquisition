import os
from datetime import date
from collections import namedtuple


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
    """Create a LuxIssueDir object from a directory."""
    issue_dir = os.path.basename(path)
    local_id = issue_dir.split('_')[2]
    issue_date = issue_dir.split('_')[3]
    year, month, day = issue_date.split('-')
    # how many rights still to be discussed
    rights = 'o' if 'public_domain' in path else 'c'
    editions_mappings = {
        1: 'a',
        2: 'b',
        3: 'c',
        4: 'd',
        5: 'e'
    }

    if len(issue_dir.split('_')) == 4:
        edition = 'a'
    elif len(issue_dir.split('_')) == 5:
        edition = issue_dir.split('_')[4]
        edition = editions_mappings[int(edition)]

    return LuxIssueDir(
        local_id,
        date(int(year), int(month), int(day)),
        edition,
        path,
        rights
    )


def detect_issues(base_dir):
    """Parse a directory structure and detect newspaper issues to be imported.

    :param base_dir: the root of the directory structure
    :type base_dir: LuxIssueDir
    :return: list of `LuxIssueDir` instances
    :rtype: list
    """
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
