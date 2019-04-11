import os
from dask import bag as db
import json
import logging
from datetime import date
from collections import namedtuple

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {
    1: 'a',
    2: 'b',
    3: 'c',
    4: 'd',
    5: 'e'
}


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

    if len(issue_dir.split('_')) == 4:
        edition = 'a'
    elif len(issue_dir.split('_')) == 5:
        edition = issue_dir.split('_')[4]
        edition = EDITIONS_MAPPINGS[int(edition)]

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


def select_issues(cfg_file, input_dir):
    # detect/select issues
    if cfg_file and os.path.isfile(cfg_file):

        logger.info(f"Found config file: {os.path.realpath(cfg_file)}")
        with open(cfg_file, 'r') as f:
            config = json.load(f)

        issues = detect_issues(input_dir)
        issue_bag = db.from_sequence(issues)
        selected_issues = issue_bag\
            .filter(lambda i: i.journal in config['newspapers'].keys())\
            .compute()

        logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                len(selected_issues),
                selected_issues
            )
        )
    return selected_issues
