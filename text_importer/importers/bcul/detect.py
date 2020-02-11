import logging
import os
from collections import namedtuple
from typing import List, Optional

from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter

from text_importer.importers.bcul.helpers import parse_info, find_mit_file

logger = logging.getLogger(__name__)

BCULIssueDir = namedtuple(
        "IssueDirectory", [
            'journal',
            'date',
            'edition',
            'path',
            'rights'
            ]
        )


def dir2issue(path: str, access_rights: dict) -> Optional[BCULIssueDir]:
    """ Creates a `BCULIssueDir` from a directory (RERO format)

    .. note ::
        This function is called internally by :func:`detect_issues`

    :param str path: Path of issue.
    :param dict access_rights: Dictionary for access rights.
    :return: New ``Rero2IssueDir`` object.
    """
    mit_file = find_mit_file(path)
    if mit_file is None:
        logger.error("Could not find MIT file in {}".format(path))
        return None
    date, journal = parse_info(mit_file)
    
    return BCULIssueDir(journal=journal, date=date, edition="a", path=path,
                        rights="open_public")


def detect_issues(base_dir: str, access_rights: str) -> List[BCULIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BCUL used to
    organize the dump of Abbyy

    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Path to ``access_rights.json`` file.
    :return: List of `BCULIssueDir` instances, to be imported.
    """
    
    dir_path, dirs, files = next(os.walk(base_dir))
    issue_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    
    return [dir2issue(_dir, None) for _dir in issue_dirs]


def select_issues(base_dir: str, config: dict, access_rights: str) -> Optional[List[BCULIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :param str access_rights: Path to ``access_rights.json`` file.
    :return: List of `Rero2IssueDir` instances, to be imported.
    """
    
    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config["newspapers"]
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] is missing in the config file.")
        return
    
    issues = detect_issues(base_dir, access_rights)
    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag \
        .filter(lambda i: (len(filter_dict) == 0 or i.journal in filter_dict.keys()) and i.journal not in exclude_list) \
        .compute()
    
    exclude_flag = False if not exclude_list else True
    filtered_issues = _apply_datefilter(filter_dict, selected_issues,
                                        year_only=year_flag) if not exclude_flag else selected_issues
    logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                    len(filtered_issues),
                    filtered_issues
                    )
            )
    return filtered_issues
