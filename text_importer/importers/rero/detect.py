import json
import logging
import os
from collections import namedtuple
from datetime import datetime
from typing import List, Optional

from dask import bag as db

from text_importer.utils import get_access_right

logger = logging.getLogger(__name__)

Rero2IssueDir = namedtuple(
        "IssueDirectory", [
                'journal',
                'date',
                'edition',
                'path',
                'rights'
                ]
        )

EDITIONS_MAPPINGS = {
        1: 'a',
        2: 'b',
        3: 'c',
        4: 'd',
        5: 'e'
        }


def dir2issues(path: str, access_rights: dict) -> Rero2IssueDir:
    """ Creates an IssueDir from a directory (RERO format)
    :param path: Path of issue
    :param access_rights: Dictionary for access rights
    :return: IssueDir
    """
    journal, issue = path.split('/')[-2:]
    date, edition = issue.split('_')
    date = datetime.strptime(date, '%Y%m%d').date()
    
    edition = EDITIONS_MAPPINGS[int(edition)]
    
    return Rero2IssueDir(journal=journal, date=date, edition=edition, path=path,
                         rights=get_access_right(journal, date, access_rights))


def detect_issues(base_dir: str, access_rights: str, data_dir: str = 'data') -> List[Rero2IssueDir]:
    """ Parse directory structure and detect newspaper issues to be imported (RERO format)
    :param base_dir: the root of the directory structure
    :param access_rights: The file where access rights are stored
    :param data_dir: Directory where data is stored (usually `data/`)
    :return: list of `IssueDir` instances
    """
    
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    journal_dirs = [
            os.path.join(journal, _dir, _dir2)
            for journal in journal_dirs
            for _dir in os.listdir(journal)
            if _dir == data_dir
            for _dir2 in os.listdir(os.path.join(journal, _dir))
            ]
    
    issues_dirs = [os.path.join(j_dir, l) for j_dir in journal_dirs for l in os.listdir(j_dir)]
    
    with open(access_rights, 'r') as f:
        access_rights_dict = json.load(f)
    
    return [dir2issues(_dir, access_rights_dict) for _dir in issues_dirs]


def select_issues(cfg_file, input_dir, access_rights: str) -> Optional[List[Rero2IssueDir]]:
    """
    
    :param cfg_file:
    :param input_dir:
    :param access_rights:
    :return:
    """
    
    if cfg_file and os.path.isfile(cfg_file):
        logger.info(f"Found config file: {os.path.realpath(cfg_file)}")
        with open(cfg_file, 'r') as f:
            config_dict = json.load(f)
    else:
        logger.info(f"Could not load config file: {os.path.realpath(cfg_file)}")
        return
    
    # read filters from json configuration (see config.example.json)
    try:
        filter_dict = config_dict["newspapers"]
        exclude_list = config_dict["exclude_newspapers"]
        year_flag = config_dict["year_only"]
    
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] is missing in the config file.")
        return
    
    issues = detect_issues(input_dir, access_rights)
    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag \
        .filter(lambda i: (len(filter_dict) == 0 or i.journal in filter_dict.keys()) and i.journal not in exclude_list) \
        .compute()
    
    # TODO : date filter
    
    logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                    len(selected_issues),
                    selected_issues
                    )
            )
    return selected_issues
