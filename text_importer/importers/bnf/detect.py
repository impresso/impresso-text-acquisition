import logging
import os
from collections import namedtuple
from datetime import datetime
from typing import List, Optional
from zipfile import ZipFile

from bs4 import BeautifulSoup
from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter

from text_importer.importers.mets_alto.mets import get_dmd_sec

logger = logging.getLogger(__name__)

BnfIssueDir = namedtuple(
        "IssueDirectory", [
            'journal',
            'date',
            'edition',
            'path',
            'rights'
        ]
)
"""A light-weight data structure to represent a newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

.. note ::

    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

:param str journal: Newspaper ID
:param datetime.date date: Publication date
:param str edition: Edition of the newspaper issue ('a', 'b', 'c', etc.)
:param str path: Path to the archive containing OCR and OLR data
:param str rights: Access rights on the data (open, closed, etc.)

>>> from datetime import date
>>> i = BnfIssueDir(journal='Marie-Claire', date=datetime.date(1938, 3, 11), edition='a', path='./BNF/files/4701034.zip', rights='open_public')
"""


def dir2issue(archive_path: str, date_format: str = "%Y-%m-%d") -> BnfIssueDir:
    """ Creates a `BnfIssueDir` object from an archive path
    
    .. note ::
        This function is called internally by `detect_issues`
    
    :param str archive_path: Path of issue archive
    :param str date_format: Date formatting (default `%Y-%m-%d`
    :return: New ``BnfIssueDir`` object
    """
    archive = ZipFile(archive_path)
    issue_id = os.path.splitext(os.path.basename(archive_path))[0]
    manifest_file = os.path.join(issue_id, "manifest.xml")

    issue = None
    if manifest_file in archive.namelist():
        manifest = BeautifulSoup(archive.open(manifest_file).read(), "xml")
        try:
            issue_info = get_dmd_sec(manifest, 2)  # Issue info is in dmdSec of id 2
            journal = issue_info.find("publisher").contents[0].replace("-", "").lower()
            np_date = datetime.strptime(issue_info.find("date").contents[0], date_format).date()
            edition = "a"
            rights = "open_public"
            issue = BnfIssueDir(journal, np_date, edition, archive_path, rights)
        except Exception as e:
            logger.error(f"Got error {e} when importing {archive_path}")
    else:
        logger.warning(f"Could not find manifest in {archive_path}")
    return issue


def detect_issues(base_dir: str, access_rights: str = None):
    """ Detects BNF issues to import within the filesystem
    
    This function the directory structure used by BNF (one subdir by journal)
    
    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfIssueDir` instances, to be imported.
    """
    dir_path, dirs, files = next(os.walk(base_dir))
    batches_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]

    issue_archives = [
        os.path.join(batch_dir, file)
        for batch_dir in batches_dirs
        for file in os.listdir(batch_dir)
        if file.endswith(".zip")
    ]

    issue_dirs = [dir2issue(path) for path in issue_archives]
    issue_dirs = [issue for issue in issue_dirs if issue is not None]
    return issue_dirs


def select_issues(base_dir: str, config: dict, access_rights: str) -> Optional[List[BnfIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfIssueDir` instances, to be imported.
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
