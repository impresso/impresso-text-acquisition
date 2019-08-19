import logging
import os
from collections import namedtuple
from datetime import date
from typing import List, Optional

import dask.bag as db
import pandas as pd
from impresso_commons.path.path_fs import _apply_datefilter

logger = logging.getLogger(__name__)

SwaIssueDir = namedtuple(
        "IssueDirectory", [
                'journal',
                'date',
                'edition',
                'path',
                'rights',
                'pages',
                ]
        )
"""A light-weight data structure to represent a SWA newspaper issue.

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
:param str path: Path to the archive holding issue's data
:param str rights: Access rights on the data (open, closed, etc.)
:param list pages: List of tuples (page_canonical_id, alto_path), alto_path is the path from within the archive

>>> from datetime import date
>>> i = IssueDirectory(journal='arbeitgeber', date=datetime.date(1908, 7, 4), edition='a', path='./SWA/impresso_ocr/schwar_000059110_DSV01_1908.zip', rights='open_public', \
                        pages=[('arbeitgeber-1908-07-04-a-p0001', 'schwar_000059110_DSV01_1908/ocr/schwar_000059110_DSV01_1908_alto/BAU_1_000059110_1908_0001.xml'), ...])
"""


def _apply(part: pd.DataFrame) -> pd.Series:
    """
    Helper to parse csv document
    
    :param pd.Dataframe part: Partition for current issue
    :return: pd.Series: Holding the pages and archives for issue
    """
    res = []
    archives = set()
    for i, x in part.iterrows():
        res.append((x.identifier_impresso, x.full_xml_path))
        archives.add(x.goobi_name + '.zip')
    return pd.Series({'pages': res, 'archives': archives})


def get_issuedir(row: pd.Series, archives_full_dir: str) -> Optional[SwaIssueDir]:
    """ Creates a `SwaIssueDir` from a row of the csv file

    .. note ::
        This function is called internally by :func:`detect_issues`

    :param pd.Series row: Path of issue.
    :param str archives_full_dir: Path to archive for current issue
    :return: New ``SwaIssueDir`` object.
    """
    if len(row.archives) > 1:
        logger.debug(f"Issue {row.manifest_id} has more than one archive {row.archives}")
    
    archive = os.path.join(archives_full_dir, list(row.archives)[0])
    if os.path.isfile(archive):
        split = row.manifest_id.split('-')[:-1]
        if len(split) == 5:
            try:
                journal, year, mo, day, edition = split
                d = date(int(year), int(mo), int(day))
                return SwaIssueDir(journal, date=d, edition=edition, path=archive, rights='open_public',
                                   # TODO: ask about rights
                                   pages=row.pages)
            except ValueError as e:
                logger.debug(f"Issue {row.manifest_id} does not have a regular name")
        else:
            logger.debug(f"Issue {row.manifest_id} does not have regular name")
    else:
        logger.debug(f"Issue {row.manifest_id} does not have archive")
    return None


def detect_issues(base_dir: str, access_rights: str, csv_file: str = 'impresso_ids.zip',
                  archives_dir: str = 'impresso_ocr') -> List[SwaIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that RERO used to
    organize the dump of Mets/Alto OCR data.

    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Path to ``access_rights.json`` file.
    :param str csv_file: Compressed csv file holding Issue and page identifiers with alto paths (impresso_ids.zip as default)
    :param str archives_dir: Directory where archives are stored (impresso_ocr/ as default)
    :return: List of `SwaIssueDir` instances, to be imported.
    """
    
    archives_full_dir = os.path.join(base_dir, archives_dir)
    csv_file = os.path.join(base_dir, csv_file)
    result = []
    if os.path.isfile(csv_file):
        df = pd.read_csv(csv_file, compression='zip').groupby('manifest_id').apply(_apply).reset_index()
        result = df.apply(lambda r: get_issuedir(r, archives_full_dir), axis=1)
        result = result[~result.isna()].values
    else:
        logger.warning(f"Could not find csv file {csv_file}")
    return result


def select_issues(base_dir: str, config: dict, access_rights: str) -> List[SwaIssueDir]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    :param str base_dir: Path to the base directory of newspaper data.
    :param dict config: Config dictionary for filtering.
    :param str access_rights: Path to ``access_rights.json`` file.
    :return: List of `SwaIssueDir` instances, to be imported.
    """
    
    try:
        filter_dict = config.get("newspapers")
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] is missing in the config file.")
        return []
    
    exclude_flag = False if not exclude_list else True
    logger.debug(f"got filter_dict: {filter_dict}, "
                 f"\nexclude_list: {exclude_list}, "
                 f"\nyear_flag: {year_flag}"
                 f"\nexclude_flag: {exclude_flag}")
    
    filter_newspapers = set(filter_dict.keys()) if not exclude_list else set(exclude_list)
    logger.debug(f"got filter_newspapers: {filter_newspapers}, with exclude flag: {exclude_flag}")
    issues = detect_issues(base_dir, access_rights)
    
    issue_bag = db.from_sequence(issues)
    selected_issues = issue_bag \
        .filter(lambda i: (len(filter_dict) == 0 or i.journal in filter_dict.keys()) and i.journal not in exclude_list) \
        .compute()
    
    logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                    len(selected_issues),
                    selected_issues
                    )
            )
    
    exclude_flag = False if not exclude_list else True
    filtered_issues = _apply_datefilter(filter_dict, selected_issues,
                                        year_only=year_flag) if not exclude_flag else selected_issues
    logger.info(
            "{} newspaper issues remained after applying filter: {}".format(
                    len(filtered_issues),
                    filtered_issues
                    )
            )
    return selected_issues
