"""This module contains helper functions to find KB OCR data to import.
"""
import logging
import os
from collections import namedtuple
from datetime import datetime, timedelta
from string import ascii_lowercase
from typing import Optional

import requests
import pandas as pd
import numpy as np
import string
from bs4 import BeautifulSoup
from dask import bag as db
from impresso_commons.path.path_fs import _apply_datefilter
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

logger = logging.getLogger(__name__)

EDITIONS_MAPPINGS = {
    1: 'a',
    2: 'b',
    3: 'c',
    4: 'd',
    5: 'e',
    6: 'f',
    7: 'g'
}

KbIssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path',
        'rights',
        'identifier'
    ]
)
"""A light-weight data structure to represent a newspaper issue in KB format.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    journal (str): Newspaper ID.
    date (datetime.date): Publication date or issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.
    rights (str): Access rights on the data (open, closed, etc.).
    identifier (str): Unique identifier associated with this issue in KB's API.

>>> from datetime import date
>>> i = KbIssueDir('0001', date(1618,06,14), 'a', './1618/06/14/DDD_ddd_010500649_mpeg21/', 'open', 'DDD:ddd:010500649:mpeg21')
"""

OAI_API_ISSUE_METADATA_URI = "https://services.kb.nl/mdo/oai?verb=GetRecord&identifier={identifier}&metadataPrefix=didl"
API_PAGE_IMG_URI = "http://resolver.kb.nl/resolve?urn={identifier}:{pagenumber}:image"
API_ISSUE_IMGS_URI = "http://resolver.kb.nl/resolve?urn={identifier}:pdf"


def load_issues_index(base_dir: str, basedir_files: list[str]) -> pd.DataFrame:
    index_filename = [f for f in basedir_files if 'index' in f]

    if len(index_filename) == 0:
        logger.critical("No index file was found.")
        raise KeyError
    elif len(index_filename) > 1:
        logger.warning("More than one index file was found.")
        raise KeyError

    index_filename = index_filename[0]
    index_file = os.path.join(base_dir, index_filename)
    cols = ['journal', 'date', 'source_file', 'relative_path']
    index_df = pd.read_csv(index_file, sep = '\t', names=cols)

    # some tsv files can have parsing issues.
    if index_df.isnull().values.any():
        logger.critical("NaN values found in index file.")
    
    # extract the issue identifiers from the paths information
    index_df['issue_identifier'] = index_df['relative_path'].apply(
        lambda x: x.split('/')[-2].replace('_', ':')
    )
    index_df['delpher_issue_identifier'] = index_df['issue_identifier'].apply(
        lambda x: ':'.join(x.split(':')[1:])
    )

    # remove issues which correspond to "No journal"
    index_df = index_df[ index_df['journal'] != '[Zonder titel]']

    # There should be no duplicated issues
    return index_df.drop_duplicates()

def get_or_create_journal_ids(
    index_data: pd.DataFrame | None = None, filepath: str | None = None
) -> dict[str, dict[str, str]]:
    # If the file with the journal->id mapping already exists, use it
    if filepath is not None and os.path.exists(filepath):
        journals_idx_ids = pd.read_csv(filepath, index_col=0)
    else:
        assert index_data is not None, "index_data should be defined if `filepath` is None"
        # otherwise create an internal identifier for each unique journal 
        journals_idx_ids = (index_data[['journal']]
                            .drop_duplicates()
                            .set_index('journal'))
        journals_idx_ids['journal_idx'] = list(map(
            lambda x: str(x).zfill(8), np.arange(1, len(journals_idx_ids)+1)
        ))
        
        # optionally save the newly created index for future runs
        if filepath is not None:
            journals_idx_ids.to_csv(filepath)

    return journals_idx_ids.to_dict('index')

def get_mult_editions_issues(
    grouped_df: pd.DataFrame
) -> dict[tuple[str, str], dict]:
    # only keep entries for journals and date with more than one issue
    journals_by_date = grouped_df[grouped_df['issue_identifier'].map(len) > 1]

    return (journals_by_date[['journal', 'date', 'issue_identifier']]
            .set_index(['journal', 'date']).to_dict('index'))


def identify_mult_editions(
    grpd_index: pd.DataFrame, journal_idx_ids: dict[str, dict]
) -> dict[str, dict]:

    # identifies which journals and dates count multiple issues 
    mult_editions = get_mult_editions_issues(grpd_index)
    logger.info(f"Found {len(mult_editions)} journal-date pairs "
                "with multiple issue editions.")
    
    # add `dates` key for each journal with the issue editions from the same day
    for (j, d),ids in mult_editions.items():
        if 'dates' in journal_idx_ids[j].keys():
            journal_idx_ids[j]['dates'][d] = ids['issue_identifier']
        else:
            journal_idx_ids[j]['dates'] = {d: ids['issue_identifier']}

    return journal_idx_ids


def dir2issue(row: pd.Series, base_dir: str, 
              journals_idx_dict: dict[str], 
              access_rights: dict | None = None) -> Optional[KbIssueDir]:
    
    pub_date = datetime.date.fromisoformat(row['date'])
    # dict mode
    journal = journals_idx_dict[row['journal']]['journal_idx']
    # df mode
    #journals_index.loc[journals_index['journal'] == row['journal'], 'journal_idx'].values[0]
    path = os.path.join(base_dir, row['date'])
    identifier = row['issue_identifier']
    if ('dates' in journals_idx_dict[row['journal']] and 
        row['date'] in journals_idx_dict[row['journal']]['dates']):
        edition_index = (journals_idx_dict[row['journal']]['dates'][row['date']]
                         .index(row['issue_identifier']))
    else:
        edition_index = 0
    edition = string.ascii_lowercase[edition_index]
    if access_rights is not None:
        rights = None
    else:
        rights = 'closed'

    return KbIssueDir(journal=journal, date=pub_date, edition=edition, 
                      path=path, rights=rights, identifier=identifier)


def detect_issues(base_dir: str, access_rights: str) -> list[KbIssueDir]:
    """Detect newspaper issues to import within the filesystem.

    This function expects the directory structure that BNF-EN used to
    organize the dump of Mets/Alto OCR data.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        access_rights (str): Not used for this importer (kept for conformity).

    Returns:
        List[BnfEnIssueDir]: List of `BnfEnIssueDir` instances to import.
    """
    dir_path, dirs, files = next(os.walk(base_dir))

    #TODO add potential filtering of newspapers

    index_df = load_issues_index(base_dir, files)

    # TODO adapt approach once a journal -> id mapping exists
    #journal_ids_filename = 'kb_journal_ids.csv'
    #journal_ids_path = os.path.join(base_dir, journal_ids_filename) 
    journal_idx_ids = get_or_create_journal_ids(index_df, None)

    # group the index of issues by journal and date and add the journal ids 
    grpd_index = index_df.groupby(['journal', 'date']).agg(list).reset_index()
    grpd_index['journal_idx'] = grpd_index['journal'].apply(
        lambda x: journal_idx_ids[x]['journal_idx']
    )

    # identify journal & dates on which multiple editions exist
    j_ids_and_editions = identify_mult_editions(journal_idx_ids, grpd_index)
    
    issue_dirs = index_df.apply(
        lambda row: dir2issue(row, base_dir, j_ids_and_editions), axis=1
    )

    return list(issue_dirs)


def select_issues(
    base_dir: str, config: dict, access_rights: str
) -> Optional[list[KbIssueDir]]:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See `this section <../importers.html#configuration-files>`__ for
    further details on how to configure filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        config (dict): Config dictionary for filtering.
        access_rights (str): Not used for this importer (kept for conformity).

    Returns:
        Optional[list[BnfEnIssueDir]]: `BnfEnIssueDir` instances to import.
    """
    try:
        filter_dict = config["newspapers"]
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] "
                        "is missing in the config file.")
        return
    
    issues = detect_issues(base_dir, access_rights)
    issue_bag = db.from_sequence(issues)
    selected_issues = (
        issue_bag.filter(
            lambda i: (
                len(filter_dict) == 0 or i.journal in filter_dict.keys()
            ) and i.journal not in exclude_list
        ).compute()
    )
    
    exclude_flag = False if not exclude_list else True
    filtered_issues = _apply_datefilter(
        filter_dict, selected_issues, year_only=year_flag
    ) if not exclude_flag else selected_issues
    logger.info(f"{len(filtered_issues)} newspaper issues remained "
                f"after applying filter: {filtered_issues}")
    
    return filtered_issues

