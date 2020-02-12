"""Set of helper functions for BNF importer"""
import logging
import os
from datetime import datetime
from typing import List
from zipfile import ZipFile

from impresso_commons.path import IssueDir
from impresso_commons.path.path_fs import canonical_path

from text_importer.importers import CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPE_TABLE, CONTENTITEM_TYPE_OBITUARY, \
    CONTENTITEM_TYPE_ADVERTISEMENT, CONTENTITEM_TYPE_ARTICLE
from text_importer.importers.classes import ZipArchive

BNF_CONTENT_TYPES = ["article", "advertisement", "illustration", "ornament", "freead",
                     "table"]  # BNF types that do not have a direct `area` descendant
SECTION = "section"
"""Content types as defined in BNF Mets flavour. These are the ones we are interested in parsing.
    The `SECTION` type should be flattened, and should not be part of content items, but it is needed to parse what's
     inside.
"""

type_translation = {
    'illustration': CONTENTITEM_TYPE_IMAGE,
    'advertisement': CONTENTITEM_TYPE_ADVERTISEMENT,
    'ornament': CONTENTITEM_TYPE_OBITUARY,
    'table': CONTENTITEM_TYPE_TABLE,
    'article': CONTENTITEM_TYPE_ARTICLE,
    'freead': CONTENTITEM_TYPE_ADVERTISEMENT
    }

logger = logging.getLogger(__name__)


def add_div(_dict: dict, _type: str, div_id: str, label: str) -> dict:
    """Adds a div item to the given dictionary (sorted by type). The types should be in `BNF_CONTENT_TYPES` or `SECTION`

    :param dict _dict: The dictionary where to add the div
    :param str _type: The type of the div
    :param str div_id: The div ID
    :param str label: The label of the div
    :return: The updated dictionary
    """
    if _type in BNF_CONTENT_TYPES or _type == SECTION:
        if _type in _dict:
            _dict[_type].append((div_id, label))
        else:
            _dict[_type] = [(div_id, label)]
    else:
        logger.warning(f"Tried to add div of type {_type}")
    return _dict


def extract_bnf_archive(dest_dir: str, issue_dir: IssueDir) -> ZipArchive:
    """Extracts the archive of the given BNFIssueDir into the destination dir

    :param str dest_dir: The destination directory
    :param IssueDir issue_dir: The IssueDir of the BNF issue
    :return: ZipArchive: Object used to read the extracted data
    """
    issue_id = canonical_path(issue_dir, path_type='dir').replace('/', '-')
    if os.path.isfile(issue_dir.path):
        archive_tmp_path = os.path.join(
                dest_dir,
                canonical_path(issue_dir, path_type='dir')
                )
        
        try:
            archive = ZipFile(issue_dir.path)
            
            logger.debug((
                f"Contents of archive for {issue_id}:"
                f" {archive.namelist()}"
            ))
            return ZipArchive(archive, archive_tmp_path)
        except Exception as e:
            msg = f"Bad Zipfile for {issue_id}, failed with error : {e}"
            raise ValueError(msg)
    else:
        msg = f"Could not find archive {issue_dir.path} for {issue_id}"
        raise ValueError(msg)


def get_journal_name(archive_path):
    """ Returns the Journal name from the path of the issue. It assumes the journal name is one directory above the issue
    
    :param archive_path:
    :return:
    """
    journal = archive_path.split('/')[-2].split('-')
    journal = "".join(journal).lower()
    return journal


def parse_date(date_string: str, formats: List[str]) -> datetime.date:
    """ Parses a date given a list of formats
    
    :param date_string:
    :param formats:
    :return:
    """
    date = None
    for f in formats:
        try:
            date = datetime.strptime(date_string, f).date()
        except ValueError as e:
            pass
    if date is None:
        raise ValueError("Could not parse date {}".format(date_string))
    return date
