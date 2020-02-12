"""Set of helper functions for BNF importer"""
import logging
from datetime import datetime
from typing import List

from text_importer.importers import CONTENTITEM_TYPE_ADVERTISEMENT, CONTENTITEM_TYPE_ARTICLE, CONTENTITEM_TYPE_IMAGE, \
    CONTENTITEM_TYPE_OBITUARY, CONTENTITEM_TYPE_TABLE

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