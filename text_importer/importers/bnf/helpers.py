"""Set of helper functions for BNF importer"""
import logging
from datetime import datetime
from typing import List, Optional, Tuple

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


def is_multi_date(date_string: str) -> bool:
    """ Checks whether a given date string is composed of more than one date
    
    :param str date_string:
    :return: True if the string represents multiple dates
    """
    return len(date_string) > 10


def get_dates(date_string: str, separators: List[str]) -> List[Optional[str]]:
    """ Assumes that the given date string represents exactly 2 dates.
    Tries to separate them using the given separators, and returns when it has found two dates.
    
    If the function does not find exactly two dates, it returns None
    
    :param str date_string: The date string
    :param list[str] separators: The list of potential separators
    :return:
    """
    for s in separators:
        if len(date_string.split(s)) == 2:
            return date_string.split(s)
    return [None, None]


def parse_date(date_string: str, formats: List[str], separators: List[str]) -> Tuple[datetime.date, datetime.date]:
    """ Parses a date given a list of formats
    
    :param date_string:
    :param formats:
    :param separators:
    :return:
    """
    date, secondary = None, None
    ds_1, ds_2 = None, None  # Date_string 1 and 2
    
    # Dates have at least 10 characters. Some (very rarely) issues have only year/month
    if len(date_string) < 10:
        raise ValueError("Could not parse date {}".format(date_string))
    elif is_multi_date(date_string):  # Here we potentially have two dates, take the first one
        logger.info(f"Got two dates {date_string}")
        ds_1, ds_2 = get_dates(date_string, separators)
        
        if ds_1 is None:
            raise ValueError("Could not parse date {}".format(date_string))
    else:
        ds_1 = date_string
    
    # Now parse
    for f in formats:
        try:
            date = datetime.strptime(ds_1, f).date()
            if ds_2 is not None:
                secondary = datetime.strptime(ds_2, f).date()
        except ValueError as e:
            pass
    
    if date is None:
        raise ValueError("Could not parse date {}".format(date_string))
    return date, secondary
