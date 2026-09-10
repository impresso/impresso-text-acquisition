"""Set of helper functions for BNF importer"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

from text_preparation.importers import (
    CONTENTITEM_TYPE_ADVERTISEMENT,
    CONTENTITEM_TYPE_ARTICLE,
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_OBITUARY,
    CONTENTITEM_TYPE_TABLE,
)

# BNF types that do not have a direct `area` descendant
BNF_CONTENT_TYPES = [
    "article",
    "advertisement",
    "illustration",
    "ornament",
    "freead",
    "table",
]
SECTION = "section"
"""
Content types as defined in BNF Mets flavour. 
These are the ones we are interested in parsing. 
The `SECTION` type should be flattened, and shouldn't be part of content items,
but it is needed to parse what's inside.
"""

type_translation = {
    "illustration": CONTENTITEM_TYPE_IMAGE,
    "advertisement": CONTENTITEM_TYPE_ADVERTISEMENT,
    "ornament": CONTENTITEM_TYPE_OBITUARY,
    "table": CONTENTITEM_TYPE_TABLE,
    "article": CONTENTITEM_TYPE_ARTICLE,
    "freead": CONTENTITEM_TYPE_ADVERTISEMENT,
}

logger = logging.getLogger(__name__)


def add_div(
    _dict: dict[str, tuple[str, str]], _type: str, div_id: str, label: str
) -> dict[str, tuple[str, str]]:
    """Adds a div item to the given dictionary (sorted by type).

    The types used as keys should be in `BNF_CONTENT_TYPES` or `SECTION`.

    Args:
        _dict (dict[str, tuple[str, str]]): The dictionary where to add the div
        _type (str): The type of the new div to add.
        div_id (str): The div ID to add.
        label (str): The label of the div to add.

    Returns:
        dict[str, tuple[str, str]]: The updated dictionary.
    """
    if _type in BNF_CONTENT_TYPES or _type == SECTION:
        if _type in _dict:
            _dict[_type].append((div_id, label))
        else:
            _dict[_type] = [(div_id, label)]
    else:
        logger.warning("Tried to add div of type %s", _type)

    return _dict


def get_journal_name(archive_path: str) -> str:
    """Return the Journal name from the path of the issue.

    It assumes the journal name is one directory above the issue.

    Args:
        archive_path (str): Path to the issue's archive

    Returns:
        str: Extracted journal name in lowercase.
    """
    journal = archive_path.split("/")[-2].split("-")
    journal = "".join(journal).lower()

    return journal


def is_multi_date(date_string: str) -> bool:
    """Check whether a given date string is composed of more than one date.

    This check is based on the assumption that a full date is 10 chars long.

    Args:
        date_string (str): Date to check for

    Returns:
        bool: True if the string represents multiple dates
    """
    return len(date_string) > 10


def get_dates(date_string: str, separators: list[str]) -> list[Optional[str]]:
    """Extract date from given string using list of possible separators.

    Assumes that the given date string represents exactly 2 dates.
    Tries to separate them using the given separators, and return when two
    date were found, otherwise list of None is returned.

    Args:
        date_string (str): The date string to separate.
        separators (list[str]): The list of potential separators.

    Returns:
        list[Optional[str]]: Separated date or pair of None.
    """
    for s in separators:
        if len(date_string.split(s)) == 2:
            return date_string.split(s)

    return [None, None]


def parse_date(
    date_string: str, formats: list[str], separators: list[str]
) -> tuple[datetime.date, Optional[datetime.date]]:
    """Parse a date given a list of formats.

    The input string can sometimes represent a pair of dates, in which case
    they are both parsed if possible.

    Args:
        date_string (str): Date string to parse.
        formats (list[str]): Possible dates formats.
        separators (list[str]): List of possible date separators.

    Raises:
        ValueError: The input date string is too short to be a full date.
        ValueError: The string contains two dates that could not be split
            correctly.
        ValueError: The (first) date could not be parsed correctly.

    Returns:
        tuple[datetime.date, Optional[datetime.date]]: Parsed date, potentially
            parsed pair of dates.
    """
    date, secondary = None, None
    # Date_string 1 and 2
    ds_1, ds_2 = None, None

    # Dates have at least 10 characters.
    # Some (very rarely) issues have only year/month
    if len(date_string) < 10:
        raise ValueError(f"Could not parse date {date_string}")
    # Here we potentially have two dates, take the first one
    elif is_multi_date(date_string):
        logger.info("Got two dates %s", date_string)
        ds_1, ds_2 = get_dates(date_string, separators)

        if ds_1 is None:
            raise ValueError(f"Could not parse date {date_string}")
    else:
        ds_1 = date_string

    # Now parse
    for f in formats:
        try:
            date = datetime.strptime(ds_1, f).date()
            if ds_2 is not None:
                secondary = datetime.strptime(ds_2, f).date()
        except ValueError:
            pass

    if date is None:
        raise ValueError(f"Could not parse date {date_string}")

    return date, secondary


def get_manifest_info(issue_path: str) -> tuple[dict[int, tuple[int, int]], str]:
    """Read the issue's IIIF presentation `manifest.json` file if present.

    When available, the manifest.json file (fetched from the BNF IIIF
    presentation API) is the preferred source of the facsimile width/height
    (in pixels) of each page, matched based on the `f{page_number}` fragment
    of each canvas item's `id`. Legacy BNF OLR issues (`en_olr`, `mp_olr`)
    currently don't have this file on disk, in which case callers should fall
    back to reading each page's ALTO `<Page>` tag WIDTH/HEIGHT attributes.

    typical item in the manifest:
    {
    "id": "https://openapi.bnf.fr/iiif/presentation/v3/ark:/12148/bpt6k9742578x/f1/canvas",
    "type": "Canvas",
    "label": {
        "fr": [
        "NP"
        ]
    },
    "height": 6084,
    "width": 4584,
    [...]

    This function extracts the page dimensions and the newspaper title from
    the IIIF presentation API metadata.

    Args:
        issue_path (str): Path to the directory containing the issue's data.

    Returns:
        tuple[dict[int, tuple[int, int]], str]: Mapping from page number to
            (width, height) in pixels, and extracted title string.
            Empty if no `manifest.json` was found in `issue_path`, or if it
            couldn't be parsed as expected.
    """
    manifest_path = os.path.join(issue_path, "manifest.json")
    page_dims: dict[int, tuple[int, int]] = {}
    title_variant = ""

    if not os.path.exists(manifest_path):
        return page_dims, title_variant

    try:
        with open(manifest_path, "r", encoding="utf-8") as fin:
            manifest_contents = json.load(fin)

        for m_dict in manifest_contents["metadata"]:
            if m_dict["label"]["fr"] == ["Titre"]:
                title_variant = m_dict["value"]["fr"][0]

        for mft_item in manifest_contents.get("items", []):
            # typical item id:
            # ".../presentation/v3/ark:/12148/{ark_id}/f{page_number}/canvas"
            item_pg_fnum = mft_item["id"].split("/")[-2]
            page_no = int(item_pg_fnum[1:])
            page_dims[page_no] = (mft_item["width"], mft_item["height"])
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as e:
        logger.warning("Could not parse manifest.json at %s: %s", manifest_path, e)

    return page_dims, title_variant
