"""Utility functions to parse BNF ALTO files."""
import logging
from typing import List, Dict, Optional, Tuple

import bs4
from bs4 import NavigableString
from bs4.element import Tag

from text_importer.importers.bnf.helpers import BNF_CONTENT_TYPES, type_translation
from text_importer.importers.mets_alto.alto import parse_textline, distill_coordinates

logger = logging.getLogger(__name__)


def parse_printspace(element: Tag, mappings: Dict[str, str]) -> List[dict]:
    """Parse the ``<PrintSpace>`` element of an ALTO XML document.

    :param Tag element: Input XML element (``<PrintSpace>``).
    :param Dict[str,str] mappings: Description of parameter `mappings`.
    :return: Description of returned object.
    :rtype: List[dict]

    """

    regions = []

    for block in element.children:

        if isinstance(block, bs4.element.NavigableString):
            continue

        block_id = block.get('ID')
        if block.name == "ComposedBlock":
            cb_regions = parse_printspace(block, mappings)
            regions += cb_regions
        else:
            if block_id in mappings:
                part_of_contentitem = mappings[block_id]
            else:
                part_of_contentitem = None

            coordinates = distill_coordinates(block)

            lines = [
                parse_textline(line_element)
                for line_element in block.findAll('TextLine')
            ]

            paragraph = {
                "c": coordinates,
                "l": lines
            }

            region = {
                "c": coordinates,
                "p": [paragraph]
            }

            if part_of_contentitem:
                region['pOf'] = part_of_contentitem
            regions.append(region)
    return regions


def parse_div_parts(div: Tag) -> List[dict]:
    """Parses the parts of a div (lower level divs that are not in `BNF_CONTENT_TYPES`.
    Typically, any div of type in `BNF_CONTENT_TYPES` is composed of child divs. This is what this function parses.

    :param bs4.element.Tag div:
    :return: List[dict] The list of parts of this Tag. The keys are {'comp_role', 'comp_id', 'comp_fileid', 'comp_page_no'}
    """
    parts = []
    for child in div.children:

        if isinstance(child, NavigableString):
            continue
        elif isinstance(child, Tag):
            type_attr = child.get('TYPE')
            comp_role = type_attr.lower() if type_attr else None

            if comp_role not in BNF_CONTENT_TYPES:
                areas = child.findAll('area')
                for area in areas:
                    comp_id = area.get('BEGIN')
                    comp_fileid = area.get('FILEID')
                    comp_page_no = int(comp_fileid.split(".")[1])

                    parts.append(
                            {
                                'comp_role': comp_role,
                                'comp_id': comp_id,
                                'comp_fileid': comp_fileid,
                                'comp_page_no': comp_page_no
                            }
                    )
    return parts


def parse_embedded_cis(div: Tag,
                       label: str,
                       issue_id: str,
                       parent_id: Optional[str],
                       counter: int) -> Tuple[List[dict], int]:
    """This function parses the embedded div Tags within the given one. The input `div` should be of type in
    `BNF_CONTENT_TYPES` and should have children of types also in that category.

    This function separates them, as they should be separate content items.

    :param bs4.element.Tag div: The parent tag
    :param str label: The label of the parent tag
    :param str issue_id: The ID of the issue
    :param str parent_id: The ID of the parent tag (to put into pOf), can be None
    :param int counter: Counter for content items
    :return: Tuple[List[dict], int]: The embedded CIs and the counter after adding them
    """
    new_cis = []
    for child in div.children:

        if isinstance(child, NavigableString):
            continue
        elif isinstance(child, Tag):
            type_attr = child.get('TYPE')
            comp_role = type_attr.lower() if type_attr else None
            if comp_role in BNF_CONTENT_TYPES:
                if comp_role in type_translation:
                    impresso_type = type_translation[comp_role]
                else:
                    logger.warning(f"Type {comp_role} does not have translation")
                    impresso_type = comp_role

                metadata = {
                    'id': "{}-i{}".format(issue_id, str(counter).zfill(4)),
                    'tp': impresso_type,
                    'pp': [],
                }
                lab = child.get('LABEL') or label

                if lab is not None:
                    metadata['t'] = lab
                # Check if the parent exists (sometimes tables are embedded into articles, but the articles are empty)
                if parent_id is not None:
                    metadata['pOf'] = parent_id
                new_ci = {
                    'm': metadata,
                    'l': {'parts': parse_div_parts(child)}
                }
                new_cis.append(new_ci)
                counter += 1
    return new_cis, counter
