"""Utility functions to parse BNF ALTO files."""

import logging

import bs4
from bs4 import NavigableString
from bs4.element import Tag

from text_preparation.importers.bnf.helpers import BNF_CONTENT_TYPES, type_translation
from text_preparation.importers.mets_alto.alto import (
    parse_textline,
    distill_coordinates,
)

logger = logging.getLogger(__name__)


def parse_printspace(
    element: Tag, mappings: dict[str, str]
) -> tuple[list[dict], list[str] | None]:
    """Parse the ``<PrintSpace>`` element of an ALTO XML document for BNF.

    Args:
        element (Tag): Input XML element (``<PrintSpace>``).
        mappings (Dict[str, str]): Description of parameter `mappings`.

    Returns:
        tuple[list[dict], list[str] | None]: Parsed regions and paragraphs, and
            potential notes on issues encountered during the parsing.
    """

    regions = []
    notes = []
    for block in element.children:

        if isinstance(block, bs4.element.NavigableString):
            continue

        block_id = block.get("ID")
        if block.name == "ComposedBlock":
            cb_regions, new_notes = parse_printspace(block, mappings)
            regions += cb_regions
            notes += new_notes
        else:
            if block_id in mappings:
                part_of_contentitem = mappings[block_id]
            else:
                part_of_contentitem = None

            coordinates = distill_coordinates(block)

            tmp = [
                parse_textline(line_element)
                for line_element in block.findAll("TextLine")
            ]

            if len(tmp) > 0:
                lines, new_notes = list(zip(*tmp))
                new_notes = [i for n in new_notes for i in n]
            else:
                lines, new_notes = [], []

            paragraph = {"c": coordinates, "l": lines}

            region = {"c": coordinates, "p": [paragraph]}

            if part_of_contentitem:
                region["pOf"] = part_of_contentitem
            notes += new_notes
            regions.append(region)

    return regions, notes


def parse_div_parts(div: Tag) -> list[dict[str, str | int]]:
    """Parse the parts of a given div element.

    Typically, any div of type in `BNF_CONTENT_TYPES` is composed of child
    divs. This is what this function parses.
    Each element of the output contains keys {'comp_role', 'comp_id',
    'comp_fileid', 'comp_page_no'}.

    Args:
        div (Tag): Child div to parse.

    Returns:
        list[dict[str, str | int]]: The list of parts of this Tag.
    """
    parts = []
    for child in div.children:

        if isinstance(child, NavigableString):
            continue
        elif isinstance(child, Tag):
            type_attr = child.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None

            if comp_role not in BNF_CONTENT_TYPES:
                areas = child.findAll("area")
                for area in areas:
                    comp_id = area.get("BEGIN")
                    comp_fileid = area.get("FILEID")
                    comp_page_no = int(comp_fileid.split(".")[1])

                    parts.append(
                        {
                            "comp_role": comp_role,
                            "comp_id": comp_id,
                            "comp_fileid": comp_fileid,
                            "comp_page_no": comp_page_no,
                        }
                    )
    return parts


def parse_embedded_cis(
    div: Tag, label: str, issue_id: str, parent_id: str | None, counter: int
) -> tuple[list[dict], int]:
    """Parse the div Tags embedded in the given one.

    The input `div` should be of type in `BNF_CONTENT_TYPES` and should have
    children of types also in that category.
    Each child tag represents separate content items, which should thus be
    processed separately.

    Args:
        div (Tag): The parent tag.
        label (str): The label of the parent tag.
        issue_id (str): The ID of the issue.
        parent_id (str | None): The ID of the parent tag (to put into pOf).
        counter (int): Counter for content items.

    Returns:
        tuple[list[dict], int]: The embedded CIs and resulting updated counter.
    """
    new_cis = []
    for child in div.children:

        if isinstance(child, NavigableString):
            continue
        elif isinstance(child, Tag):
            type_attr = child.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None
            if comp_role in BNF_CONTENT_TYPES:
                if comp_role in type_translation:
                    impresso_type = type_translation[comp_role]
                else:
                    logger.warning("Type %s does not have translation", comp_role)
                    impresso_type = comp_role

                metadata = {
                    "id": "{}-i{}".format(issue_id, str(counter).zfill(4)),
                    "tp": impresso_type,
                    "pp": [],
                }
                lab = child.get("LABEL") or label

                if lab is not None:
                    metadata["t"] = lab
                # Check if the parent exists
                # (sometimes tables are embedded into articles,
                # but the articles are empty)
                if parent_id is not None:
                    metadata["pOf"] = parent_id
                new_ci = {"m": metadata, "l": {"parts": parse_div_parts(child)}}
                new_cis.append(new_ci)
                counter += 1

    return new_cis, counter
