"""Utility functions to parse Alto XML files."""

import bs4
from bs4.element import Tag


def distill_coordinates(element: Tag) -> list[int]:
    """Extract image coordinates from any XML tag.

    Note:
        This function assumes the following attributes to be present in the
        input XML element: ``HPOS``, ``VPOS``. ``WIDTH``, ``HEIGHT``.

    Args:
        element (Tag): Input XML tag containing coordinates to distill.

    Returns:
        list[int]: An ordered list of coordinates (``x``, ``y``, ``width``,
            ``height``).
    """
    hpos = int(float(element.get("HPOS")))
    vpos = int(float(element.get("VPOS")))
    width = int(float(element.get("WIDTH")))
    height = int(float(element.get("HEIGHT")))

    # NB: these coordinates need to be converted
    return [hpos, vpos, width, height]


def parse_textline(element: Tag) -> tuple[dict, list[str]]:
    """Parse the ``<TextLine>`` element of an ALTO XML document.

    Args:
        element (Tag): Input XML element (``<TextLine>``).

    Returns:
        tuple[dict, list[str]]: Parsed lines or text in the canonical format
            and notes about potential missing token coordinates.
    """
    line = {}
    line["c"] = distill_coordinates(element)
    tokens = []

    notes = []
    for child in element.children:

        if isinstance(child, bs4.element.NavigableString):
            continue

        if child.name == "String":
            # Here we do this in case coordinates are not found for this String
            try:
                coords = distill_coordinates(child)
            except TypeError:
                notes.append(f"Token {child.get('ID')} does not have coordinates")
                coords = None
                continue

            token = {"c": coords, "tx": child.get("CONTENT")}

            if child.get("SUBS_TYPE") == "HypPart1":
                # token['tx'] += u"\u00AD"
                token["tx"] += "-"
                token["hy"] = True
            elif child.get("SUBS_TYPE") == "HypPart2":
                token["nf"] = child.get("SUBS_CONTENT")

            tokens.append(token)

    line["t"] = tokens
    return line, notes


def parse_printspace(element: Tag, mappings: dict[str, str]) -> tuple[list[dict], list[str]]:
    """Parse the ``<PrintSpace>`` element of an ALTO XML document.

    This element contains all the OCR information about the content items of
    a page, up to the lowest level of the hierarchy: the regions, paragraphs,
    lines and tokens, each with their corresponding coordinates.

    Args:
        element (Tag): Input XML element (``<PrintSpace>``).
        mappings (dict[str, str]): Mapping from OCR component ids to their
            corresponding canonical Content Item ID.

    Returns:
        tuple[list[dict], list[str]]: List of page regions in the canonical
            format and notes about potential parsing problems.
    """

    regions = []
    notes = []
    # in case of a blank page, the PrintSpace element is not found thus
    # it will be none
    if element:
        for block in element.children:

            if isinstance(block, bs4.element.NavigableString):
                continue

            block_id = block.get("ID")
            if block_id in mappings:
                part_of_contentitem = mappings[block_id]
            else:
                part_of_contentitem = None

            coordinates = distill_coordinates(block)

            tmp = [parse_textline(line_element) for line_element in block.findAll("TextLine")]

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


def parse_style(style_div: Tag) -> dict[str, float | str]:
    """Parse the font-style information in the ALTO files (for BNL and BNF).

    Args:
        style_div (Tag): Element of XML file containing font-style information.

    Returns:
        dict[str, float | str]: Parsed style for Issue canonical format.
    """
    font_family = style_div.get("FONTFAMILY")
    font_size = style_div.get("FONTSIZE")
    font_style = style_div.get("FONTSTYLE")
    font_id = style_div.get("ID")

    font_name = font_family
    if font_style is not None:
        font_name = "{}-{}".format(font_name, font_style)

    style = {
        "id": font_id,
        "fs": float(font_size) if font_size != "" else None,
        "f": font_name,
    }
    return style
