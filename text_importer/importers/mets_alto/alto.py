"""Utility functions to parse Alto XML files."""

from typing import Dict, List, Tuple

import bs4
from bs4.element import Tag


def distill_coordinates(element: Tag) -> List[int]:
    """Extract image coordinates from any XML tag.

    .. note ::
        This function assumes the following attributes to be present in the
        input XML element: ``HPOS``, ``VPOS``. ``WIDTH``, ``HEIGHT``.

    :param Tag element: Input XML tag.
    :return: An ordered list of coordinates (``x``, ``y``, ``width``,
        ``height``).
    :rtype: List[int]

    """
    hpos = int(float(element.get('HPOS')))
    vpos = int(float(element.get('VPOS')))
    width = int(float(element.get('WIDTH')))
    height = int(float(element.get('HEIGHT')))
    
    # NB: these coordinates need to be converted
    return [hpos, vpos, width, height]


def parse_textline(element: Tag) -> Tuple[dict, List[str]]:
    line = {}
    line['c'] = distill_coordinates(element)
    tokens = []
    
    notes = []
    for child in element.children:
        
        if isinstance(child, bs4.element.NavigableString):
            continue
        
        if child.name == 'String':
            
            # Here we do this in case coordinates are not found for this String
            try:
                coords = distill_coordinates(child)
            except TypeError as e:
                notes.append("Token {} does not have coordinates".format(child.get('ID')))
                continue
            token = {
                'c': coords,
                'tx': child.get('CONTENT')
                }
            
            if child.get('SUBS_TYPE') == "HypPart1":
                # token['tx'] += u"\u00AD"
                token['tx'] += "-"
                token['hy'] = True
            elif child.get('SUBS_TYPE') == "HypPart2":
                token['nf'] = child.get('SUBS_CONTENT')
            
            tokens.append(token)
    
    line['t'] = tokens
    return line, notes


def parse_printspace(element: Tag, mappings: Dict[str, str]) -> Tuple[List[dict], List[str]]:
    """Parse the ``<PrintSpace>`` element of an ALTO XML document.

    :param Tag element: Input XML element (``<PrintSpace>``).
    :param Dict[str,str] mappings: Description of parameter `mappings`.
    :return: Description of returned object.
    :rtype: List[dict]

    """
    
    regions = []
    notes = []
    # in case of a blank page, the PrintSpace element is not found thus
    # it will be none
    if element:
        for block in element.children:
            
            if isinstance(block, bs4.element.NavigableString):
                continue
            
            block_id = block.get('ID')
            if block_id in mappings:
                part_of_contentitem = mappings[block_id]
            else:
                part_of_contentitem = None
            
            coordinates = distill_coordinates(block)
            
            tmp = [
                parse_textline(line_element)
                for line_element in block.findAll('TextLine')
                ]
            
            if len(tmp) > 0:
                lines, new_notes = list(zip(*tmp))
                new_notes = [i for n in new_notes for i in n]
            else:
                lines, new_notes = [], []
            
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
            
            notes += new_notes
            regions.append(region)
    return regions, notes


def parse_style(style_div):
    """ Parses the font-style information in the ALTO files (BNL and BNF)
    
    :param style_div:
    :return:
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
        "fs": float(font_size),
        "f": font_name
        }
    return style
