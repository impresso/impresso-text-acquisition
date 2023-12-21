"""Utility functions to parse Alto XML files."""

import logging
import bs4
import os
from bs4.element import Tag

logger = logging.getLogger(__name__)


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
    hpos = int(float(element.get('HPOS')))
    vpos = int(float(element.get('VPOS')))
    width = int(float(element.get('WIDTH')))
    height = int(float(element.get('HEIGHT')))
    
    # NB: these coordinates need to be converted
    return [hpos, vpos, width, height]


def parse_textline(element: Tag, text_style: str | None = None
) -> tuple[dict, list[str]]:
    """Parse the ``<TextLine>`` element of an ALTO XML document.

    Args:
        element (Tag): Input XML element (``<TextLine>``).
        text_style (str | None, optional): text style ID. Defaults to None.

    Returns:
        tuple[dict, list[str]]: Parsed lines or text in the canonical format
            and notes about potential missing token coordinates. 
    """
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
                notes.append(f"Token {child.get('ID')} does not have coordinates")
                continue
            token = {
                'c': coords,
                'tx': child.get('CONTENT')
            }
            
            if text_style is not None:
                token['s'] = text_style

            if child.get('SUBS_TYPE') == "HypPart1":
                # token['tx'] += u"\u00AD"
                token['tx'] += "-"
                token['hy'] = True
            elif child.get('SUBS_TYPE') == "HypPart2":
                token['nf'] = child.get('SUBS_CONTENT')
            
            tokens.append(token)
    
    line['t'] = tokens
    return line, notes


def parse_printspace(element: Tag, mappings: dict[str, str],
                     text_styles: dict[str, dict] | None = None
) -> tuple[list[dict], list[str]]:
    """Parse the ``<PrintSpace>`` element of an ALTO XML document.

    This element contains all the OCR information about the content items of
    a page, up to the lowest level of the hierarchy: the regions, paragraphs, 
    lines and tokens, each with their corresponding coordinates.
    
    Args:
        element (Tag): Input XML element (``<PrintSpace>``).
        mappings (dict[str, str]): Mapping from OCR component ids to their 
            corresponding canonical Content Item ID.
        text_styles (dict[str, dict], optional): Text styles present on this
            page, with their page-level IDs as key and the text style object 
            containing issue-level IDs as value. Defaults to None.

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
            
            block_id = block.get('ID')
            if block_id in mappings:
                part_of_contentitem = mappings[block_id]
            else:
                part_of_contentitem = None
            
            coordinates = distill_coordinates(block)

            # fetch the issue-level text style id for this font
            block_style_id = (
                text_styles[block.get('STYLEREFS').split(' ')[0]]['id']
                if block.get('STYLEREFS') and text_styles
                else None
            )
            
            tmp = [
                parse_textline(line_element, block_style_id)
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

def parse_style(
    style_div: Tag, page_num: int | None = None
) -> dict[str, float | str]:
    """Parse the font-style information in the ALTO files.
    
    Function used by ONB, KB, BNL and BNF.
    Font style information defined at page-level, but gathered at issue level
    in the canonical format. When font IDs across pages don't match, the page 
    number can be defined to add it to the font's id.

    Args:
        style_div (Tag): Element of XML file containing font-style information.
        page_num (int | None, optional): Page number. Defaults to None.

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
    
    # add the page number when multiple pages have different ids for the fonts
    if page_num is not None:
        font_id = f"page_{str(page_num)}-{font_id}"

    style = {
        "id": font_id,
        "fs": float(font_size),
        "f": font_name
    }
    return style


def find_alto_files_or_retry(
    alto_path: str, issue_id: str, name_contents: str = '.xml'
) -> list[str]:
        """List XML files present in given page file dir, retry up to 3 times.

        During the processing, some IO errors can randomly happen when listing
        the contents of the directory, preventing the correct parsing of 
        the issue. The error is raised after the third try.
        If the given directory does not exist, only try once.

        Args:
            alto_path (str): Path to the directory with the Alto XML files.
            issue_id (str): Canonical ID of the issue the pages belong to.
            name_contents (str, optional): String used to filter the files of
                interest – Alto XML files. Defaults to '.xml'.

        Raises:
            e: Given directory does not exist, or listing its contents failed
                three times in a row.

        Returns:
            list[str]: List of paths of the pages' Alto XML files.
        """
        if not os.path.exists(alto_path):
            logger.critical(f"Could not find pages for {issue_id}")
            tries = 1

        tries = 3
        for i in range(tries):
            try:
                page_file_names = [
                    file
                    for file in os.listdir(alto_path)
                    if not file.startswith('.') and name_contents in file
                ]
                return page_file_names
            except IOError as e:
                if i < tries - 1: # i is zero indexed
                    logger.warning(f"Caught error for {issue_id}, "
                                   f"retrying (up to {tries} times) "
                                   f"to find pages. Error: {e}.")
                    continue
                else:
                    logger.warning("Reached maximum amount "
                                   f"of errors for {issue_id}.")
                    raise e