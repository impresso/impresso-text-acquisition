import logging
import os
from time import strftime
from typing import Dict, List

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from text_importer.importers import CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPES
from text_importer.importers.mets_alto import (MetsAltoNewspaperIssue,
                                               MetsAltoNewspaperPage,
                                               parse_mets_amdsec)
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URL = "https://impresso-project.ch/api/proxy/iiif/"


class ReroNewspaperPage(MetsAltoNewspaperPage):
    """Class representing a page in RERO (Mets/Alto) data."""
    
    def add_issue(self, issue):
        self.issue = issue
        self.page_data['iiif'] = os.path.join(IIIF_ENDPOINT_URL, self.id)
    
    # no coordinate conversion needed, but keeping it here for now
    def _convert_coordinates(self, page_data):
        """
         no conversion of coordinates
        :param page_data:
        :return:
        """
        return False, page_data


class ReroNewspaperIssue(MetsAltoNewspaperIssue):
    """Class representing an issue in RERO (Mets/Alto) data.
    All functions defined in this child class are specific to parsing RERO Mets/Alto format
    """
    
    def _find_pages(self):
        """Detects the Alto XML page files for a newspaper issue and initializes page objects."""
        alto_path = os.path.join(self.path, 'ALTO')
        
        if not os.path.exists(alto_path):
            logger.critical(f"Could not find pages for {self.id}")
        
        page_file_names = [
            file
            for file in os.listdir(alto_path)
            if not file.startswith('.') and '.xml' in file
            ]
        
        page_numbers = []
        
        for fname in page_file_names:
            page_no = fname.split('.')[0]
            page_numbers.append(int(page_no))
        
        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4))
            for page_n in page_numbers
            ]
        
        self.pages = []
        for filename, page_no, page_id in zip(
                page_file_names, page_numbers, page_canonical_names
                ):
            try:
                self.pages.append(
                        ReroNewspaperPage(
                                page_id,
                                page_no,
                                filename,
                                alto_path
                                )
                        )
            except Exception as e:
                logger.error(
                        f'Adding page {page_no} {page_id} {filename}',
                        f'raised following exception: {e}'
                        )
                raise e
    
    def _parse_content_parts(self, content_div) -> List[Dict[str, str]]:
        """
        Given the div of a content item, this function parses the children and constructs the legacy `parts` component
        :param content_div: The div containing the content item
        :return: list[dict] of different parts for this content item (role, id, fileid, page)
        """
        parts = []
        for child in content_div.children:
            
            if isinstance(child, NavigableString):
                continue
            elif isinstance(child, Tag):
                type_attr = child.get('TYPE')
                comp_role = type_attr.lower() if type_attr else None
                areas = child.findAll('area')
                for area in areas:
                    comp_id = area.get('BEGIN')
                    comp_fileid = area.get('FILEID')
                    comp_page_no = int(comp_fileid.replace('ALTO', ''))
                    
                    parts.append(
                            {
                                'comp_role': comp_role,
                                'comp_id': comp_id,
                                'comp_fileid': comp_fileid,
                                'comp_page_no': comp_page_no
                                }
                            )
        return parts
    
    def _parse_content_item(self, item_div, counter: int):
        """
        Parses a content item div and returns the dictionary representing it.

        :param item_div: Div of content item
        :param counter: Number of content items already added
            (needed to generate canonical id).
        :return:  dict, of the resulting content item

        """
        div_type = item_div.get('TYPE').lower()
        
        # TODO: check if other content items can be translated
        # (maybe add translation dict)
        if div_type == 'picture':
            div_type = CONTENTITEM_TYPE_IMAGE
        
        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning(f"Found new content item type: {div_type}")
        
        metadata = {
            'id': "{}-i{}".format(self.id, str(counter).zfill(4)),
            'tp': div_type,
            'pp': [],
            't': item_div.get('LABEL')
            }
        
        content_item = {
            "m": metadata,
            "l": {
                "id": item_div.get('ID'),
                "parts": self._parse_content_parts(item_div)
                }
            }
        for p in content_item['l']['parts']:
            pge_no = p["comp_page_no"]
            if pge_no not in content_item['m']['pp']:
                content_item['m']['pp'].append(pge_no)
                
        if content_item['m']['tp'] == CONTENTITEM_TYPE_IMAGE:
            content_item['m']['c'], content_item['iiif_link'] = self._get_image_info(content_item['l']['parts'])
        return content_item
    
    def _parse_content_items(self, mets_doc: BeautifulSoup):
        """
        Extract content item elements from a Mets XML file.
        :param BeautifulSoup mets_doc:
        :return:
        """
        content_items = []
        divs = mets_doc.find('div', {'TYPE': 'CONTENT'}).findChildren(
                'div',
                recursive=False
                )  # Children of "Content" tag
        
        # Sort to have same naming
        sorted_divs = sorted(divs, key=lambda x: x.get('ID').lower())
        
        found_types = set(x.get('TYPE') for x in sorted_divs)
        print(f"Found types {found_types} for content items")
        
        counter = 1
        for div in sorted_divs:
            # Parse Each contentitem
            content_items.append(self._parse_content_item(div, counter))
            counter += 1
        return content_items
    
    def _parse_mets(self):
        """Parses the Mets XML file of the newspaper issue."""
        
        mets_doc = self.xml
        
        self.image_properties = parse_mets_amdsec(
                mets_doc,
                x_res='XphysScanResolution',
                y_res='YphysScanResolution'
                )  # Parse the resolution of page images
        
        # Parse all the content items
        content_items = self._parse_content_items(mets_doc)
        
        self.issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "i": content_items,
            "id": self.id,
            "ar": self.rights,
            "pp": [p.id for p in self.pages]
            }
    
    def _get_image_info(self, parts):
        # Fetch the legacy parts
        
        assert len(parts) == 1, "Image has more than 1 part"
        part = parts[0]
        
        # Fetch page number and corresponding page
        pge_nb = part['comp_page_no']
        comp_id = part['comp_id']
        page = [p for p in self.pages if p.number == pge_nb][0]
        
        elements = page.xml.findAll("TextBlock", {"ID": comp_id})
        assert len(elements) <= 1, "Image comp_id matches multiple TextBlock tags"
        if len(elements) == 0:
            return []
        
        element = elements[0]
        hpos, vpos, width, height = element.get('HPOS'), element.get('VPOS'), element.get('WIDTH'), element.get('HEIGHT')
        coords = [int(hpos), int(vpos), int(width), int(height)]
        iiif_link = os.path.join(IIIF_ENDPOINT_URL, page.id, ",".join(coords), 'full', '0', 'default.jpg')
        
        return coords, iiif_link
