import logging
import os
from time import strftime
from typing import List, Optional

from text_importer.importers import (CONTENTITEM_TYPES, CONTENTITEM_TYPE_IMAGE)
from text_importer.importers.mets_alto import (MetsAltoNewspaperIssue,
                                               MetsAltoNewspaperPage)
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URL = "https://impresso-project.ch/api/proxy/iiif/"


class BlNewspaperPage(MetsAltoNewspaperPage):
    
    def add_issue(self, issue: MetsAltoNewspaperIssue):
        """Adds the given `BlNewspaperIssue` as an attribute for this class
        
        :param BlNewspaperIssue issue:
        """
        self.issue = issue
        self.page_data['iiif'] = os.path.join(IIIF_ENDPOINT_URL, self.id)


class BlNewspaperIssue(MetsAltoNewspaperIssue):
    
    def _find_pages(self):
        """Detects the Alto XML page files for a newspaper issue and initializes page objects."""
        page_file_names = [
                file
                for file in os.listdir(self.path)
                if not file.startswith('.') and '.xml' in file and 'mets' not in file
                ]
        page_numbers = [int(os.path.splitext(fname)[0].split('_')[-1]) for fname in page_file_names]
        
        page_canonical_names = ["{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers]
        
        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                self.pages.append(BlNewspaperPage(page_id, page_no, filename, self.path))
            except Exception as e:
                logger.error(
                        f'Adding page {page_no} {page_id} {filename}',
                        f'raised following exception: {e}'
                        )
                raise e
    
    def _get_part_dict(self, div, comp_role: Optional[str]) -> dict:
        """Helper function to construct the part for a certain div entry of METS
        
        :param div: Content item div
        :param str comp_role: Role of the component
        :return: dict
        """
        
        comp_fileid = div.find('area', {'BETYPE': 'IDREF'}).get('FILEID')
        comp_id = div.get('ID')
        comp_page_no = int(div.parent.get('ORDER'))
        if comp_role is None:
            type_attr = div.get('TYPE')
            comp_role = type_attr.lower() if type_attr else None
        return {'comp_role': comp_role, 'comp_id': comp_id, 'comp_fileid': comp_fileid, 'comp_page_no': int(comp_page_no)}
    
    def _parse_content_parts(self, item_div, phys_map) -> List[dict]:
        """Given a item entry in METS, and the physical structure of the Newspaper, parses all parts relation to item.
        
        :param item_div: The div corresponding to the item
        :param phys_map: The physical structure of the Issue
        :return: list[dict] Of each content part for given item
        """
        # Find all parts and their IDS
        tag = f"#{item_div.get('ID')}"
        linkgrp = self.xml.find('smLocatorLink', {'xlink:href': tag}).parent
        
        # Remove `#` from xlink:href
        parts_ids = [x.get('xlink:href')[1:] for x in linkgrp.findAll('smLocatorLink') if x.get('xlink:href') != tag]
        parts = []
        for p in parts_ids:
            div = phys_map.find('div', {'ID': p})  # Get element in physical map
            type_attr = div.get('TYPE')
            comp_role = type_attr.lower() if type_attr else None
            
            if comp_role == 'page':  # In that case, need to add all parts
                for x in div.findAll('div'):
                    parts.append(self._get_part_dict(x, None))
            else:
                parts.append(self._get_part_dict(div, comp_role))
        
        return parts
    
    def _get_content_item_language(self, dmdid: str) -> str:
        """ Given a DMDID, searches the METS file for the item's language
        
        :param str dmdid: Legacy ID of the content item
        :return: str Language of content item
        """
        return self.xml.find('dmdSec', {'ID': dmdid}).findChild('languageTerm').text
    
    def _parse_content_item(self, item_div, counter: int, phys_structmap) -> dict:
        """Parses the given content item: searches for all parts and constructs unique IDs
        
        :param item_div: The div of the content item
        :param int counter: The counter to get unique ordered IDs
        :param phys_structmap: The physical structmap element of the Mets file
        :return: dict Representing the content item
        """
        div_type = item_div.get('TYPE').lower()
        
        if div_type == 'picture':
            div_type = CONTENTITEM_TYPE_IMAGE
        
        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning(f"Found new content item type: {div_type}")
        
        metadata = {
                'id': "{}-i{}".format(self.id, str(counter).zfill(4)),
                'tp': div_type,
                'pp': [],
                'l': self._get_content_item_language(item_div.get('DMDID')) # Get language from METS file
                }
        
        # Load physical struct map
        content_item = {
                "m": metadata,
                "l": {
                        "id": item_div.get('ID'),
                        "parts": self._parse_content_parts(item_div, phys_structmap) # Find all parts in physical map
                        }
                }
        for p in content_item['l']['parts']:
            pge_no = p["comp_page_no"]
            if pge_no not in content_item['m']['pp']:
                content_item['m']['pp'].append(pge_no)
                # TODO: add coordinates for images as well as iiif_link
        return content_item
    
    def _parse_content_items(self) -> List[dict]:
        """
        Extract content item elements from a Mets XML file.
        :return: list[dict] The list of all content items and the relevant information
        """
        content_items = []
        # Get logical structure of issue
        divs = self.xml.find('structMap', {'TYPE': 'LOGICAL'}).find('div', {'TYPE': 'ISSUE'}).findChildren('div')
        
        # Sort to have same naming
        sorted_divs = sorted(divs, key=lambda x: x.get('DMDID').lower())
        
        # Get all CI types
        found_types = set(x.get('TYPE') for x in sorted_divs)
        print(f"Found types {found_types} for content items")
        
        phys_structmap = self.xml.find('structMap', {'TYPE': 'PHYSICAL'})

        counter = 1
        for div in sorted_divs:
            # Parse Each contentitem
            content_items.append(self._parse_content_item(div, counter, phys_structmap))
            counter += 1
        return content_items
    
    def _parse_mets(self):
        """Parses the Mets XML file of the newspaper issue."""
        
        # No image properties in BL data
        # Parse all the content items
        content_items = self._parse_content_items()
        
        self.issue_data = {
                "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                "i": content_items,
                "id": self.id,
                "ar": self.rights,
                "pp": [p.id for p in self.pages]
                }
