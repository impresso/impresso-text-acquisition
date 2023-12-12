import logging
import os
from time import strftime
from typing import Dict, List

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from impresso_commons.path import IssueDir

from text_importer.importers import (CONTENTITEM_TYPES, CONTENTITEM_TYPE_ADVERTISEMENT, CONTENTITEM_TYPE_IMAGE,
                                     CONTENTITEM_TYPE_TABLE)
from text_importer.importers.bnf.helpers import BNF_CONTENT_TYPES
from text_importer.importers.mets_alto import (MetsAltoNewspaperIssue,
                                               MetsAltoNewspaperPage)
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URL = "https://gallica.bnf.fr/iiif/ark:/12148/"
IIIF_SUFFIX = "full/full/0/default.jpg"
SECTION_TYPE = "section"

type_translation = {
    'illustration': CONTENTITEM_TYPE_IMAGE,
    'advertisement': CONTENTITEM_TYPE_ADVERTISEMENT,
    }


class BnfEnNewspaperPage(MetsAltoNewspaperPage):
    """Class representing a page in RERO (Mets/Alto) data."""
    
    def __init__(self, _id: str, n: int, filename: str, basedir: str):
        super().__init__(_id, n, filename, basedir)
        
        page_tag = self.xml.find('Page')
        self.page_width = float(page_tag.get('WIDTH'))
    
    def add_issue(self, issue):
        self.issue = issue
        ark = issue.ark_link
        self.page_data['iiif'] = os.path.join(IIIF_ENDPOINT_URL, ark, "f{}".format(self.number), IIIF_SUFFIX)


class BnfEnNewspaperIssue(MetsAltoNewspaperIssue):
    
    def __init__(self, issue_dir: IssueDir):
        self.ark_link = issue_dir.ark_link
        super().__init__(issue_dir)
    
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
            page_no = fname.split('.')[0].split('-')[1]
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
                        BnfEnNewspaperPage(
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
    
    def _get_ci_language(self, dmdid):
        """
        Finds the language code of the CI with given DMDID. Languages are usually in a dmdSec at the beginning of a METS file
        TODO: remove self.xml here in favor of an a function argument.
        :param dmdid:
        :return:
        """
        doc = self.xml
        lang = doc.find("dmdSec", {"ID": dmdid})
        if lang is None:
            return None
        lang = lang.find("mods:languageTerm") 
        if lang is None:
            return None
        return lang.text
    
    def _parse_content_item(self, item_div, counter: int) -> dict:
        """
        Parses a content item div and returns the dictionary representing it.

        :param item_div: Div of content item
        :param counter: Number of content items already added
            (needed to generate canonical id).
        :return:  dict, of the resulting content item

        """
        div_type = item_div.get('TYPE').lower()
        # Translate types
        if div_type in type_translation:
            div_type = type_translation[div_type]
        
        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning(f"Found new content item type: {div_type}")
        
        metadata = {
            'id': "{}-i{}".format(self.id, str(counter).zfill(4)),
            'tp': div_type,
            'pp': [],
            't': item_div.get('LABEL')
            }
        
        # Get CI language
        language = self._get_ci_language(item_div.get('DMDID'))
        if language is not None:
            metadata['l'] = language
        
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
        
        if div_type in [CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPE_TABLE]:
            content_item['m']['c'], content_item['iiif_link'] = self._get_image_info(content_item)
        return content_item
    
    def _decompose_section(self, div):
        """ In RERO3, sometimes textblocks and images are withing `Section` tags. Those need to be recursively decomposed

        :param div: The `Section` div
        :return: all children divs that are not `Section`
        """
        logger.info("Decomposing section type")
        section_divs = [d for d in div.findAll("div") if
                        d.get('TYPE').lower() in BNF_CONTENT_TYPES]
        section_divs = sorted(section_divs, key=lambda x: x.get('ID').lower())  # Sort to get same IDS
        
        final_divs = []
        # Now do it recursively
        for d in section_divs:
            d_type = d.get('TYPE')
            if d_type is not None:
                if d_type.lower() == SECTION_TYPE:
                    final_divs += self._decompose_section(d)  # Recursively decompose
                else:
                    final_divs.append(d)
        return final_divs
    
    def _parse_content_items(self):
        """
        Extract content item elements from a Mets XML file.
        :param BeautifulSoup mets_doc:
        :return:
        """
        content_items = []
        doc = self.xml
        
        dmd_sections = doc.findAll("dmdSec")
        struct_map = doc.find("div", {"TYPE": "CONTENT"})
        # Sort to have same naming
        sorted_divs = sorted(dmd_sections, key=lambda x: x.get('ID').lower())
        
        counter = 1
        for d in sorted_divs:
            div = struct_map.findAll("div", {"DMDID": d.get('ID')})
            if len(div) == 0:
                continue
            elif len(div) > 1:
                logger.warning("Multiple divs matching {} in structmap for {}".format(d.get('ID'), self.id))
            else:
                div = div[0]
                div_type = div.get('TYPE').lower()
                if div_type == SECTION_TYPE:
                    section_divs = self._decompose_section(div)
                    for sd in section_divs:
                        content_items.append(self._parse_content_item(sd, counter))
                        counter += 1
                else:
                    content_items.append(self._parse_content_item(div, counter))
                    counter += 1
        return content_items
    
    def _get_image_info(self, content_item):
        # Fetch the legacy parts
        
        # Images cannot be on multiple pages
        num_pages = len(content_item['m']['pp'])
        assert num_pages == 1, "Image is on more than one page"
        
        page_nb = content_item['m']['pp'][0]
        page = [p for p in self.pages if p.number == page_nb][0]
        parts = content_item['l']['parts']
        
        assert len(parts) >= 1, f"No parts for image {content_item['m']['id']}"
        
        if len(parts) > 1:
            logger.info(f"Found multiple parts for image {content_item['m']['id']}, selecting largest one")
        
        coords = None
        max_area = 0
        # Image can have multiple parts, choose largest one (with max area)
        for part in parts:
            comp_id = part['comp_id']
            
            elements = page.xml.findAll(["ComposedBlock", "TextBlock"], {"ID": comp_id})
            assert len(elements) <= 1, "Image comp_id matches multiple TextBlock tags"
            if len(elements) == 0:
                continue
            
            element = elements[0]
            hpos, vpos, width, height = element.get('HPOS'), element.get('VPOS'), element.get('WIDTH'), element.get('HEIGHT')
            
            # Select largest image
            area = int(float(width)) * int(float(height))
            if area > max_area:
                max_area = area
                coords = [int(float(hpos)), int(float(vpos)), int(float(width)), int(float(height))]
        
        # coords = convert_coordinates(coords, self.image_properties[page.number], page.page_width)
        iiif_link = os.path.join(IIIF_ENDPOINT_URL, self.ark_link, "f{}".format(page.number),
                                 ",".join([str(x) for x in coords]), 'full', '0', 'default.jpg')
        
        return coords, iiif_link
    
    def _parse_mets(self):
        content_items = self._parse_content_items()
        
        self.issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "i": content_items,
            "id": self.id,
            "ar": self.rights,
            "pp": [p.id for p in self.pages]
        }
