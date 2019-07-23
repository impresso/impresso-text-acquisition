import codecs
import logging
import os
from time import strftime
from typing import Dict, List

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from impresso_commons.path.path_fs import IssueDir

from text_importer.helpers import get_issue_schema, get_page_schema
from text_importer.importers import *
from text_importer.importers.lux import alto

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URL = "https://impresso-project.ch/api/proxy/iiif/"


class ReroNewspaperPage(object):
    def __init__(self, n, _id, filename, basedir):
        self.number = n
        self.id = _id
        self.filename = filename
        self.basedir = basedir
        self.issue = None
        self.data = {
                'id': _id,
                'cdt': strftime("%Y-%m-%d %H:%M:%S"),
                'r': []  # here go the page regions
                }
    
    def add_issue(self, issue):
        self.issue = issue
        self.data['iiif'] = os.path.join(IIIF_ENDPOINT_URL, self.id)
    
    def parse(self):
        
        doc = self.xml
        
        mappings = {}
        for ci in self.issue._issue_data['i']:
            ci_id = ci['m']['id']
            if 'parts' in ci['l']:
                for part in ci['l']['parts']:
                    mappings[part['comp_id']] = ci_id
        
        pselement = doc.find('PrintSpace')
        page_data = alto.parse_printspace(pselement, mappings)
        self.data['cc'], self.data["r"] = False, page_data
        return
    
    @property
    def xml(self):
        """Returns a BeautifulSoup object with Alto XML of the page."""
        alto_xml_path = os.path.join(self.basedir, self.filename)
        
        with codecs.open(alto_xml_path, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        alto_doc = BeautifulSoup(raw_xml, 'xml')
        return alto_doc
    
    def to_json(self):
        """Validates `page.data` against PageSchema & serializes to string.

        ..note::
            Validation adds a substantial overhead to computing time. For
            serialization of lots of pages it is recommendable to bypass
            schema validation.
        """
        page = Pageschema(**self.data)
        return page.serialize()


class ReroNewspaperIssue(object):
    def __init__(self, issue_dir):
        self.id = "{}-{}-{}".format(
                issue_dir.journal,
                "{}-{}-{}".format(
                        issue_dir.date.year,
                        str(issue_dir.date.month).zfill(2),
                        str(issue_dir.date.day).zfill(2)
                        ),
                issue_dir.edition
                )
        
        self.edition = issue_dir.edition
        self.journal = issue_dir.journal
        self.path = issue_dir.path
        self.date = issue_dir.date
        self._issue_data = {}
        self._notes = []
        self.image_properties = {}
        self.ark_id = None
        self.rights = None
        
        self._find_pages()
        self._parse_mets()
    
    def _find_pages(self):
        """
        Finds the pages associated to the issue and stores them as `ReroNewspaperPage`
        """
        alto_path = os.path.join(self.path, 'ALTO')
        
        if not os.path.exists(alto_path):
            logger.critical(f"Could not find pages for {self.id}")
        
        page_file_names = [file for file in os.listdir(alto_path) if not file.startswith('.') and '.xml' in file]
        
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
                        ReroNewspaperPage(page_no, page_id, filename, alto_path)
                        )
            except Exception as e:
                logger.error(
                        f'Adding page {page_no} {page_id} {filename}',
                        f'raised following exception: {e}'
                        )
                raise e
    
    def _parse_mets_filegroup(self, element) -> Dict:  # TODO: make this function generic
        # return a list of page image ids
        
        return {
                int(child.get("SEQ")): child.get("ADMID")
                for child in element.findAll('file')
                }
    
    def _parse_mets_amdsec(self, mets_doc) -> Dict:  # TODO : make this generic
        """
        Gathers information about each page (size of image)
        :param mets_doc:  BeautifulSoup document of METS.xml
        :return: dict, containing the resolution for each image
        """
        image_filegroup = mets_doc.findAll('fileGrp', {'USE': 'Images'})[0]
        page_image_ids = self._parse_mets_filegroup(image_filegroup)  # Returns {page: im_id}
        
        amd_sections = {
                image_id: mets_doc.findAll('amdSec', {'ID': image_id})[0]  # Returns {page_id: amdsec}
                for image_id in page_image_ids.values()
                }
        
        image_properties_dict = {}
        for image_no, image_id in page_image_ids.items():
            amd_sect = amd_sections[image_id]
            try:
                image_properties_dict[image_no] = {
                        'x_resolution': int(amd_sect.find('XphysScanResolution').text),
                        'y_resolution': int(amd_sect.find('YphysScanResolution').text)
                        }
            # if it fails it's because of value < 1
            except Exception as e:
                logger.debug(f'Error occured when parsing {e}')
                image_properties_dict[image_no] = {
                        'x_resolution': 300,
                        'y_resolution': 300
                        }
        return image_properties_dict
    
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
        Parses a content item div and returns the dictionary representing it
        :param item_div: Div of content item
        :param counter: Number of content items already added (to generate canonical id)
        :return:  dict, of the resulting content item
        """
        div_type = item_div.get('TYPE').lower()
        
        if div_type == 'picture':  # TODO: check if other content items can be translated (maybe add translation dict)
            div_type = CONTENTITEM_TYPE_IMAGE
        
        if div_type not in CONTENTITEM_TYPES:  # Check if new content item is found (or if we need more translation)
            logger.debug(f"Found new content item type: {div_type}")
        
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
        return content_item
    
    def _parse_content_items(self, mets_doc):
        """
        Given the XML document, this function parses all the content items it can find
        :param mets_doc:
        :return:
        """
        content_items = []
        divs = mets_doc.find('div', {'TYPE': 'CONTENT'}).findChildren('div',
                                                                      recursive=False)  # Children of "Content" tag
        sorted_divs = sorted(divs, key=lambda x: x.get('ID').lower())  # Sort to have same naming
        
        found_types = set(x.get('TYPE') for x in sorted_divs)
        print(f"Found types {found_types} for content items")
        
        counter = 1
        for div in sorted_divs:
            content_items.append(self._parse_content_item(div, counter))  # Parse Each contentitem
            counter += 1
        return content_items
    
    def _parse_mets(self):
        """Parses the Mets XML file of the newspaper issue."""
        
        mets_doc = self.xml
        
        self.image_properties = self._parse_mets_amdsec(mets_doc)  # Parse the resolution of page images
        
        content_items = self._parse_content_items(mets_doc)  # Parse all the content items
        
        self._issue_data = {
                "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                "i": content_items,
                "id": self.id,
                "ar": "open_public",  # TODO : change this
                "pp": [p.id for p in self.pages]
                }
    
    @property
    def xml(self):
        mets_file = [os.path.join(self.path, f) for f in os.listdir(self.path) if 'mets.xml' in f.lower()]
        if len(mets_file) == 0:
            logger.critical(f"Could not find METS file in {self.path}")
            return
        
        mets_file = mets_file[0]
        
        with codecs.open(mets_file, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        mets_doc = BeautifulSoup(raw_xml, 'xml')
        return mets_doc
    
    @property
    def issuedir(self) -> IssueDir:
        return IssueDir(self.journal, self.date, self.edition, self.path)
    
    def to_json(self):
        issue = IssueSchema(**self._issue_data)
        return issue.serialize()
