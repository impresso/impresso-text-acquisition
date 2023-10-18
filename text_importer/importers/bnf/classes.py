import codecs
import gzip
import logging
import os
from glob import glob
from time import strftime
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from impresso_commons.path import IssueDir

from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers.bnf.helpers import BNF_CONTENT_TYPES, add_div, type_translation
from text_importer.importers.bnf.parsers import parse_div_parts, parse_embedded_cis, parse_printspace
from text_importer.importers.mets_alto import MetsAltoNewspaperIssue, MetsAltoNewspaperPage
from text_importer.importers.mets_alto.alto import distill_coordinates, parse_style
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URL = "https://gallica.bnf.fr/iiif"
IIIF_MANIFEST_SUFFIX = "full/full/0/manifest.json"
IIIF_IMAGE_SUFFIX = "full/0/default.jpg"


class BnfNewspaperPage(MetsAltoNewspaperPage):
    """Class representing a page in BNF data."""
    
    def __init__(self, _id: str, n: int, filename: str, basedir: str):
        
        self.is_gzip = filename.endswith("gz")
        super().__init__(_id, n, filename, basedir)
        self.ark_link = self.xml.find("fileIdentifier").getText()
    
    def _parse_font_styles(self):
        """ Parses the styles at page level"""
        style_divs = self.xml.findAll("TextStyle")
        
        styles = []
        for d in style_divs:
            styles.append(parse_style(d))
        
        self.page_data['s'] = styles
    
    def add_issue(self, issue: MetsAltoNewspaperIssue):
        """Sets the `issue` attribute for the current Page.

        :param MetsAltoNewspaperIssue issue: The parent Issue
        """
        self.issue = issue
        iiif_url = os.path.join(IIIF_ENDPOINT_URL, self.ark_link, IIIF_MANIFEST_SUFFIX)
        self.page_data['iiif'] = iiif_url
        self._parse_font_styles()
    
    def parse(self):
        """Parses the page into JSON, using the parent issue's content items"""
        doc = self.xml
        
        mappings = {}
        for ci in self.issue.issue_data['i']:
            ci_id = ci['m']['id']
            if 'parts' in ci['l']:
                for part in ci['l']['parts']:
                    mappings[part['comp_id']] = ci_id
        
        pselement = doc.find('PrintSpace')
        page_data, notes = parse_printspace(pselement, mappings)
        self.page_data['cc'], self.page_data["r"] = self._convert_coordinates(
                page_data
                )
        if len(notes) > 0:
            self.page_data['n'] = notes
    
    @property
    def xml(self) -> BeautifulSoup:
        """ Redefined as for some issues, the pages are in gz format
        
        :return:
        """
        if not self.is_gzip:
            return super(BnfNewspaperPage, self).xml
        else:
            alto_xml_path = os.path.join(self.basedir, self.filename)
            with gzip.open(alto_xml_path, 'r') as f:
                raw_xml = f.read()
            
            alto_doc = BeautifulSoup(raw_xml, 'xml')
            return alto_doc


class BnfNewspaperIssue(MetsAltoNewspaperIssue):
    """Class representing an issue in BNF data.
    All functions defined in this child class are specific to parsing BNF Mets/Alto format
    """
    
    def __init__(self, issue_dir: IssueDir):
        self.issue_uid = os.path.basename(issue_dir.path)
        self.secondary_date = issue_dir.secondary_date
        super().__init__(issue_dir)
    
    @property
    def xml(self) -> BeautifulSoup:
        """Returns a BeautifulSoup object with Mets XML file of the issue."""
        mets_regex = os.path.join(self.path, "toc", f"*{self.issue_uid}.xml")
        mets_file = glob(mets_regex)
        if len(mets_file) == 0:
            logger.critical(f"Could not find METS file in {self.path}")
            return None
        mets_file = mets_file[0]
        with codecs.open(mets_file, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        mets_doc = BeautifulSoup(raw_xml, 'xml')
        return mets_doc
    
    def _find_pages(self):
        """Finds all the pages associated to the current issue, and stores them in `pages` attribute"""
        ocr_path = os.path.join(self.path, "ocr")  # Pages in `ocr` folder
        
        pages = [
            (file, int(file.split('.')[0][1:]))
            for file in os.listdir(ocr_path)
            if not file.startswith('.') and '.xml' in file
            ]
        
        page_filenames, page_numbers = zip(*pages)
        
        page_canonical_names = ["{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers]
        
        self.pages = {}
        for filename, page_no, page_id in zip(page_filenames, page_numbers, page_canonical_names):
            try:
                self.pages[page_no] = BnfNewspaperPage(page_id, page_no, filename, ocr_path)
            except Exception as e:
                logger.error(
                        f'Adding page {page_no} {page_id} {filename}',
                        f'raised following exception: {e}'
                        )
                raise e
    
    def _get_divs_by_type(self) -> Dict[str, List[Tuple[str, str]]]:
        """Parses the `dmdSec` tags from the METS file, and sorts them by type. Then, it searches for `div` tags
        in the `content` of the `structMap` that do not have the `DMDID` attribute, and for which the type is in
        `BNF_CONTENT_TYPES`. It finally flattens the sections into what they actually contain, and also adds the flattened
        to the return dict.

        :return: dict[str, List[Tuple[str, str]]: All the `div` sorted by type, and the values are the List of
                                                (div_id, div_label)
        """
        mets = self.xml
        dmd_sections = [x for x in mets.findAll("dmdSec") if x.find("mods")]
        struct_map = mets.find("structMap", {"TYPE": "logical"})
        struct_content = struct_map.find("div", {"TYPE": "CONTENT"})
        
        by_type = {}
        
        # First parse DMD section and keep DIV IDs of referenced items
        for s in dmd_sections:  # Iterate on the DMD section
            divs = struct_map.findAll("div", {"DMDID": s.get("ID")})
            
            if len(divs) > 1:  # Means this DMDID is a class of objects
                if s.find("mods:classification") is not None:
                    _type = s.find("mods:classification").getText().lower()
                    for d in divs:
                        by_type = add_div(by_type, _type, d.get("ID"), d.get("LABEL"))
                else:
                    logger.warning(f"MultiDiv with no classification for {self.id}")
            else:
                div = divs[0]
                _type = div.get("TYPE").lower()
                by_type = add_div(by_type, _type, div.get("ID"), div.get("LABEL"))
        
        # Parses div sections that are direct children of CONTENT in the logical structMap,
        # and keeps the ones without DMDID
        for c in struct_content.findChildren("div", recursive=False):
            if c.get("DMDID") is None and c.get("TYPE") is not None:
                _type = c.get("TYPE").lower()
                by_type = add_div(by_type, _type, c.get("ID"), c.get("LABEL"))
        
        if 'section' in by_type:
            by_type = self._flatten_sections(by_type, struct_content)
        
        return by_type
    
    def _flatten_sections(self, by_type: dict, struct_content) -> Dict[str, List[Tuple[str, str]]]:
        """ Flattens the sections of the issue (i.e. make the children parts standalone CIs)
        
        :param by_type:
        :param struct_content:
        :return:
        """
        # Flatten the sections
        for div_id, lab in by_type['section']:
            div = struct_content.find("div", {"ID": div_id})  # Get all divs of this section
            for d in div.findChildren("div", recursive=False):
                dmdid = d.get("DMDID")
                div_id = d.get("ID")
                ci_type = d.get("TYPE").lower()
                d_label = d.get("LABEL")
                if dmdid is None and ci_type in BNF_CONTENT_TYPES:  # This div needs to be added to the content items
                    by_type = add_div(by_type, ci_type, div_id, d_label or lab)
                elif dmdid is None:
                    logging.debug(f" {self.id}: {div_id} of type {ci_type} within section is not in CONTENT_TYPES")
        del by_type['section']
        return by_type
    
    def _parse_div(self, div_id: str, div_type: str, label: str, item_counter: int) -> Tuple[List[dict], int]:
        """Parses the given `div_id` from the `structMap` of the METS file.

        :param str div_id: Unique ID of the div to parse
        :param str div_type: Type of the div (should be in `BNF_CONTENT_TYPES`=
        :param str label: Label of the div (title)
        :param int item_counter: The current counter for CI IDs
        :return:
        """
        article_div = self.xml.find("div", {"ID": div_id})  # Get the tag
        article_div = article_div.find("div", {"TYPE": "BODY"}) \
                      or article_div  # Try to get the body if there is one (we discard headings)
        parts = parse_div_parts(article_div)  # Parse the parts of the tag
        metadata = None
        if len(parts) > 0:  # If parts were found, create content item for this DIV
            article_id = "{}-i{}".format(self.id, str(item_counter).zfill(4))
            metadata = {
                'id': article_id,
                'tp': type_translation[div_type],
                'pp': [],
                }
            if label is not None:
                metadata['t'] = label
            ci = {
                'm': metadata,
                'l': {
                    'parts': parts
                    }
                }
            item_counter += 1
        else:  # Otherwise, only parse embedded CIs
            article_id = None
        embedded, item_counter = parse_embedded_cis(article_div, label, self.id, article_id, item_counter)
        
        if metadata is not None:
            embedded.append(ci)
        return embedded, item_counter
    
    def _get_image_iiif_link(self, ci_id: str, parts: List) -> tuple[str]:
        """ Gets the image iiif link given the ID of the CI (
        
        :param str ci_id: The ID of the image CI
        :param list parts: Parts of the image
        :return: The IIIF link to the image
        """
        image_part = [p for p in parts if p['comp_role'] == CONTENTITEM_TYPE_IMAGE]
        iiif_link = None
        if len(image_part) == 0:
            logger.warning(f"Content item {ci_id} of type {CONTENTITEM_TYPE_IMAGE} does not have image part.")
        elif len(image_part) > 1:
            logger.warning(f"Content item {ci_id} of type {CONTENTITEM_TYPE_IMAGE} has multiple image parts.")
        else:
            
            image_part_id = image_part[0]['comp_id']
            page = self.pages[image_part[0]['comp_page_no']]
            block = page.xml.find("Illustration", {"ID": image_part_id})
            if block is None:
                logger.warning(f"Could not find image {image_part_id} for CI {ci_id}")
            else:
                coords = distill_coordinates(block)
                iiif_link = os.path.join(IIIF_ENDPOINT_URL, page.ark_link, "info.json")
        return coords, iiif_link
    
    def _parse_mets(self):
        """Parses the METS file for the current Issue"""
        
        # First get all the divs by type
        by_type = self._get_divs_by_type()
        item_counter = 1
        content_items = []
        
        # Then start parsing them
        for div_type, divs in by_type.items():
            for div_id, div_label in divs:
                cis, item_counter = self._parse_div(div_id, div_type, div_label, item_counter)
                content_items += cis
        
        # Finally add the pages and iiif link
        for x in content_items:
            x['m']['pp'] = list(set(c['comp_page_no'] for c in x['l']['parts']))
            if x['m']['tp'] == CONTENTITEM_TYPE_IMAGE:
                x['c'], x['m']['iiif_link'] = self._get_image_iiif_link(
                    x['m']['id'], x['l']['parts']
                )
        
        self.pages = list(self.pages.values())
        
        self.issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "i": content_items,
            "id": self.id,
            "ar": self.rights,
            "pp": [p.id for p in self.pages]
            }
        # Note for newspapers with two dates (197 cases)
        if self.secondary_date is not None:
            self.issue_data['n'] = ["Secondary date {}".format(self.secondary_date)]
