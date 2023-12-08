"""This module contains the definition of the BCUL importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the ABBYY format to a unified canoncial format.
"""
import codecs
import logging
import os
from time import strftime
import requests

from bs4 import BeautifulSoup

from text_importer.importers.bcul.helpers import (find_mit_file, get_page_number, 
                                                  get_div_coords, parse_textblock)
from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers import CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPE_TABLE

logger = logging.getLogger(__name__)

BCUL_IMAGE_TYPE = "Picture"
BCUL_TABLE_TYPE = "Table"
BCUL_CI_TYPES = {BCUL_IMAGE_TYPE, BCUL_TABLE_TYPE}
BCUL_CI_TRANSLATION = {
    BCUL_IMAGE_TYPE: CONTENTITEM_TYPE_IMAGE,
    BCUL_TABLE_TYPE: CONTENTITEM_TYPE_TABLE
}
IIIF_PRES_BASE_URI = "https://scriptorium.bcu-lausanne.ch/api/iiif"
IIIF_IMG_BASE_URI = f"{IIIF_PRES_BASE_URI}-img"
IIIF_SUFFIX = 'info.json'
IIIF_MANIFEST_SUFFIX = 'manifest'


class BCULNewspaperPage(NewspaperPage):

    def __init__(self, _id: str, number: int, page_path: str, iiif_uri: str):
        super().__init__(_id, number)
        self.path = page_path
        self.iiif_base_uri = iiif_uri
        self.page_data = {
            'id': _id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'r': [],  # here go the page regions
            'iiif_img_base_uri': iiif_uri,
            }
    
    @property
    def xml(self) -> BeautifulSoup:
        if self.path.endswith("bz2"):
            with codecs.open(self.path, encoding="bz2") as f:
                xml = BeautifulSoup(f, "xml")
        elif self.path.endswith("xml"):
            with open(self.path) as f:
                xml = BeautifulSoup(f, "xml")
        return xml
    
    @property
    def ci_id(self) -> str:
        """
        Return the content item ID of the page.

        Given that BCUL data do not entail article-level segmentation,
        each page is considered as a content item. Thus, to mint the content
        item ID we take the canonical page ID and simply replace the "p"
        prefix with "i".

        :return: str Content item id
        """
        split = self.id.split('-')
        split[-1] = split[-1].replace('p', 'i')
        return "-".join(split)
    
    def add_issue(self, issue: NewspaperIssue):
        self.issue = issue
    
    def get_ci_divs(self):
        return self.xml.findAll("block", {"blockType": lambda x: x in BCUL_CI_TYPES})
    
    def parse(self):
        doc = self.xml
        text_blocks = doc.findAll("block", {"blockType": "Text"})
        page_data = [parse_textblock(tb, self.id) for tb in text_blocks]
        self.page_data["cc"] = True
        self.page_data["r"] = page_data


class BCULNewspaperIssue(NewspaperIssue):
    
    def __init__(self, issue_dir):
        super().__init__(issue_dir)
        self.mit_file = find_mit_file(issue_dir.path)
        self.is_json = self.mit_file.endswith(".json")
        self.is_xml = self.mit_file.endswith(".xml")
        
        # Check if MIT file in one of the two required formats
        if not (self.is_json or self.is_xml):
            logger.error("Mit file {} is not JSON nor XML".format(self.mit_file))
            raise ValueError
        
        # issue manifest, identifier is directory name
        self.iiif_manifest = os.path.join(IIIF_PRES_BASE_URI, 
                                          self.path.split('/')[-1], 
                                          IIIF_MANIFEST_SUFFIX)
        self.content_items = []
        
        self._find_pages()
        self._find_content_items()
        
        self.issue_data = {
            'id': self.id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'i': self.content_items,
            'ar': self.rights,
            'pp': [p.id for p in self.pages],
            'iiif_manifest_uri': self.iiif_manifest
            }

    def _get_iiif_link_json(self, page_path: str) -> str:
        """Return iiif image base uri in case `mit` file is in JSON.

        In this case, the page identifier is simply the page XML file's name.

        Args:
            page_path (str): Path to the page XML file the content item is on.

        Returns:
            str: IIIF image base uri (without suffix).
        """
        # when mit file is a JSON, each page file's name is the iiif identifier
        page_identifier = os.path.basename(page_path).split('.')[0]
        return os.path.join(IIIF_IMG_BASE_URI, page_identifier)

    def _get_iiif_link_xml(self, page_number: int) -> str | None:
        """Return iiif image base uri in case `mit` file is in XML.

        In this case, the iiif URI to images needs to be fetched from the iiif
        presentation API, in the issue's manifest.

        Args:
            page_number (int): Page number for which to fetch the iiif URI.

        Returns:
            str | None: IIIF image base uri (no suffix) or None if request to 
                presentation API failed.
        """
        try:
            response = requests.get(self.iiif_manifest)
            if response.status_code == 200:
                iiif = (
                    response.json()['sequences'][0]['canvases'][page_number]
                        ['images'][0]['resource']['@id']
                )
                # replace suffix mapping to page's image by information json.
                return '/'.join(iiif.split('/')[:-4])
            else:
                logger.error(f"Got {response.status_code} response while "
                             f"querying the IIIF API for {self.id}.")
        except Exception as e:
            logger.error(f'Error while querying IIIF API for {self.id}: {e}.')
        logger.warning(f"No iiif link stored for content item {self.id}.")
        # Exception or wrong HTTP response, so no iiif link to return
        return None
    
    def _find_pages_xml(self):
        """ Finds the pages when the format is XML
        
        :return:
        """
        # List all files and get the filenames from the MIT file
        files = os.listdir(self.path)
        
        with open(self.mit_file) as f:
            mit = BeautifulSoup(f, "xml")
        
        pages = sorted([os.path.basename(x.get('xml')) for x in mit.findAll("image")])
        for p in pages:
            found = False
            # Since files are in .xml.bz2 format, need to check which one corresponds
            for f in files:
                if p in f:
                    page_path = os.path.join(self.path, f)
                    page_no = int(f.split('.')[0].split('_')[-1])
                    page_id = "{}-p{}".format(self.id, str(page_no).zfill(4))
                    page_iiif = self._get_iiif_link_xml(page_no)
                    page = BCULNewspaperPage(page_id, page_no, page_path, page_iiif)
                    self.pages.append(page)
                    found = True
            if not found:
                logger.error("Page {} not found in {}".format(p, self.path))
    
    def _find_pages_json(self):
        """ Finds the pages when the format is JSON
        
        :return:
        """
        # Get all exif files
        files = [os.path.join(self.path, x) for x in os.listdir(self.path) if os.path.splitext(x)[0].endswith("exif")]
        for f in files:
            # Page file is the same name without `_exif`
            file_id = os.path.splitext(os.path.basename(f))[0].replace("_exif", "")
            page_path = os.path.join(self.path, "{}.xml".format(file_id))
            page_no = get_page_number(f)
            page_id = "{}-p{}".format(self.id, str(page_no).zfill(4))
            page_iiif = self._get_iiif_link_json(page_path)
            page = BCULNewspaperPage(page_id, page_no, page_path, page_iiif)
            self.pages.append(page)
    
    def _find_pages(self):
        """ Finds the pages of the current issue
        Since BCUL has two different formats for the issue level, we need to take both into account
        
        :return:
        """
        if self.is_json:
            self._find_pages_json()
        elif self.is_xml:
            self._find_pages_xml()

    def _find_content_items(self):
        # First add the pages as content items
        for n, page in enumerate(sorted(self.pages, key=lambda x: x.number)):
            ci_id = self.id + '-i' + str(n + 1).zfill(4)
            ci = {
                'm': {
                    'id': ci_id,
                    'pp': [page.number],
                    'tp': 'page',
                    }
                }
            self.content_items.append(ci)
        
        n = len(self.content_items) + 1
        # Get all images and tables
        for p in sorted(self.pages, key=lambda x: x.number):
            page_cis = [(div.get('blockType'), get_div_coords(div)) for div in p.get_ci_divs()]  # Get page
            for div_type, coords in sorted(page_cis, key=lambda x: x[1]):  # Sort by coordinates
                ci_id = self.id + '-i' + str(n).zfill(4)

                ci = {
                    'm': {
                        'id': ci_id,
                        'pp': [p.number],
                        'tp': BCUL_CI_TRANSLATION[div_type],
                    },
                }

                # Content item is an image TODO check this works with samples that have images
                if ci['m']['tp'] == CONTENTITEM_TYPE_IMAGE:
                    ci['m']['iiif_link'] = os.path.join(p.iiif_base_uri, IIIF_SUFFIX)
                    ci['c'] = coords

                self.content_items.append(ci)
                n += 1
