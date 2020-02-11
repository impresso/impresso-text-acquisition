import codecs
import logging
import os
from time import strftime

from bs4 import BeautifulSoup

from text_importer.importers.bcul.helpers import find_mit_file, get_page_number, get_div_coords
from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers import CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPE_TABLE

logger = logging.getLogger(__name__)

BCUL_IMAGE_TYPE = "Picture"


class BCULNewspaperPage(NewspaperPage):
    
    def __init__(self, _id: str, number: int, page_path: str):
        super().__init__(_id, number)
        self.path = page_path
    
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
    
    def get_image_divs(self):
        return self.xml.findAll("block", {"blockType": BCUL_IMAGE_TYPE})
    
    def parse(self):
        pass


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
        
        self.content_items = []
        
        self._find_pages()
        self._find_content_items()
        
        self.issue_data = {
            'id': self.id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'i': self.content_items,
            'ar': self.rights,
            'pp': [p.id for p in self.pages]
            }
    
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
                    page = BCULNewspaperPage(page_id, page_no, page_path)
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
            page = BCULNewspaperPage(page_id, page_no, page_path)
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
    
    def _find_images(self, start_counter):
        # Get all images
        for p in sorted(self.pages, key=lambda x: x.number):
            page_images = [get_div_coords(div) for div in p.get_image_divs()]  # Get page
            for im in sorted(page_images):
                ci_id = self.id + '-i' + str(start_counter).zfill(4)
                
                ci = {
                    'm': {
                        'id': ci_id,
                        'pp': [p.number],
                        'tp': CONTENTITEM_TYPE_IMAGE,
                        'c': im,
                        }
                    }
                self.content_items.append(ci)
                start_counter += 1
