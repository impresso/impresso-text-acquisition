from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.bcul.helpers import find_mit_file, get_page_number
import logging
import os
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class BCULNewspaperPage(NewspaperPage):
    
    def __init__(self, _id: str, number: int, page_path: str):
        super().__init__(_id, number)
        self.path = page_path
    
    def add_issue(self, issue: NewspaperIssue):
        self.issue = issue
    
    def parse(self):
        print("NTM")


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
    
    def _find_pages_xml(self):
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
        if self.is_json:
            self._find_pages_json()
        elif self.is_xml:
            self._find_pages_xml()
