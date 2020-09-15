import logging
import os
from time import strftime
from zipfile import ZipFile

from text_importer.importers.classes import NewspaperIssue, ZipArchive
from text_importer.importers.mets_alto.alto import parse_printspace
from text_importer.importers.mets_alto.classes import MetsAltoNewspaperPage
from text_importer.importers.swa.detect import SwaIssueDir

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URL = "https://ub-sipi.ub.unibas.ch/impresso"
SWA_XML_ENCODING = "utf-8-sig"


class SWANewspaperPage(MetsAltoNewspaperPage):
    """Class representing a newspaper page in SWA data.

    :param str alto_path: Full path to the Alto XML file.
    """
    
    def __init__(self, _id: str, number: int, alto_path: str):
        self.alto_path = alto_path
        basedir, filename = os.path.split(alto_path)
        super().__init__(_id, number, filename, basedir, encoding=SWA_XML_ENCODING)
        self.iiif = os.path.join(IIIF_ENDPOINT_URL, filename.split('.')[0])
        self.page_data['iiif'] = self.iiif
    
    def add_issue(self, issue: NewspaperIssue):
        self.issue = issue
    
    @property
    def ci_id(self) -> str:
        """
        Return the content item ID of the page.

        Given that SWA data do not entail article-level segmentation,
        each page is considered as a content item. Thus, to mint the content
        item ID we take the canonical page ID and simply replace the "p"
        prefix with "i".

        :return: str Content item id
        """
        split = self.id.split('-')
        split[-1] = split[-1].replace('p', 'i')
        return "-".join(split)
    
    @property
    def file_exists(self) -> bool:
        """ Checks whether the ALTO file exists for this page
        
        :return:
        """
        return os.path.exists(self.alto_path) and os.path.isfile(self.alto_path)
    
    def parse(self):
        doc = self.xml
        pselement = doc.find('PrintSpace')
        ci_id = self.ci_id
        
        mappings = {k.get('ID'): ci_id for k in pselement.findAll('TextBlock')}
        page_data, notes = parse_printspace(pselement, mappings)
        
        self.page_data['cc'], self.page_data['r'] = True, page_data
        
        # Add notes to page data
        if len(notes) > 0:
            self.page_data['n'] = notes
        return notes
    
    def get_iiif_image(self):
        return os.path.join(self.iiif, "full/full/0/default.jpg")


class SWANewspaperIssue(NewspaperIssue):
    """Class representing a SWA Newspaper Issue.

    .. note ::

        SWA is in ALTO format, but there isn't any Mets file. So in that case,
        issues are simply a collection of pages.

    :param SwaIssueDir issue_dir: SwaIssueDir of the current Issue
    :param str temp_dir: Temporary directory to extract archives

    """
    
    def __init__(self, issue_dir: SwaIssueDir, temp_dir: str):
        super().__init__(issue_dir)
        self.archive = self._parse_archive(temp_dir)
        self.temp_pages = issue_dir.pages
        self.content_items = []
        
        self._notes = []
        self._find_pages()
        self._find_content_items()
        
        self.issue_data = {
            'id': self.id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'i': self.content_items,
            'ar': self.rights,
            'pp': [p.id for p in self.pages],
            'notes': self._notes
            }
    
    def _parse_archive(self, temp_dir: str) -> ZipArchive:
        if os.path.isfile(self.path):
            try:
                archive = ZipFile(self.path)
                logger.debug(
                        f"Contents of archive for {self.id}: {archive.namelist()}"
                        )
                return ZipArchive(archive, temp_dir)
            except Exception as e:
                msg = f"Bad Zipfile for {self.id}, failed with error : {e}"
                raise ValueError(msg)
        else:
            msg = f"Could not find archive {self.path} for {self.id}"
            raise ValueError(msg)
    
    def _find_pages(self):
        for n, val in enumerate(sorted(self.temp_pages)):
            page_id, page_path = val
            page_path = os.path.join(self.archive.dir, page_path)
            page = SWANewspaperPage(page_id, n + 1, page_path)
            
            # Check page existence
            if not page.file_exists:
                self._notes.append(f"Alto file for {page_id} missing {page_path}")
            else:
                self.pages.append(page)
            
        if len(self.pages) == 0:
            raise ValueError(f"Could not find any page for {self.id}")
    
    def _find_content_items(self):
        for page in sorted(self.pages, key=lambda x: x.id):
            page_number = page.number
            ci_id = self.id + '-i' + str(page_number).zfill(4)
            ci = {
                'm': {
                    'id': ci_id,
                    'pp': [page_number],
                    'tp': 'page',
                    }
                }
            self.content_items.append(ci)
