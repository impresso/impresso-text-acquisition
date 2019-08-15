import logging
import os
from time import strftime
from zipfile import ZipFile

from text_importer.importers.classes import Archive, NewspaperIssue
from text_importer.importers.mets_alto.alto import parse_printspace
from text_importer.importers.mets_alto.classes import MetsAltoNewspaperPage
from text_importer.importers.swa.detect import SwaIssueDir

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URL = "https://ub-sipi.ub.unibas.ch/impresso"


class SWANewspaperPage(MetsAltoNewspaperPage):
    def __init__(self, _id: str, number: int, alto_path: str):
        basedir, filename = os.path.split(alto_path)
        super().__init__(_id, number, filename, basedir)
        self.iiif = os.path.join(IIIF_ENDPOINT_URL, filename.split('.')[0])
        self.page_data['iiif'] = self.iiif
    
    def add_issue(self, issue: NewspaperIssue):
        self.issue = issue
    
    @property
    def ci_id(self):
        split = self.id.split('-')
        split[-1] = split[-1].replace('p', 'i')
        return "-".join(split)
    
    def parse(self):
        doc = self.xml
        pselement = doc.find('PrintSpace')
        ci_id = self.ci_id
        
        mappings = {k.get('ID'): ci_id for k in pselement.findAll('TextBlock')}
        page_data = parse_printspace(pselement, mappings)
        
        self.page_data['cc'], self.page_data['r'] = False, page_data
        
        if all(len(p.page_data['r']) > 0 for p in self.issue.pages):
            self.issue.archive.cleanup()
    
    def get_iiif_image(self):
        return os.path.join(self.iiif, "full/full/0/default.jpg")


class SWANewspaperIssue(NewspaperIssue):
    
    def __init__(self, issue_dir: SwaIssueDir, temp_dir: str):
        super().__init__(issue_dir)
        self.archive = self._parse_archive(temp_dir)
        self.temp_pages = issue_dir.pages
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
    
    def _parse_archive(self, temp_dir: str) -> Archive:
        if os.path.isfile(self.path):
            try:
                archive = ZipFile(self.path)
                logger.debug(f"Contents of archive for {self.id}: {archive.namelist()}")
                return Archive(archive, temp_dir)
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
            self.pages.append(SWANewspaperPage(page_id, n + 1, page_path))
    
    def _find_content_items(self):
        for n, page in enumerate(sorted(self.pages, key=lambda x: x.id)):
            ci_id = self.id + '-i' + str(n + 1).zfill(4)
            ci = {
                    'm': {
                            'id': ci_id,
                            'pp': [n + 1],
                            'tp': 'page',
                            }
                    }
            self.content_items.append(ci)
