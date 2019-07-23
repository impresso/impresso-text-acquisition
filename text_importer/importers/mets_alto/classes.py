import codecs
import logging
import os
from abc import ABC, abstractmethod
from time import strftime

from bs4 import BeautifulSoup
from impresso_commons.path.path_fs import IssueDir

from typing import Tuple, List

from text_importer.helpers import get_issue_schema, get_page_schema
from text_importer.importers.mets_alto import alto

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)


class MetsAltoNewspaperPage(ABC):
    
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
    
    @property
    def xml(self):
        """Returns a BeautifulSoup object with Alto XML of the page."""
        alto_xml_path = os.path.join(self.basedir, self.filename)
        
        with codecs.open(alto_xml_path, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        alto_doc = BeautifulSoup(raw_xml, 'xml')
        return alto_doc
    
    def to_json(self) -> str:
        """Validates `page.data` against PageSchema & serializes to string.

        ..note::
            Validation adds a substantial overhead to computing time. For
            serialization of lots of pages it is recommendable to bypass
            schema validation.
        """
        page = Pageschema(**self.data)
        return page.serialize()
    
    @abstractmethod
    def add_issue(self, issue):
        pass
    
    def _convert_coordinates(self, page_data) -> Tuple[bool, List[dict]]:
        pass
    
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
        self.data['cc'], self.data["r"] = self._convert_coordinates(page_data)
        return


class MetsAltoNewPaperIssue(ABC):
    
    def __init__(self, issue_dir):
        # create the canonical issue id
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
        self.pages = []
        self.rights = issue_dir.rights
        self.image_properties = {}
        self.ark_id = None
        
        self._find_pages()
        self._parse_mets()
    
    @abstractmethod
    def _find_pages(self):
        pass
    
    @abstractmethod
    def _parse_mets(self):
        pass
    
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
    
    def to_json(self) -> str:
        issue = IssueSchema(**self._issue_data)
        return issue.serialize()
