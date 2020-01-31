import codecs
import logging
import os
from abc import abstractmethod
from time import strftime
from typing import List, Tuple

from bs4 import BeautifulSoup
from impresso_commons.path import IssueDir

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.mets_alto import alto
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)


class MetsAltoNewspaperPage(NewspaperPage):
    """Generic class representing a newspaper page in Alto format.

    .. note ::

        New Mets/Alto importers should sub-classes this class and implement
        its abstract methods (i.e. :meth:`~MetsAltoNewspaperPage.add_issue()`).

    :param str _id: Canonical page ID.
    :param int n: Page number.
    :param str filename: Name of the page Alto XML file.
    :param str basedir: Base directory where Alto files are located.

    """
    
    def __init__(self, _id: str, n: int, filename: str, basedir: str, encoding: str = 'utf-8'):
        super().__init__(_id, n)
        self.filename = filename
        self.basedir = basedir
        self.encoding = encoding
        self.page_data = {
            'id': _id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'r': []  # here go the page regions
            }
    
    @property
    def xml(self) -> BeautifulSoup:
        """Returns a BeautifulSoup object with Alto XML of the page."""
        alto_xml_path = os.path.join(self.basedir, self.filename)
        
        with codecs.open(alto_xml_path, 'r', encoding=self.encoding) as f:
            raw_xml = f.read()
        
        alto_doc = BeautifulSoup(raw_xml, 'xml')
        return alto_doc
    
    # TODO: decide whether to remove this
    def _convert_coordinates(self, page_data: List[dict]) -> Tuple[bool, List[dict]]:
        return False, page_data
    
    @abstractmethod
    def add_issue(self, issue):
        pass
    
    def parse(self):
        doc = self.xml
        
        mappings = {}
        for ci in self.issue.issue_data['i']:
            ci_id = ci['m']['id']
            if 'parts' in ci['l']:
                for part in ci['l']['parts']:
                    mappings[part['comp_id']] = ci_id
        
        pselement = doc.find('PrintSpace')
        page_data, notes = alto.parse_printspace(pselement, mappings)
        self.page_data['cc'], self.page_data["r"] = self._convert_coordinates(
                page_data
                )
        
        if len(notes) > 0:
            self.page_data['n'] = notes

class MetsAltoNewspaperIssue(NewspaperIssue):
    """Generic class representing a newspaper issue in Mets/Alto format.

    .. note ::

        New Mets/Alto importers should sub-class this class and implement
        its abstract methods (i.e. ``_find_pages()``, ``_parse_mets()``).

    :param IssueDir issue_dir: Description of parameter `issue_dir`.

    """
    
    def __init__(self, issue_dir: IssueDir):
        super().__init__(issue_dir)
        # create the canonical issue id
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
        """Returns a BeautifulSoup object with Mets XML file of the issue.

        .. note ::
            By default the issue Mets file is the only file containing
            `mets.xml` in its file name and located in the directory
            `self.path`. Individual importers can overwrite this behavior
            if necessary.
        """
        mets_file = [
            os.path.join(self.path, f)
            for f in os.listdir(self.path)
            if 'mets.xml' in f.lower()
            ]
        if len(mets_file) == 0:
            logger.critical(f"Could not find METS file in {self.path}")
            return
        
        mets_file = mets_file[0]
        
        with codecs.open(mets_file, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        mets_doc = BeautifulSoup(raw_xml, 'xml')
        return mets_doc
