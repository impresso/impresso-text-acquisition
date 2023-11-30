"""This module contains the definition of generic Mets/Alto importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
Mets/Alto format to a unified canoncial format.
The classes in this module are meant to be subclassed to handle independently
the parsing for each version of the Mets/Atlo format and their specificities.
"""

import codecs
import logging
import os
from abc import abstractmethod
from time import strftime
from typing import Any

from bs4 import BeautifulSoup
from impresso_commons.path import IssueDir

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.mets_alto import alto
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)


class MetsAltoNewspaperPage(NewspaperPage):
    """Newspaper page in generic Alto format.

    Note: 
        New Mets/Alto importers should sub-classes this class and implement
        its abstract methods (i.e. :meth:`~MetsAltoNewspaperPage.add_issue()`).

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (NewspaperIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file.
    """
    
    def __init__(self, _id: str, number: int, filename: str,
                 basedir: str, encoding: str = 'utf-8') -> None:
        super().__init__(_id, number)
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
        """Read Alto XML file of the page and create a BeautifulSoup object.

        Returns:
            BeautifulSoup: BeautifulSoup object with Alto XML of the page.
        """
        alto_xml_path = os.path.join(self.basedir, self.filename)
        
        with codecs.open(alto_xml_path, 'r', encoding=self.encoding) as f:
            raw_xml = f.read()
        
        alto_doc = BeautifulSoup(raw_xml, 'xml')
        return alto_doc
    
    def _convert_coordinates(self, page_regions: list[dict[str, Any]]
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Convert region coordinates to iiif format if possible.

        Args:
            page_regions (list[dict[str, Any]]): Page regions from canonical
                Page format.

        Returns:
            tuple[bool, list[dict[str, Any]]]: Whether the region coordinates
                are in iiif format and page regions.
        """
        return True, page_regions
    
    @abstractmethod
    def add_issue(self, issue: NewspaperIssue) -> None:
        pass
    
    def parse(self) -> None:
        doc = self.xml
        
        mappings = {}
        for ci in self.issue.issue_data['i']:
            ci_id = ci['m']['id']
            if 'parts' in ci['l']:
                for part in ci['l']['parts']:
                    mappings[part['comp_id']] = ci_id
        
        pselement = doc.find('PrintSpace')
        page_regions, notes = alto.parse_printspace(pselement, mappings)
        self.page_data['cc'], self.page_data["r"] = self._convert_coordinates(
            page_regions
        )
        # Add notes for missing coordinates in SWA
        if len(notes) > 0:
            self.page_data['n'] = notes


class MetsAltoNewspaperIssue(NewspaperIssue):
    """Newspaper issue in generic Mets/Alto format.

    Note: 
        New Mets/Alto importers should sub-class this class and implement
        its abstract methods (i.e. ``_find_pages()``, ``_parse_mets()``).

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        journal (str): Newspaper unique identifier or name.
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`NewspaperPage` instances from this issue.
        rights (str): Access rights applicable to this issue.
        image_properties (dict[str, Any]): metadata allowing to convert region 
            OCR/OLR coordinates to iiif format compliant ones.
        ark_id (int): Issue ARK identifier, for the issue's pages' iiif links.
    """
    
    def __init__(self, issue_dir: IssueDir) -> None:
        super().__init__(issue_dir)
        # create the canonical issue id
        self.image_properties = {}
        self.ark_id = None
        
        self._find_pages()
        self._parse_mets()
    
    @abstractmethod
    def _find_pages(self) -> None:
        pass
    
    @abstractmethod
    def _parse_mets(self) -> None:
        """Parse the Mets XML file corresponding to this issue.
        """
        pass

    @property
    def xml(self) -> BeautifulSoup:
        """Read Mets XML file of the issue and create a BeautifulSoup object.

        Note:
            By default the issue Mets file is the only file containing
            `mets.xml` in its file name and located in the directory
            `self.path`. Individual importers can overwrite this behavior
            if necessary.
    
        Returns:
            BeautifulSoup: BeautifulSoup object with Mets XML of the issue.
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
