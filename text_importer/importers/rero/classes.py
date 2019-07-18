import codecs
import logging
import os
import re
from time import strftime

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from impresso_commons.path.path_fs import IssueDir

from text_importer.helpers import get_issue_schema, get_page_schema
from text_importer.importers.lux import alto
from text_importer.importers.lux.helpers import convert_coordinates, encode_ark
from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers import CONTENTITEM_TYPE_TABLE
from text_importer.importers import CONTENTITEM_TYPE_ARTICLE
from text_importer.importers import CONTENTITEM_TYPE_WEATHER
from text_importer.importers import CONTENTITEM_TYPE_OBITUARY
from text_importer.importers import CONTENTITEM_TYPE_ADVERTISEMENT

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)


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
    
    @property
    def xml(self):
        """Returns a BeautifulSoup object with Alto XML of the page."""
        alto_xml_path = os.path.join(self.basedir, self.filename)
        
        with codecs.open(alto_xml_path, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        alto_doc = BeautifulSoup(raw_xml, 'xml')
        return alto_doc


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
        
        self._find_pages()
    
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
    def issuedir(self):
        return IssueDir(self.journal, self.date, self.edition, self.path)
    
    def _find_pages(self):
        alto_path = os.path.join(self.path, 'ALTO')
        
        if not os.path.exists(alto_path):
            logger.critical(f"Could not find pages for {self.path}")
        
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
            
    def _parse_mets(self):
        """Parses the Mets XML file of the newspaper issue."""

        mets_file = [
            os.path.join(self.path, f)
            for f in os.listdir(self.path)
            if 'mets.xml' in f
        ][0]

        with codecs.open(mets_file, 'r', "utf-8") as f:
            raw_xml = f.read()

        mets_doc = BeautifulSoup(raw_xml, 'xml')
        
        # TODO: Implement METS parsing
