"""This module contains the definition of abstract classes."""
import logging
import os
import shutil

from abc import ABC, abstractmethod
from zipfile import ZipFile

from impresso_commons.path.path_fs import IssueDir, canonical_path

from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)


class NewspaperIssue(ABC):
    """Abstract class representing a newspaper issue.

    Each text importer needs to define a subclass of `NewspaperIssue` which
    specifies the logic to handle OCR data in a given format (e.g. Olive).
    """
    
    def __init__(self, issue_dir: IssueDir):
        self.id = canonical_path(issue_dir, path_type='dir').replace('/', '-')
        self.edition = issue_dir.edition
        self.journal = issue_dir.journal
        self.path = issue_dir.path
        self.date = issue_dir.date
        self.issue_data = {}
        self._notes = []
        self.pages = []
        self.rights = issue_dir.rights
    
    @abstractmethod
    def _find_pages(self):
        pass
    
    @property
    def issuedir(self) -> IssueDir:
        return IssueDir(self.journal, self.date, self.edition, self.path)
    
    def to_json(self) -> str:
        """Validates ``self.issue_data`` & serializes it to string.

        .. note ::
            Validation adds a substantial overhead to computing time. For
            serialization of large amounts of issues it is recommendable to
            bypass schema validation.
        """
        issue = IssueSchema(**self.issue_data)
        return issue.serialize()


class NewspaperPage(ABC):
    """Abstract class representing a newspaper page.

    Each text importer needs to define a subclass of ``NewspaperPage`` which
    specifies the logic to handle OCR data in a given format (e.g. Alto).
    """
    
    def __init__(self, _id: str, number: int):
        """
        :param str _id: Canonical page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        :param int number: Page number.

        """
        self.id = _id
        self.number = number
        self.page_data = {}
        self.issue = None
    
    def to_json(self) -> str:
        """Validate ``self.page.data`` & serializes it to string.

        .. note ::
            Validation adds a substantial overhead to computing time. For
            serialization of large amounts of pages it is recommendable to
            bypass schema validation.
        """
        page = Pageschema(**self.page_data)
        return page.serialize()
    
    @abstractmethod
    def add_issue(self, issue: NewspaperIssue):
        """Add to a page object its parent, i.e. the newspaper issue.

        This allows each page to preserve contextual information coming from
        the newspaper issue.

        :param NewspaperIssue issue: Newspaper issue contaning this page.
        """
        pass
    
    @abstractmethod
    def parse(self):
        """Process the page file and transform into canonical format.

        .. note ::

            This lazy behavior means that the page contents are not processed
            upon creation of the page object, but only once the ``parse()``
            method is called.
        """
        pass


class Archive(object):
    def __init__(self, archive: ZipFile, temp_dir: str):
        logger.debug(f"Extracting archive in {temp_dir}")
        self.name_list = archive.namelist()
        self.dir = temp_dir
        self.extract_archive(archive)
        archive.close()
    
    def extract_archive(self, archive: ZipFile):
        if not os.path.exists(self.dir):
            logger.debug(f"Creating {self.dir}")
            try:
                os.makedirs(self.dir)
            except FileExistsError as e:
                pass
        for f in archive.filelist:
            if f.file_size > 0:
                try:
                    archive.extract(f.filename, path=self.dir)
                except FileExistsError as e:
                    pass
    
    def namelist(self):
        return self.name_list
    
    def read(self, file):
        path = os.path.join(self.dir, file)
        with open(path, 'rb') as f:
            f_bytes = f.read()
        return f_bytes
    
    def cleanup(self):
        logging.info(f"Deleting archive {self.dir}")
        shutil.rmtree(self.dir)
        prev_dir = os.path.split(self.dir)[0]
        while os.path.isdir(prev_dir) and len(os.listdir(prev_dir)) == 0:
            logging.info(f"Deleting {prev_dir}")
            os.rmdir(prev_dir)
            prev_dir = os.path.split(prev_dir)[0]
