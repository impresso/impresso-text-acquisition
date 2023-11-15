"""This module contains the definition of abstract importer classes.

In particular, the classes define newspaper Issues and Pages objects which
convert OCR data in various formats (Olive, Mets/Alto, Tetml...) to a unified
Impresso canoncial format, allowing to process and create a large corpus of 
digitized historical newspapers.
The classes in this module are meant to be subclassed to handle independently
the parsing for each OCR format.
"""

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

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        journal (str): Newspaper unique identifier or name.
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): List of :obj:`NewspaperPage` instances from this issue.
        rights (str): Access rights applicable to this issue.
    """
    
    def __init__(self, issue_dir: IssueDir) -> None:
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
    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`NewspaperPage` instances are added to :attr:`pages`.
        """
        pass
    
    @property
    def issuedir(self) -> IssueDir:
        """`IssueDir`: IssueDirectory corresponding to this issue.
        """
        return IssueDir(self.journal, self.date, self.edition, self.path)
    
    def to_json(self) -> str:
        """Validate ``self.issue_data`` & serialize it to string.

        Note:
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

    Args: 
        _id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (NewspaperIssue | None): Issue this page is from.
    """
    
    def __init__(self, _id: str, number: int) -> None:
        self.id = _id
        self.number = number
        self.page_data = {}
        self.issue = None
    
    def to_json(self) -> str:
        """Validate ``self.page.data`` & serialize it to string.

        Note:
            Validation adds a substantial overhead to computing time. For
            serialization of large amounts of pages it is recommendable to
            bypass schema validation.
        """
        page = Pageschema(**self.page_data)
        return page.serialize()
    
    @abstractmethod
    def add_issue(self, issue: NewspaperIssue) -> None:
        """Add to a page object its parent, i.e. the newspaper issue.

        This allows each page to preserve contextual information coming from
        the newspaper issue.

        Args:
            issue (NewspaperIssue): Newspaper issue containing this page.
        """
        pass
    
    @abstractmethod
    def parse(self) -> None:
        """Process the page XML file and transform into canonical Page format.

        Note:
            This lazy behavior means that the page contents are not processed
            upon creation of the page object, but only once the ``parse()``
            method is called.
        """
        pass


class ZipArchive(object):
    """Archive document to be temporarily unpacked.
    
    It is usually unpacked into a temp directory to avoid jamming the memory.

    Args:
        archive (ZipFile): Zip archive containing files with OCR data.
        temp_dir (str): Directory used for temporary storage of the contents.

    Attributes:
        name_list (list[str]): List of filenames in the archive.
        dir (str): Path to directory in which archive contents are.
    """
    
    def __init__(self, archive: ZipFile, temp_dir: str) -> None:
        logger.debug(f"Extracting archive in {temp_dir}")
        self.name_list = archive.namelist()
        self.dir = temp_dir
        self.extract_archive(archive)
        archive.close()
    
    def extract_archive(self, archive: ZipFile) -> None:
        """Recursively extract all files from the archive.

        Args:
            archive (ZipFile): Archive to unpack.
        """
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
    
    def namelist(self) -> list[str]:
        """list[str]: List of filenames in the archive.
        """
        return self.name_list
    
    def read(self, file: str) -> bytes:
        """Read given file in binary mode.

        Args:
            file (str): File to read.

        Returns:
            bytes: File contents as bytes.
        """
        path = os.path.join(self.dir, file)
        with open(path, 'rb') as f:
            f_bytes = f.read()
        return f_bytes
    
    def cleanup(self) -> None:
        """Recursively delete the unpacked archive.
        """
        logging.info(f"Deleting archive {self.dir}")
        shutil.rmtree(self.dir)
        prev_dir = os.path.split(self.dir)[0]
        while os.path.isdir(prev_dir) and len(os.listdir(prev_dir)) == 0:
            logging.info(f"Deleting {prev_dir}")
            os.rmdir(prev_dir)
            prev_dir = os.path.split(prev_dir)[0]
