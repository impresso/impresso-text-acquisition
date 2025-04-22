import logging
import os
import shutil

from abc import ABC, abstractmethod
from zipfile import ZipFile

from impresso_essentials.utils import IssueDir
from text_preparation.importers import CanonicalIssue, CanonicalPage
from impresso_essentials.io.fs_utils import canonical_path

from text_preparation.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)

class SwissInfoRadioBulletinPage(CanonicalPage):
    """Radio-Bulletin Page for SWISSINFO's OCR format.

    Args:
        page_dir (PageDir): Identifying information about the page.

    Attributes:
        id (str): Canonical Page ID (e.g. ``SOC_CJ-1940-01-05-a-p0001``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Media title unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        page_data (dict[str, Any]): Page data according to canonical format.
    """

    def __init__(self, page_dir: IssueDir) -> None:
        super().__init__(page_dir)
        self.page_data = Pageschema(self.id, self.date, self.alias, self.edition)

class SwissInfoRadioBulletinIssue(CanonicalIssue):
    """Radio-Bulletin Issue for SWISSINFO's OCR format.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``SOC_CJ-1940-01-05-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Media title unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj: `SwissInfoRadioBulletinPage` instances from this issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:   
        super().__init__(issue_dir)
        self.issue_data = IssueSchema(self.id, self.date, self.alias, self.edition)
        self._notes = []
        self.pages = []
        # TODO remove!!
        self.rights = issue_dir.rights
        self._find_pages()

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`NewspaperPage` instances are added to :attr:`pages`.
        """