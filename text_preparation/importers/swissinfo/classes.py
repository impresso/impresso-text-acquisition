import logging
import os
import shutil
from time import strftime
from typing import Any

from zipfile import ZipFile

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium
from text_preparation.importers.classes import CanonicalIssue, CanonicalPage
from text_preparation.importers.swissinfo.detect import SwissInfoIssueDir
from impresso_essentials.io.fs_utils import canonical_path

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = "info.json"
IIIF_IMAGE_SUFFIX = "full/full/0/default.jpg"

class SwissInfoRadioBulletinPage(CanonicalPage):
    """Radio-Bulletin Page for SWISSINFO's OCR format.

    Args:
        page_dir (PageDir): Identifying information about the page.

    Attributes:
        id (str): Canonical Page ID (e.g. ``SOC_CJ-1940-01-05-a-p0001``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Radio Bulleting issue this page is from.
        path (str): Path to the json page file.
    """

    def __init__(self, _id: str, number: int, page_path: str) -> None:
        super().__init__(_id, number)
        self.path = page_path
        self.iiif_base_uri = os.path.join(IIIF_ENDPOINT_URI, self.id, IIIF_SUFFIX)
        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "r": [],  # here go the page regions
            "iiif_img_base_uri": self.iiif_base_uri,
        }

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue

    def parse(self) -> None:
        # go from the "ocr_pages" format of the json file to the canonical page regions etc
        # page numbering starts at 1
        ocr_json = self.issue.page_jsons[self.number-1]
        page_regions = self.extract_regions(ocr_json)#[parse_textblock(tb, self.ci_id) for tb in text_blocks]

        self.page_data["cc"] = True
        self.page_data["r"] = page_regions



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
        self.json_file = issue_dir.path
        self._notes = []
        self.pages = []

        self._find_pages()
        self._find_content_item()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "st": SourceType.RB.value,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "n": self._notes,
        }

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`NewspaperPage` instances are added to :attr:`pages`.
        """