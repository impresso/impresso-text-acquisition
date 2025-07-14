"""This module contains the definition of the SWA importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the SWA version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
from time import strftime
from zipfile import ZipFile

from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers.classes import CanonicalIssue, ZipArchive
from text_preparation.importers.mets_alto.alto import parse_printspace
from text_preparation.importers.mets_alto.classes import MetsAltoCanonicalPage
from text_preparation.importers.swa.detect import SwaIssueDir

logger = logging.getLogger(__name__)
IIIF_IMG_BASE_URI = "https://ub-sipi.ub.unibas.ch/impresso"
IIIF_PRES_BASE_URI = "https://ub-iiifpresentation.ub.unibas.ch/impresso_sb"
IIIF_MANIFEST_SUFFIX = "manifest"
SWA_XML_ENCODING = "utf-8-sig"


class SWANewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in SWA (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        alto_path (str): Full path to the Alto XML file.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file.
        iiif (str): The iiif URI to the newspaper page image.
    """

    def __init__(self, _id: str, number: int, alto_path: str) -> None:
        self.alto_path = alto_path
        basedir, filename = os.path.split(alto_path)
        super().__init__(_id, number, filename, basedir, encoding=SWA_XML_ENCODING)
        self.iiif = os.path.join(IIIF_IMG_BASE_URI, filename.split(".")[0])
        self.page_data["iiif_img_base_uri"] = self.iiif
        # TODO add page width & height

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue

    @property
    def ci_id(self) -> str:
        """Return the content item ID of the page.

        Given that SWA data do not entail article-level segmentation,
        each page is considered as a content item. Thus, to mint the content
        item ID we take the canonical page ID and simply replace the "p"
        prefix with "i".

        Returns:
            str: Content item id.
        """
        split = self.id.split("-")
        split[-1] = split[-1].replace("p", "i")
        return "-".join(split)

    @property
    def file_exists(self) -> bool:
        """Check whether the Alto XML file exists for this page.

        Returns:
            bool: True if the Alto XML file exists, False otherwise.
        """
        return os.path.exists(self.alto_path) and os.path.isfile(self.alto_path)

    def parse(self) -> None:
        doc = self.xml
        pselement = doc.find("PrintSpace")
        ci_id = self.ci_id

        mappings = {k.get("ID"): ci_id for k in pselement.findAll("TextBlock")}
        page_data, notes = parse_printspace(pselement, mappings)

        self.page_data["cc"], self.page_data["r"] = True, page_data

        # Add notes to page data
        if len(notes) > 0:
            self.page_data["n"] = notes
        return notes

    def get_iiif_image(self) -> str:
        """Create the iiif URI to the full journal page image.

        Returns:
            str: iiif URI of the image of the full page.
        """
        return os.path.join(self.iiif, "full/full/0/default.jpg")


class SWANewspaperIssue(CanonicalIssue):
    """Newspaper issue in SWA Mets/Alto format.

    Note:
        SWA is in ALTO format, but there isn't any Mets file. So in that case,
        issues are simply a collection of pages.

    Args:
        issue_dir (SwaIssueDir): Identifying information about the issue.
        temp_dir (str): Temporary directory to extract archives.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`CanonicalPage` instances from this issue.
        archive (ZipArchive): Archive containing all the Alto XML files for the
            issue's pages.
        temp_pages (list[tuple[str, str]]): Temporary list of pages found for
            this issue. A page is a tuple (page_canonical_id, alto_path), where
            alto_path is the path from within the archive.
        content_items (list[dict[str,Any]]): Content items from this issue.
        notes (list[str]): Notes of missing pages gathered while parsing.
    """

    def __init__(self, issue_dir: SwaIssueDir, temp_dir: str) -> None:
        super().__init__(issue_dir)
        self.archive = self._parse_archive(temp_dir)
        self.temp_pages = issue_dir.pages
        self.content_items = []

        self._notes = []
        self._find_pages()
        self._find_content_items()

        iiif_manifest = os.path.join(IIIF_PRES_BASE_URI, f"{self.id}-issue", IIIF_MANIFEST_SUFFIX)

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "iiif_manifest_uri": iiif_manifest,
            "notes": self._notes,
        }

    def _parse_archive(self, temp_dir: str) -> ZipArchive:
        """Open and parse the Zip archive located at :attr:`path` if possible.

        Args:
            temp_dir (str): Temporary directory in which to unpack the archive.

        Raises:
            ValueError: The Zip archive at :attr:`path` could not be opened.
            ValueError: No Zip archive was found at given :attr:`path`.

        Returns:
            ZipArchive: Archive containing the pages XML files for the issue.
        """
        if os.path.isfile(self.path):
            try:
                archive = ZipFile(self.path)
                logger.debug("Contents of archive for %s: %s", self.id, archive.namelist())
                return ZipArchive(archive, temp_dir)
            except Exception as e:
                msg = f"Bad Zipfile for {self.id}, failed with error : {e}"
                raise ValueError(msg)
        else:
            msg = f"Could not find archive {self.path} for {self.id}"
            raise ValueError(msg)

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`SWANewspaperPage` instances are added to :attr:`pages`.

        Raises:
            ValueError: No page was found for this issue..
        """
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

    def _find_content_items(self) -> None:
        """Create content items for the pages in this issue.

        Given that SWA data do not entail article-level segmentation,
        each page is considered as a content item.
        """
        for page in sorted(self.pages, key=lambda x: x.id):
            page_number = page.number
            ci_id = self.id + "-i" + str(page_number).zfill(4)
            ci = {
                "m": {
                    "id": ci_id,
                    "pp": [page_number],
                    "tp": "page",
                }
            }
            self.content_items.append(ci)
