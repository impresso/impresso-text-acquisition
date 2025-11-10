"""This module contains the definition of SUB importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the SUB version of the Mets/Alto format to a unified canonical format.
These classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
from time import strftime
from typing import Any

from bs4 import BeautifulSoup
from bs4.element import Tag

from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.importers.sub.detect import SubIssueDir

logger = logging.getLogger(__name__)

# SUB-specific constants
IIIF_ENDPOINT_URI = "https://iiif.sub.uni-hamburg.de/object/"
IIIF_MANIFEST_SUFFIX = "/manifest"


class SubNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in SUB (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        page_size (tuple[int, int]): Width and height of the page image.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.

    Attributes:
        id (str): Canonical Page ID (e.g. ``hamb_echo-1888-02-01-a-p0001``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (SubNewspaperIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str): Encoding of XML file.
    """

    def __init__(
        self,
        _id: str,
        number: int,
        filename: str,
        basedir: str,
        page_size: tuple[int, int],
        encoding: str = "utf-8",
    ) -> None:
        super().__init__(_id, number, filename, basedir, encoding)

        # Add the facsimile height and width to the page data
        self.page_data["fw"] = page_size[0]
        self.page_data["fh"] = page_size[1]

    def add_issue(self, issue: "SubNewspaperIssue") -> None:
        """Add the given `SubNewspaperIssue` as an attribute for this class.

        Args:
            issue (SubNewspaperIssue): Issue this page is from
        """
        self.issue = issue
        # Construct IIIF image base URI for this page
        self.page_data["iiif_img_base_uri"] = f"{IIIF_ENDPOINT_URI}{issue.ppn}/manifest"


class SubNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in SUB (Mets/Alto) format.

    All functions defined in this child class are specific to parsing SUB
    Mets/Alto format.

    Args:
        issue_dir (SubIssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``hamb_echo-1888-02-01-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`SubNewspaperPage` instances from this issue.
        ppn (str): PPN identifier from the METS filename.
        title (str): Newspaper title extracted from METS metadata.
    """

    def __init__(self, issue_dir: SubIssueDir) -> None:
        # Initialize attributes to prevent errors
        self.ppn = None
        self.title = None
        self.page_sizes = {}
        
        super().__init__(issue_dir)

    def _extract_ppn_from_mets(self) -> str:
        """Extract PPN identifier from METS filename.
        
        The METS filename follows the pattern: PPN{ppn}_{date}.xml
        
        Returns:
            str: The PPN identifier
            
        Raises:
            ValueError: If no valid METS file is found
        """
        mets_files = [
            f for f in os.listdir(self.path)
            if f.endswith('.xml') and f.startswith('PPN')
        ]
        
        if not mets_files:
            raise ValueError(f"No METS file found in {self.path}")
            
        # Extract PPN from filename (e.g., PPN1754726119_18880201.xml)
        mets_filename = mets_files[0]
        ppn = mets_filename.split('_')[0]  # Gets "PPN1754726119"
        
        return ppn

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created `SubNewspaperPage` instances are added to the `pages` attribute.

        Raises:
            Exception: If creating a `SubNewspaperPage` raises an exception.
        """
        # Find the METS file and extract PPN
        self.ppn = self._extract_ppn_from_mets()
        self.mets_file = os.path.join(
            self.path,
            [f for f in os.listdir(self.path) if f.startswith('PPN') and f.endswith('.xml')][0]
        )
        
        # Parse METS to get page information
        with open(self.mets_file, 'r', encoding='utf-8') as f:
            mets_soup = BeautifulSoup(f, 'xml')
        
        # Extract page dimensions from techMD sections
        self._extract_page_dimensions(mets_soup)
        
        # Find all page files from FULLTEXT fileGrp
        file_grp = mets_soup.find('fileGrp', {'USE': 'FULLTEXT'})
        if not file_grp:
            logger.warning(f"No FULLTEXT fileGrp found in {self.mets_file}")
            return
            
        page_files = []
        for file_elem in file_grp.find_all('file'):
            flocat = file_elem.find('FLocat')
            if flocat and 'xlink:href' in flocat.attrs:
                href = flocat['xlink:href']
                # Extract filename from URL
                filename = href.split('/')[-1]
                file_id = file_elem.get('ID', '')
                # Extract page number from file ID (e.g., FILE_0001_FULLTEXT -> 1)
                try:
                    page_num = int(file_id.split('_')[1])
                    page_files.append((page_num, filename))
                except (IndexError, ValueError):
                    logger.warning(f"Could not extract page number from {file_id}")
                    continue
        
        # Sort by page number
        page_files.sort(key=lambda x: x[0])
        
        self.pages = []
        for page_num, filename in page_files:
            page_id = f"{self.id}-p{str(page_num).zfill(4)}"
            
            try:
                # Get page dimensions (default to 3150x4743 if not found)
                page_size = self.page_sizes.get(page_num, (3150, 4743))
                
                page = SubNewspaperPage(
                    page_id,
                    page_num,
                    filename,
                    self.path,
                    page_size
                )
                self.pages.append(page)
                logger.debug(f"Added page {page_num}: {page_id}")
                
            except Exception as e:
                msg = f"Adding page {page_num} {page_id} {filename} raised exception: {e}"
                logger.error(msg)
                raise e

    def _extract_page_dimensions(self, mets_soup: BeautifulSoup) -> None:
        """Extract page dimensions from METS technical metadata.
        
        Args:
            mets_soup (BeautifulSoup): Parsed METS XML document
        """
        # Find all techMD elements with IIIF or image metadata
        tech_mds = mets_soup.find_all('techMD')
        
        for tech_md in tech_mds:
            tech_id = tech_md.get('ID', '')
            # Extract page number from ID (e.g., FILE_0001_IIIF_AMDT1 -> 1)
            try:
                page_num = int(tech_id.split('_')[1])
            except (IndexError, ValueError):
                continue
                
            # Look for image dimensions in mix:mix metadata
            mix_elem = tech_md.find('mix')
            if mix_elem:
                width_elem = mix_elem.find('imageWidth')
                height_elem = mix_elem.find('imageHeight')
                
                if width_elem and height_elem:
                    try:
                        width = int(width_elem.text)
                        height = int(height_elem.text)
                        self.page_sizes[page_num] = (width, height)
                        logger.debug(f"Page {page_num} dimensions: {width}x{height}")
                    except ValueError:
                        logger.warning(f"Invalid dimensions for page {page_num}")

    def _extract_title_from_mets(self) -> str | None:
        """Extract newspaper title from METS metadata.
        
        Returns:
            str | None: The newspaper title, or None if not found
        """
        try:
            with open(self.mets_file, 'r', encoding='utf-8') as f:
                mets_soup = BeautifulSoup(f, 'xml')
            
            # Look for title in dmdSec/MODS metadata
            title_elem = mets_soup.find('title')
            if title_elem:
                return title_elem.text.strip()
                
            logger.warning(f"No title found in {self.mets_file}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting title from METS: {e}")
            return None

    def _parse_mets(self) -> None:
        """Parse METS file to extract issue-level metadata.
        
        This method extracts the title and prepares the issue data structure
        according to the canonical format.
        """
        # Extract title from METS
        self.title = self._extract_title_from_mets()
        
        # For SUB format, we don't have detailed content item segmentation in METS
        # The logical structure is minimal, so we create a simple issue representation
        # Content items would need to be extracted from Alto files if needed
        
        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": False,  # SUB format doesn't have OLR annotations in METS
            "i": [],  # Content items - would require Alto parsing
            "pp": [p.id for p in sorted(self.pages, key=lambda x: x.number)],
            "n": self._notes,
        }
        
        # Add title if available
        if self.title:
            self.issue_data["t"] = self.title
            
        logger.info(
            f"Parsed issue {self.id}: {len(self.pages)} pages, "
            f"title='{self.title}', ppn={self.ppn}"
        )