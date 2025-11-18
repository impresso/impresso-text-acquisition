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
        # Construct IIIF image base URI for this page using the issue's PPN
        # The IIIF manifest URI is at the issue level, not page level
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
        title_ppn (str): Title-level PPN identifier (PPN without date).
        title (str): Newspaper title extracted from METS metadata.
        mets_file (str): Path to the METS XML file for this issue.
    """

    def __init__(self, issue_dir: SubIssueDir) -> None:
        # Initialize attributes to prevent errors before calling super().__init__
        self.ppn = None
        self.title_ppn = None
        self.title = None
        self.page_sizes = {}
        self.mets_file = None
        
        super().__init__(issue_dir)

    def _extract_ppn_from_mets(self) -> str:
        """Extract PPN identifier from METS filename.
        
        The METS filename follows the pattern: PPN{ppn}_{date}.xml
        The PPN contains both the title-level identifier and the date.
        
        Returns:
            str: The PPN identifier (including date suffix)
            
        Raises:
            ValueError: If no valid METS file is found
        """
        mets_files = [
            f for f in os.listdir(self.path) if f.endswith(".xml") and f.startswith("PPN")
        ]
        
        if not mets_files:
            raise ValueError(f"No METS file found in {self.path}")
            
        # Extract PPN from filename (e.g., PPN1754726119_18880201.xml)
        # The full PPN includes the date suffix (e.g., "PPN1754726119")
        mets_filename = mets_files[0]
        ppn = mets_filename.split('_')[0]  # Gets "PPN1754726119"
        
        return ppn

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created `SubNewspaperPage` instances are added to the `pages` attribute.
        This method also extracts the PPN identifiers and page dimensions from the METS file.

        Raises:
            Exception: If creating a `SubNewspaperPage` raises an exception.
        """
        # Find the METS file and extract PPN
        self.ppn = self._extract_ppn_from_mets()
        
        # The title-level PPN is the PPN without any date-specific suffix
        # For SUB, the PPN itself serves as the title identifier
        self.title_ppn = self.ppn
        
        # Get the full path to the METS file
        self.mets_file = os.path.join(
            self.path,
            [f for f in os.listdir(self.path) if f.startswith("PPN") and f.endswith(".xml")][0],
        )
        
        # Parse METS to get page information and dimensions
        with open(self.mets_file, "r", encoding="utf-8") as f:
            mets_soup = BeautifulSoup(f, "xml")
        
        # Extract page dimensions from techMD sections
        self._extract_page_dimensions(mets_soup)
        
        # Find all page files from FULLTEXT fileGrp
        file_grp = mets_soup.find("fileGrp", {"USE": "FULLTEXT"})
        if not file_grp:
            logger.warning(f"No FULLTEXT fileGrp found in {self.mets_file}")
            return
            
        page_files = []
        for file_elem in file_grp.find_all("file"):
            flocat = file_elem.find("FLocat")
            if flocat and "xlink:href" in flocat.attrs:
                href = flocat["xlink:href"]
                # Extract filename from URL
                filename = href.split("/")[-1]
                file_id = file_elem.get("ID", "")
                # Extract page number from file ID (e.g., FILE_0001_FULLTEXT -> 1)
                try:
                    page_num = int(file_id.split("_")[1])
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
                    page_size,
                )
                self.pages.append(page)
                logger.debug(f"Added page {page_num}: {page_id}")
                
            except Exception as e:
                msg = f"Adding page {page_num} {page_id} {filename} raised exception: {e}"
                logger.error(msg)
                raise e

    def _extract_page_dimensions(self, mets_soup: BeautifulSoup) -> None:
        """Extract page dimensions from METS technical metadata.
        
        Parses the techMD sections to find image width and height information
        stored in MIX format metadata.
        
        Args:
            mets_soup (BeautifulSoup): Parsed METS XML document
        """
        # Find all techMD elements with IIIF or image metadata
        tech_mds = mets_soup.find_all("techMD")
        
        for tech_md in tech_mds:
            tech_id = tech_md.get("ID", "")
            # Extract page number from ID (e.g., FILE_0001_IIIF_AMDT1 -> 1)
            try:
                page_num = int(tech_id.split("_")[1])
            except (IndexError, ValueError):
                continue
                
            # Look for image dimensions in mix:mix metadata
            mix_elem = tech_md.find("mix")
            if mix_elem:
                width_elem = mix_elem.find("imageWidth")
                height_elem = mix_elem.find("imageHeight")
                
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
        
        Searches the METS dmdSec/MODS metadata for the newspaper title.
        
        Returns:
            str | None: The newspaper title, or None if not found
        """
        try:
            with open(self.mets_file, "r", encoding="utf-8") as f:
                mets_soup = BeautifulSoup(f, "xml")
            
            # Look for title in dmdSec/MODS metadata
            title_elem = mets_soup.find("title")
            if title_elem:
                return title_elem.text.strip()
                
            logger.warning(f"No title found in {self.mets_file}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting title from METS: {e}")
            return None

    def _find_content_items(self) -> list[dict[str, Any]]:
        """Extract content items from Alto files for this issue.
        
        For SUB data without OLR (Optical Layout Recognition at article level),
        we create content items for:
        1. Page-level content items containing all TextBlocks (excluding tables)
        2. Image content items for Illustration elements
        3. Table content items for TextBlocks with type="Table"
        
        Each content item includes:
        - Metadata: id, type, language, page references
        - Legacy: tracking METS/ALTO component information for reconstruction
        
        Returns:
            list[dict[str, Any]]: List of content item dictionaries
        """
        content_items = []
        ci_counter = 1  # Counter for images and tables after page CIs
        
        # Process each page to extract content items
        for page in sorted(self.pages, key=lambda x: x.number):
            page_num_str = str(page.number).zfill(4)
            alto_file_path = os.path.join(self.path, page.filename)
            
            try:
                with open(alto_file_path, "r", encoding="utf-8") as f:
                    alto_soup = BeautifulSoup(f, "xml")
            except Exception as e:
                logger.error(f"Failed to parse Alto file {alto_file_path}: {e}")
                continue
            
            # Get the PrintSpace element which contains the page content
            print_space = alto_soup.find("PrintSpace")
            if not print_space:
                logger.warning(f"No PrintSpace found in {alto_file_path}")
                continue
            
            # === 1. Create page-level content item ===
            # Collect all TextBlock elements that are NOT tables
            text_blocks = []
            for text_block in print_space.find_all("TextBlock"):
                # Skip TextBlocks that are explicitly marked as tables
                block_type = text_block.get("TYPE", "")
                if block_type.lower() != "table":
                    text_blocks.append(text_block)
            
            # Create the page-level content item (type "page")
            page_ci_id = f"{self.id}-i{page_num_str}"
            page_ci = {
                # Metadata section
                "m": {
                    "id": page_ci_id,
                    "tp": "page",  # content type: page
                    "l": alto_soup.find("Page").get("language", "de"),  # language
                    "pp": [page.id],  # pages this CI appears on
                },
                # Legacy section - tracking ALTO components
                "l": {
                    # Composite ID: {title_ppn}-{issue_ppn}-{page_filename}
                    "id": f"{self.title_ppn}-{self.ppn}-{page.filename}",
                    # List of parts (ALTO elements) composing this CI
                    "parts": [
                        {
                            "comp_id": tb.get("ID"),  # TextBlock ID from Alto
                            "r": "body",  # role: body text
                            "file_id": page.filename,  # Alto filename
                            "pn": page.number,  # page number
                        }
                        for tb in text_blocks
                        if tb.get("ID")
                    ],
                    # Source METS filename
                    "source": os.path.basename(self.mets_file),
                    # Additional SUB-specific identifiers
                    "ppn": self.ppn,  # Issue PPN with date
                    "title_ppn": self.title_ppn,  # Title-level PPN
                },
            }
            content_items.append(page_ci)
            
            # === 2. Create content items for Illustrations (images) ===
            for illustration in print_space.find_all("Illustration"):
                illus_id = illustration.get("ID")
                if not illus_id:
                    continue
                    
                # Generate unique CI ID for this image
                ci_counter += 1
                image_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"
                
                image_ci = {
                    # Metadata section
                    "m": {
                        "id": image_ci_id,
                        "tp": "image",  # content type: image
                        "l": alto_soup.find("Page").get("language", "de"),
                        "pp": [page.id],  # page this image appears on
                    },
                    # Legacy section
                    "l": {
                        # Composite ID format for images
                        "id": f"{self.title_ppn}-{self.ppn}-{page.filename}",
                        # Single part for the illustration element
                        "parts": [
                            {
                                "comp_id": illus_id,  # Illustration ID from Alto
                                "r": "image",  # role: image
                                "file_id": page.filename,
                                "pn": page.number,
                            }
                        ],
                        "source": os.path.basename(self.mets_file),
                        "ppn": self.ppn,
                        "title_ppn": self.title_ppn,
                    },
                }
                content_items.append(image_ci)
            
            # === 3. Create content items for Tables ===
            for text_block in print_space.find_all("TextBlock"):
                block_type = text_block.get("TYPE", "")
                if block_type.lower() == "table":
                    table_id = text_block.get("ID")
                    if not table_id:
                        continue
                    
                    # Generate unique CI ID for this table
                    ci_counter += 1
                    table_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"
                    
                    table_ci = {
                        # Metadata section
                        "m": {
                            "id": table_ci_id,
                            "tp": "table",  # content type: table
                            "l": alto_soup.find("Page").get("language", "de"),
                            "pp": [page.id],
                        },
                        # Legacy section
                        "l": {
                            # Composite ID format for tables
                            "id": f"{self.title_ppn}-{self.ppn}-{page.filename}",
                            # Single part for the table TextBlock
                            "parts": [
                                {
                                    "comp_id": table_id,  # TextBlock ID from Alto
                                    "r": "table",  # role: table
                                    "file_id": page.filename,
                                    "pn": page.number,
                                }
                            ],
                            "source": os.path.basename(self.mets_file),
                            "ppn": self.ppn,
                            "title_ppn": self.title_ppn,
                        },
                    }
                    content_items.append(table_ci)
        
        logger.info(
            f"Created {len(content_items)} content items for issue {self.id}: "
            f"{sum(1 for ci in content_items if ci['m']['tp'] == 'page')} pages, "
            f"{sum(1 for ci in content_items if ci['m']['tp'] == 'image')} images, "
            f"{sum(1 for ci in content_items if ci['m']['tp'] == 'table')} tables"
        )
        
        return content_items

    def _parse_mets(self) -> None:
        """Parse METS file to extract issue-level metadata and create content items.
        
        This method:
        1. Extracts the newspaper title from METS metadata
        2. Creates content items by parsing Alto files (page CIs, images, tables)
        3. Constructs the issue_data dictionary according to canonical format
        
        Note: SUB format has minimal logical structure in METS, so content items
        are primarily extracted from Alto XML files at the page level.
        """
        # Extract title from METS
        self.title = self._extract_title_from_mets()
        
        # Extract content items from Alto files
        # This creates page-level CIs, image CIs, and table CIs
        content_items = self._find_content_items()
        
        # Construct the issue data according to canonical format
        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": False,  # SUB format doesn't have OLR annotations in METS
            "i": content_items,  # List of content items (pages, images, tables)
            "pp": [p.id for p in sorted(self.pages, key=lambda x: x.number)],
            "iiif_manifest_uri": f"{IIIF_ENDPOINT_URI}{self.ppn}{IIIF_MANIFEST_SUFFIX}",
            "n": self._notes,
        }
        
        # Add title if available
        if self.title:
            self.issue_data["t"] = self.title
            
        logger.info(
            f"Parsed issue {self.id}: {len(self.pages)} pages, "
            f"{len(content_items)} content items, "
            f"title='{self.title}', ppn={self.ppn}"
        )