"""This module contains the definition of KBR importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the KBR ALTO format to a unified canonical format.
KBR data does not have METS files, only ALTO XML files in issue directories.
These classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
from collections import Counter
from typing import Any

from bs4 import BeautifulSoup

from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.importers.mets_alto import alto
from text_preparation.importers.kbr.detect import KbrIssueDir

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = "info.json"


class KbrNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in KBR (ALTO) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        page_size (tuple[int, int]): Width and height of the page image.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.

    Attributes:
        id (str): Canonical Page ID (e.g. ``Bruxellois-1917-08-08-a-p0001``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (KbrNewspaperIssue): Issue this page is from.
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
        page_size: tuple[int, int] | None = None,
        encoding: str = "utf-8",
    ) -> None:
        super().__init__(_id, number, filename, basedir, encoding)

        self.iiif_img_base_uri = os.path.join(IIIF_ENDPOINT_URI, self.id)
        self.page_data["iiif_img_base_uri"] = self.iiif_img_base_uri

        # Add the facsimile height and width to the page data if provided
        if page_size:
            self.page_data["fw"] = page_size[0]
            self.page_data["fh"] = page_size[1]

    def add_issue(self, issue: "KbrNewspaperIssue") -> None:
        """Add the given `KbrNewspaperIssue` as an attribute for this class.

        Args:
            issue (KbrNewspaperIssue): Issue this page is from
        """
        self.issue = issue


class KbrNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in KBR (ALTO) format.

    KBR data does not have METS files - only ALTO XML files organized in
    issue directories. The issue detection and page parsing is done by
    analyzing the directory contents and ALTO file naming conventions.

    All functions defined in this child class are specific to parsing KBR
    ALTO format.

    Args:
        issue_dir (KbrIssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``Bruxellois-1917-08-08-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`KbrNewspaperPage` instances from this issue.
        page_sizes (dict): Dictionary mapping page numbers to (width, height) tuples.
    """

    def __init__(self, issue_dir: KbrIssueDir) -> None:
        # Initialize attributes to prevent errors before calling super().__init__
        self.page_sizes = {}
        self._notes = []

        super().__init__(issue_dir)

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the ALTO XML files.

        KBR data does not have a METS file, so we detect pages by scanning
        the issue directory for ALTO XML files and parsing their filenames.

        Created `KbrNewspaperPage` instances are added to the `pages` attribute.

        Raises:
            Exception: If creating a `KbrNewspaperPage` raises an exception.
        """
        # Find all ALTO XML files in the directory
        alto_files = []
        for filename in os.listdir(self.path):
            if filename.endswith(".xml"):
                alto_files.append(filename)

        if not alto_files:
            logger.warning(f"No ALTO XML files found in {self.path}")
            return

        # Parse ALTO filenames to extract page numbers
        # Format: BE-KBR00_{newspaper_id}_{date}_{edition}_{XX}_{XX}_{XX}_{page}_{unique_id}.xml
        # Example: BE-KBR00_16777935_19170808_00_02_00_0_01_0001_9955501.xml
        page_files = []
        for filename in alto_files:
            try:
                # Extract page number from filename
                # The page number is typically in position -2 before the unique ID
                parts = filename.replace(".xml", "").split("_")
                # The page number format is 0001, 0002, etc. (4 digits)
                page_num_str = parts[-2]  # Second to last part before unique ID
                page_num = int(page_num_str)
                page_files.append((page_num, filename))
            except (IndexError, ValueError) as e:
                logger.warning(f"Could not extract page number from {filename}: {e}")
                continue

        # Sort by page number
        page_files.sort(key=lambda x: x[0])

        self.pages = []
        for page_num, filename in page_files:
            page_id = f"{self.id}-p{str(page_num).zfill(4)}"

            try:
                # Try to extract page dimensions from the ALTO file
                page_size = self._extract_page_size(filename)

                page = KbrNewspaperPage(
                    page_id,
                    page_num,
                    filename,
                    self.path,
                    page_size=page_size,
                )
                self.pages.append(page)
                logger.debug(f"Added page {page_num}: {page_id}")

            except Exception as e:
                msg = f"Adding page {page_num} {page_id} {filename} raised exception: {e}"
                logger.error(msg)
                raise e

    def _extract_page_size(self, filename: str) -> tuple[int, int] | None:
        """Extract page dimensions from ALTO XML file.

        Args:
            filename (str): ALTO XML filename

        Returns:
            tuple[int, int] | None: (width, height) tuple or None if not found
        """
        try:
            alto_path = os.path.join(self.path, filename)
            with open(alto_path, "r", encoding="utf-8") as f:
                raw_xml = f.read()

            alto_doc = BeautifulSoup(raw_xml, "xml")
            page_elem = alto_doc.find("Page")

            if page_elem:
                width = int(page_elem.get("WIDTH", 0))
                height = int(page_elem.get("HEIGHT", 0))
                if width > 0 and height > 0:
                    return (width, height)
        except Exception as e:
            logger.warning(f"Could not extract page size from {filename}: {e}")

        return None

    def _find_content_items(self) -> list[dict[str, Any]]:
        """Extract content items from Alto files for this issue.

        For KBR data without OLR (Optical Layout Recognition at article level),
        we create content items for:
        1. Page-level content items containing all TextBlocks (excluding tables)
        2. Image content items for Illustration elements

        Each content item includes:
        - Metadata: id, type, language, page references
        - Legacy: tracking ALTO component information for reconstruction

        Returns:
            list[dict[str, Any]]: List of content item dictionaries
        """
        content_items = []
        # Non-page items counter starts after all page CIs (num_pages + 1)
        ci_counter = len(self.pages)

        # Process each page to extract content items
        for page in sorted(self.pages, key=lambda x: x.number):
            page_num_str = str(page.number).zfill(4)

            # Use page.xml property instead of opening file manually
            try:
                alto_soup = page.xml
            except Exception as e:
                logger.error(f"Failed to parse Alto file for page {page.number}: {e}")
                continue

            # Get the PrintSpace element which contains the page content
            print_space = alto_soup.find("PrintSpace")
            if not print_space:
                logger.warning(f"No PrintSpace found in Alto file for page {page.number}")
                continue

            # Get language from ALTO if available (try to detect from TextBlocks)
            lang = self._detect_language(alto_soup)

            # === 1. Create page-level content item ===
            # Collect all TextBlock elements that are NOT tables
            text_blocks = []
            for text_block in print_space.find_all("TextBlock"):
                # Skip TextBlocks that are explicitly marked as tables
                block_type = text_block.get("TYPE", "")
                inside_table = text_block.find_parent(
                    "ComposedBlock",
                    {"TYPE": lambda value: value and value.lower() == "table"},
                )
                if block_type.lower() != "table" and inside_table is None:
                    text_blocks.append(text_block)

            # Create the page-level content item (type "page")
            page_ci_id = f"{self.id}-i{page_num_str}"
            page_ci = {
                # Metadata section
                "m": {
                    "id": page_ci_id,
                    "tp": "page",  # content type: page
                    "l": lang,  # language
                    "pp": [int(page.id[-4:])],
                },
                # Legacy section - tracking ALTO components
                "l": {
                    # Composite ID: {issue_id}-{page_filename}
                    "id": f"{self.id}-{page.filename}",
                    # List of parts (ALTO elements) composing this CI
                    "parts": [
                        {
                            "comp_id": tb.get("ID"),  # TextBlock ID from Alto
                            "comp_role": "body",  # role: body text
                            "comp_fileid": page.filename,  # Alto filename
                            "comp_page_no": page.number,  # page number
                        }
                        for tb in text_blocks
                        if tb.get("ID")
                    ],
                    # Source filename
                    "source": page.filename,
                },
            }
            content_items.append(page_ci)

            # === 2. Create content items for Illustrations (images) ===
            for illustration in print_space.find_all("Illustration"):
                illus_id = illustration.get("ID")
                if not illus_id:
                    continue

                # Use alto.distill_coordinates for coordinate extraction
                coords = None
                try:
                    coords = alto.distill_coordinates(illustration)
                except (TypeError, ValueError) as e:
                    logger.warning(f"Invalid coordinates for Illustration {illus_id}: {e}")

                # Generate unique CI ID for this image
                ci_counter += 1
                image_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"

                image_ci = {
                    # Metadata section
                    "m": {
                        "id": image_ci_id,
                        "tp": "image",  # content type: image
                        "pp": [int(page.id[-4:])],  # page this image appears on
                        "iiif_link": os.path.join(page.iiif_img_base_uri, IIIF_SUFFIX),
                    },
                    # Legacy section
                    "l": {
                        # Composite ID format for images
                        "id": f"{self.id}-{page.filename}",
                        # Single part for the illustration element
                        "parts": [
                            {
                                "comp_id": illus_id,  # Illustration ID from Alto
                                "comp_role": "image",  # role: image
                                "comp_fileid": page.filename,
                                "comp_page_no": page.number,
                            }
                        ],
                        "source": page.filename,
                    },
                }

                # Add coordinates if available
                if coords:
                    image_ci["m"]["c"] = coords

                content_items.append(image_ci)

            # === 3. Create content items for Tables ===
            for composed_block in print_space.find_all("ComposedBlock"):
                block_type = composed_block.get("TYPE", "")
                if block_type.lower() == "table":
                    table_id = composed_block.get("ID")
                    if not table_id:
                        continue

                    # Use alto.distill_coordinates for coordinate extraction
                    coords = None
                    try:
                        coords = alto.distill_coordinates(composed_block)
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Invalid coordinates for table {table_id}: {e}")

                    # Generate unique CI ID for this table
                    ci_counter += 1
                    table_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"

                    table_ci = {
                        # Metadata section
                        "m": {
                            "id": table_ci_id,
                            "tp": "table",  # content type: table
                            "l": lang,
                            "pp": [int(page.id[-4:])],
                        },
                        # Legacy section
                        "l": {
                            # Composite ID format for tables
                            "id": f"{self.id}-{page.filename}",
                            # Single part for the table TextBlock
                            "parts": [
                                {
                                    "comp_id": table_id,  # TextBlock ID from Alto
                                    "comp_role": "table",  # role: table
                                    "comp_fileid": page.filename,
                                    "comp_page_no": page.number,
                                }
                            ],
                            "source": page.filename,
                        },
                    }

                    # Add coordinates if available
                    if coords:
                        table_ci["m"]["c"] = coords

                    content_items.append(table_ci)

        msg = (
            f"Created {len(content_items)} content items for issue {self.id}: "
            f"{sum(1 for ci in content_items if ci['m']['tp'] == 'page')} pages, "
            f"{sum(1 for ci in content_items if ci['m']['tp'] == 'image')} images, "
            f"{sum(1 for ci in content_items if ci['m']['tp'] == 'table')} tables"
        )
        logger.debug(msg)

        return content_items

    def _detect_language(self, alto_soup: BeautifulSoup) -> str:
        """Detect language from ALTO XML.

        Tries to infer the dominant page language from TextBlock elements.

        Args:
            alto_soup (BeautifulSoup): Parsed ALTO XML document

        Returns:
            str: Detected language code or 'fr' as default (KBR is primarily French)
        """
        counts: Counter[str] = Counter()
        first_seen: list[str] = []

        for text_block in alto_soup.find_all("TextBlock"):
            lang = text_block.get("language")
            if not lang:
                continue

            normalized_lang = lang.lower().split("-")[0][:2]
            counts[normalized_lang] += 1
            if normalized_lang not in first_seen:
                first_seen.append(normalized_lang)

        if counts:
            preferred_order = {lang: idx for idx, lang in enumerate(first_seen)}
            return min(
                counts,
                key=lambda lang: (-counts[lang], preferred_order[lang]),
            )

        # Default to French for KBR (Belgian newspapers)
        return "fr"

    def _parse_mets(self) -> None:
        """Parse issue metadata and create content items.

        KBR data doesn't have METS files, so we create issue metadata
        directly from the ALTO files and directory structure.

        This method:
        1. Creates content items by parsing Alto files (page CIs, images, tables)
        2. Constructs the issue_data dictionary according to canonical format
        """
        # Extract content items from Alto files
        # This creates page-level CIs, image CIs, and table CIs
        content_items = self._find_content_items()

        # Construct the issue data according to canonical format
        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": False,  # KBR format doesn't have OLR annotations
            "i": content_items,  # List of content items (pages, images, tables)
            "pp": [p.id for p in sorted(self.pages, key=lambda x: x.number)],
            "n": self._notes,
        }

        logger.info(
            f"Parsed issue {self.id}: {len(self.pages)} pages, "
            f"{len(content_items)} content items"
        )
