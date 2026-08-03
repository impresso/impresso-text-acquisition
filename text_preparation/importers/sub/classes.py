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
from bs4.element import Tag, NavigableString

from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.importers.mets_alto import alto
from text_preparation.importers.sub.detect import SubIssueDir
from text_preparation.utils import get_reading_order

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
        file_id: str,
        iiif_img_base_uri: str | None = None,
        encoding: str = "utf-8",
    ) -> None:
        super().__init__(_id, number, filename, basedir, encoding)

        # Add the facsimile height and width to the page data
        self.page_data["fw"] = page_size[0]
        self.page_data["fh"] = page_size[1]
        self.file_id = file_id
        self.iiif_img_base_uri = iiif_img_base_uri

        # Store page-level IIIF image base URI if provided
        if iiif_img_base_uri:
            self.page_data["iiif_img_base_uri"] = self.iiif_img_base_uri

    def add_issue(self, issue: "SubNewspaperIssue") -> None:
        """Add the given `SubNewspaperIssue` as an attribute for this class.

        Args:
            issue (SubNewspaperIssue): Issue this page is from
        """
        self.issue = issue

    def parse(self) -> None:
        """Parse the page's Alto XML and extract regions, paragraphs, lines, and tokens.

        This method processes the SUB Alto XML document to extract all OCR information
        and structure it into the canonical page format. It maps OCR component IDs
        to Content Item IDs and extracts page regions with their coordinates.

        The parsed data is stored in the `page_data` attribute under keys:
        - "r": List of page regions with paragraphs, lines, and tokens
        - "n": Optional notes about parsing problems (e.g., missing coordinates)
        """
        if self.alto_doc is None:
            doc = self.xml
        else:
            doc = self.alto_doc

        mappings = {}
        for ci in self.issue.issue_data["i"]:
            ci_id = ci["m"]["id"]
            if "parts" in ci["l"]:
                for part in ci["l"]["parts"]:

                    # ONLY map parts that are on THIS page to avoid BLOCK-ID collisions
                    if part.get("comp_page_no") != self.number:
                        continue

                    mappings[part["comp_id"]] = ci_id

        pselement = doc.find("PrintSpace")
        page_regions, notes = self.parse_printspace(pselement, mappings)
        self.page_data["cc"], self.page_data["r"] = self._convert_coordinates(page_regions)

        if len(notes) > 0:
            self.page_data["n"] = notes

    def parse_printspace(
        self, element: Tag, mappings: dict[str, str]
    ) -> tuple[list[dict], list[str]]:
        """Parse the ``<PrintSpace>`` element of an SUB ALTO XML document.

        This function closely resembles the one inside importers.alto, but slightly
        adapts to the SUB case, where we have an additional layer of "ComposedBlocks"
        on top of the "TextBlocks".
        The original function could be used if the SubNewspaperPage.parse() method is
        slightly adapted (potential future work).
        This element contains all the OCR information about the content items of
        a page, up to the lowest level of the hierarchy: the regions, paragraphs,
        lines and tokens, each with their corresponding coordinates.

        Args:
            element (Tag): Input XML element (``<PrintSpace>``).
            mappings (dict[str, str]): Mapping from OCR component ids to their
                corresponding canonicalw Content Item ID.

        Returns:
            tuple[list[dict], list[str]]: List of page regions in the canonical
                format and notes about potential parsing problems.
        """

        regions = []
        notes = []
        # in case of a blank page, the PrintSpace element is not found thus
        # it will be none
        if element:
            for block in element.children:

                if isinstance(block, NavigableString) or block.name == "Shape":
                    continue

                block_id = block.get("ID")

                if (
                    block.get("TYPE") and block.get("TYPE").lower() in alto.IMG_COMP_LABELS
                ) or block.name.lower() in alto.IMG_COMP_LABELS:
                    # don't add the text from illustration regions
                    # because it's very often OCR none-sense.
                    continue

                if block_id in mappings:
                    part_of_contentitem = mappings[block_id]
                elif block.name == "ComposedBlock" and len(block.findAll("TextBlock")) != 0:
                    sub_regions, sub_notes = self.parse_printspace(block, mappings)
                    regions.extend(sub_regions)
                    notes += sub_notes
                    continue
                else:
                    part_of_contentitem = None

                coordinates = alto.distill_coordinates(block)

                tmp = [
                    alto.parse_textline(line_element) for line_element in block.findAll("TextLine")
                ]

                if len(tmp) > 0:
                    lines, new_notes = list(zip(*tmp))
                    new_notes = [i for n in new_notes for i in n]
                    if isinstance(lines, tuple):
                        # formatting problem
                        lines = list(lines)
                else:
                    lines, new_notes = [], []

                if lines == []:
                    msg = f"{self.id} - empty lines, not adding them"
                    if logger.level == "DEBUG":
                        print(msg)
                    logger.debug(msg)
                    continue

                paragraph = {"c": coordinates, "l": lines}

                region = {"c": coordinates, "p": [paragraph]}

                if part_of_contentitem:
                    region["pOf"] = part_of_contentitem

                notes += new_notes
                regions.append(region)

        return regions, notes


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
        self.ppn_with_date = None  # Full PPN including date suffix (e.g., PPN1754726119_18880202)
        self.title_ppn = None
        self.title = None
        self.page_sizes = {}
        self.page_iiif_links = {}  # {page_num: iiif_base_uri}
        self.mets_file = None
        self._mets_soup = None  # Cache for parsed METS XML

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
        ppn = mets_filename.split("_")[0]  # Gets "PPN1754726119"

        return ppn, mets_filename

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created `SubNewspaperPage` instances are added to the `pages` attribute.
        This method also extracts the PPN identifiers and page dimensions from the METS file.

        Raises:
            Exception: If creating a `SubNewspaperPage` raises an exception.
        """
        # Find the METS file and extract PPN
        self.ppn, mets_filename = self._extract_ppn_from_mets()

        # The title-level PPN is the PPN without any date-specific suffix
        # For SUB, the PPN itself serves as the title identifier
        self.title_ppn = self.ppn

        # Get the full path to the METS file and extract full PPN with date
        self.mets_file = os.path.join(self.path, mets_filename)
        # Extract full PPN with date suffix (e.g., PPN1754726119_18880202)
        self.ppn_with_date = mets_filename.replace(".xml", "")

        # Parse METS using self.xml property (caches the result)
        mets_soup = self.xml
        self._mets_soup = mets_soup  # Cache for later use

        # Extract page dimensions from techMD sections
        self._extract_page_dimensions(mets_soup)

        # Extract IIIF links from fileGrp IIIF
        self._extract_page_iiif_links(mets_soup)

        page_files = []
        # Find all page files from FULLTEXT fileGrp
        file_grp = mets_soup.find("fileGrp", {"USE": "FULLTEXT"})
        if not file_grp:
            msg = f"{self.id} - No FULLTEXT fileGrp found in {self.mets_file}, finding all the page XML files present in the issuedir"
            print(msg)
            logger.warning(msg)
            # if there is no FULLTEXT fileGrp in the mets file, simply check the contents of the issuedir
            pages_filenames = [
                f for f in os.listdir(self.path) if "PPN" not in f and f.endswith(".xml")
            ]

            if len(pages_filenames) != len(self.page_iiif_links):
                msg = f"{self.id} - Found a different number of page ALTO files ({len(pages_filenames)}) than the number of iiif links ({len(self.page_iiif_links)})!"
                print(msg)
                logger.warning(msg)
            # fix 1895, 1899, 1933
            # in this case use by default file IDs mathcing the structure of the rest
            for filename in pages_filenames:
                try:
                    page_num = int(filename.replace(".xml", ""))
                    file_id = f"FILE_{str(page_num).zfill(4)}_FULLTEXT"
                    page_files.append((page_num, filename, file_id))
                except (IndexError, ValueError):
                    logger.warning(f"Problem when extracting page number from {filename}")
                    continue

        # if the FULLTEXT fileGrp is present in the mets, use it to identify the pages info.
        else:
            for file_elem in file_grp.find_all("file"):
                flocat = file_elem.find("FLocat")
                if flocat and "xlink:href" in flocat.attrs:
                    href = flocat["xlink:href"]
                    # Extract filename from URL
                    filename = href.split("/")[-1]
                    file_id = file_elem.get("ID", "")
                    # print(f"{self.id} - adding page with filename {filename} and file_id {file_id}")
                    # Extract page number from file ID (e.g., FILE_0001_FULLTEXT -> 1)
                    try:
                        page_num = int(file_id.split("_")[1])
                        page_files.append((page_num, filename, file_id))
                    except (IndexError, ValueError):
                        logger.warning(f"Could not extract page number from {file_id}")
                        continue

        # Sort by page number
        page_files.sort(key=lambda x: x[0])

        self.pages = []
        for page_num, filename, file_id in page_files:
            page_id = f"{self.id}-p{str(page_num).zfill(4)}"

            try:
                # Get page dimensions (default to 3150x4743 if not found)
                page_size = self.page_sizes.get(page_num, (3150, 4743))
                # Get page-level IIIF link
                iiif_link = self.page_iiif_links.get(page_num)

                page = SubNewspaperPage(
                    page_id,
                    page_num,
                    filename,
                    self.path,
                    page_size,
                    file_id,
                    iiif_img_base_uri=iiif_link,
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

    def _extract_page_iiif_links(self, mets_soup: BeautifulSoup) -> None:
        """Extract IIIF image base URIs for each page from METS.

        Parses the fileGrp with USE="IIIF" to get page-level IIIF links.
        Removes the "/info.json" suffix to get the base URI.

        Args:
            mets_soup (BeautifulSoup): Parsed METS XML document
        """
        iiif_grp = mets_soup.find("fileGrp", {"USE": "IIIF"})
        if not iiif_grp:
            logger.warning(f"No IIIF fileGrp found in {self.mets_file}")
            return

        for file_elem in iiif_grp.find_all("file"):
            file_id = file_elem.get("ID", "")
            # Extract page number from file ID (e.g., FILE_0001_IIIF -> 1)
            try:
                page_num = int(file_id.split("_")[1])
            except (IndexError, ValueError):
                continue

            flocat = file_elem.find("FLocat")
            if flocat and "xlink:href" in flocat.attrs:
                href = flocat["xlink:href"]
                # Remove "/info.json" suffix to get base URI
                # e.g., https://pic.sub.uni-hamburg.de/iiif/2/PPN1754726119_18880202%2F00000001.tif/info.json
                # becomes https://pic.sub.uni-hamburg.de/iiif/2/PPN1754726119_18880202%2F00000001.tif
                if href.endswith("/info.json"):
                    href = href[:-10]  # Remove "/info.json"
                self.page_iiif_links[page_num] = href
                logger.debug(f"Page {page_num} IIIF: {href}")

    def _extract_title_from_mets(self) -> str | None:
        """Extract newspaper title from METS metadata.

        Searches the METS dmdSec/MODS metadata for the newspaper title.

        Returns:
            str | None: The newspaper title, or None if not found
        """
        try:
            # Use cached METS soup if available
            mets_soup = self._mets_soup if self._mets_soup else self.xml

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
        # Non-page items counter starts after all page CIs (num_pages + 1)
        ci_counter = len(self.pages)

        # print(f"pages: {[p.id for p in sorted(self.pages, key=lambda x: x.number)]}")
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
                    "lg": alto_soup.find("Page").get("language", "de"),  # language
                    # for pp take last 4 strings and turn into digits
                    "pp": [page.number],
                },
                # Legacy section - tracking ALTO components
                "l": {
                    # Composite ID: {title_ppn}-{issue_ppn}-{page_filename}
                    "id": f"{self.ppn_with_date}-{page.filename}",
                    # List of parts (ALTO elements) composing this CI
                    "parts": [
                        {
                            "comp_id": tb.get("ID"),  # TextBlock ID from Alto
                            "comp_role": "body",  # role: body text
                            "comp_fileid": page.file_id,  # Alto filename
                            "comp_page_no": page.number,  # page number
                        }
                        for tb in text_blocks
                        if tb.get("ID")
                    ],
                    # Source METS filename
                    "src_files": {
                        "mets_xml": os.path.basename(self.mets_file),
                        "alto_xml": page.filename,
                        "image_tif": page.filename.replace("xml", "tif"),
                    },
                    # Additional SUB-specific identifiers
                    "ppn": self.ppn_with_date,  # Issue PPN with date
                    "title_ppn": self.title_ppn,  # Title-level PPN
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
                        "lg": None,
                        "pp": [page.number],  # page this image appears on
                        "iiif_link": os.path.join(page.iiif_img_base_uri, "info.json"),
                    },
                    # Legacy section
                    "l": {
                        # Composite ID format for images
                        "id": f"{self.ppn_with_date}-{page.filename}",
                        # Single part for the illustration element
                        "parts": [
                            {
                                "comp_id": illus_id,  # Illustration ID from Alto
                                "comp_role": "image",  # role: image
                                "comp_fileid": page.file_id,
                                "comp_page_no": page.number,
                            }
                        ],
                        "src_files": {
                            "mets_xml": os.path.basename(self.mets_file),
                            "alto_xml": page.filename,
                            "image_tif": page.filename.replace("xml", "tif"),
                        },
                        "ppn": self.ppn,
                        "title_ppn": self.title_ppn,
                    },
                }

                # Add coordinates if available
                if coords:
                    image_ci["c"] = coords

                content_items.append(image_ci)

            # === 3. Create content items for Tables ===
            for text_block in print_space.find_all("ComposedBlock"):
                block_type = text_block.get("TYPE", "")
                if block_type.lower() == "table":
                    table_id = text_block.get("ID")
                    if not table_id:
                        continue

                    # Use alto.distill_coordinates for coordinate extraction
                    coords = None
                    try:
                        coords = alto.distill_coordinates(text_block)
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
                            "lg": alto_soup.find("Page").get("language", "de"),
                            "pp": [int(page.id[-4:])],
                        },
                        # Legacy section
                        "l": {
                            # Composite ID format for tables
                            "id": f"{self.ppn_with_date}-{page.filename}",
                            # Single part for the table TextBlock
                            "parts": [
                                {
                                    "comp_id": table_id,  # TextBlock ID from Alto
                                    "comp_role": "table",  # role: table
                                    "comp_fileid": page.file_id,
                                    "comp_page_no": page.number,
                                }
                            ],
                            "src_files": {
                                "mets_xml": os.path.basename(self.mets_file),
                                "alto_xml": page.filename,
                                "image_tif": page.filename.replace("xml", "tif"),
                            },
                            "ppn": self.ppn,
                            "title_ppn": self.title_ppn,
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
        if logger.level == logging.debug:
            print(msg)

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

        reading_order_dict = get_reading_order(content_items)
        # add the reading order
        for ci in content_items:
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        # Construct the issue data according to canonical format
        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": False,  # SUB format doesn't have OLR annotations in METS
            "i": content_items,  # List of content items (pages, images, tables)
            "pp": [p.id for p in sorted(self.pages, key=lambda x: x.number)],
            "iiif_manifest_uri": f"{IIIF_ENDPOINT_URI}{self.ppn_with_date}{IIIF_MANIFEST_SUFFIX}",
            "n": self._notes,
        }

        # Add title if available
        if self.title:
            self.issue_data["media_title_variant"] = self.title

        logger.info(
            f"Parsed issue {self.id}: {len(self.pages)} pages, "
            f"{len(content_items)} content items, "
            f"title='{self.title}', ppn={self.ppn}"
        )
