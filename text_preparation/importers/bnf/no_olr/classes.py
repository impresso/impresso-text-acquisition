"""This module contains the definition of BNF OCR-only importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the BNF version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import gzip
import logging
import os
import json
from glob import glob
from time import strftime
from typing import Any

from bs4 import BeautifulSoup
from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

from text_preparation.importers import CONTENTITEM_TYPE_IMAGE
from text_preparation.importers.bnf.helpers import (
    BNF_CONTENT_TYPES,
    add_div,
    type_translation,
)
from text_preparation.importers.bnf.parsers import (
    parse_div_parts,
    parse_embedded_cis,
    parse_printspace,
)
from text_preparation.importers.mets_alto import MetsAltoCanonicalIssue, MetsAltoCanonicalPage, alto
from text_preparation.importers.mets_alto.alto import distill_coordinates, parse_style
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

IIIF_IMAGE_URI = "https://openapi.bnf.fr/iiif/image/v3/ark:/12148/"
IIIF_PRES_URI = "https://openapi.bnf.fr/iiif/presentation/v3/ark:/12148/"
IIIF_MANIFEST_SUFFIX = "manifest.json"
IIIF_SUFFIX = "info.json"


class BnfNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in BNF (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.
        is_gzip (bool): Whether the page's corresponding file is in .gzip.
        ark_link (str): IIIF Ark identifier for this page.
    """

    def __init__(
        self, _id: str, number: int, filename: str, basedir: str, page_size: tuple[int, int]
    ) -> None:

        # self.is_gzip = filename.endswith("gz") not applicable in this case
        super().__init__(_id, number, filename, basedir)

        # Add the facsimile height and width to the page data
        self.page_data["fw"] = page_size[0]
        self.page_data["fh"] = page_size[1]
        # Alto file ID in the BNF system - "ocr.1" for first ocr page
        self.file_id = f"ocr.{number}"
        self.iiif_img_base_uri = None
        self.ark_id = None
        # ark id is at issue level and already present in the issue object upon creation
        # self.ark_link = self.xml.find("fileIdentifier").getText()

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        self.issue = issue
        self.ark_id = self.issue.ark_id
        self.iiif_img_base_uri = os.path.join(IIIF_IMAGE_URI, self.ark_id, f"f{self.number}")
        self.page_data["iiif_img_base_uri"] = self.iiif_img_base_uri

    @property
    def xml(self) -> BeautifulSoup:
        """Read Alto XML file of the page and create a BeautifulSoup object.

        This property had to be overridden because this collection presents
        multiple possible encodings ('utf-8' for most, and sometimes "iso-8859-1")

        Returns:
            BeautifulSoup: BeautifulSoup object with Alto XML of the page.
        """
        alto_xml_path = os.path.join(self.basedir, self.filename)

        # In case of I/O error, retry twice,
        tries = 3
        for i in range(tries):
            try:
                with open(alto_xml_path, "r", encoding=self.encoding) as f:
                    raw_xml = f.read()

                alto_doc = BeautifulSoup(raw_xml, "xml")
                return alto_doc
            except UnicodeDecodeError as e:
                msg = f"WANRING - {self.id} - Trying to decode with default encoding {self.encoding} failed - retrying with 'iso-8859-1'. Error : {e}"
                print(msg)
                self.encoding = "iso-8859-1"
                logger.error(msg)
                continue
            except IOError as e:
                if i < tries - 1:  # i is zero indexed
                    msg = (
                        f"Caught error for {self.id}, retrying (up to {tries} "
                        f"times) to read xml file. Error: {e}."
                    )
                    logger.error(msg)
                    continue
                else:
                    logger.error("Reached maximum amount of errors for %s.", self.id)
                    raise e

    def parse(self) -> None:
        doc = self.xml

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
        page_data, notes = parse_printspace(pselement, mappings)
        self.page_data["cc"], self.page_data["r"] = self._convert_coordinates(page_data)
        if len(notes) > 0:
            self.page_data["n"] = notes


class BnfNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in BNF format without OLR (Alto).

    All functions defined in this child class are specific to parsing BNF
    OCR-only Alto format.

    All OCR-only data is form the new set of API downloaded data,
    so we know that the format and directory structure is always the same.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`CanonicalPage` instances from this issue.
        image_properties (dict[str, Any]): metadata allowing to convert region
            OCR/OLR coordinates to iiif format compliant ones.
        ark_id (int): Issue ARK identifier, for the issue's pages' iiif links.
        issue_uid (str): Basename of the Mets XML file of this issue.
        secondary_date (datetime.date): Potential secondary date of issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        # self.issue_uid = os.path.basename(issue_dir.path)

        self.secondary_date = issue_dir.secondary_date
        self.ark_id = issue_dir.ark_id
        self.title_ark_id = issue_dir.title_ark
        self.manifest_contents = None
        self.media_title_variant = None

        print(f"self.ark_id: {self.ark_id}, self.title_ark_id: {self.title_ark_id}")

        self.iiif_manifest = os.path.join(IIIF_PRES_URI, self.ark_id, IIIF_MANIFEST_SUFFIX)

        # TODO REMOVE ALL METS/OLR related stuff
        super().__init__(issue_dir)
        # TODO add page width & height

        self.content_items = self._find_content_items()

        # by default the date is considered to be exact
        is_exact_date = True

        # Note for newspapers with two dates (197 cases)
        if self.secondary_date is not None:
            # when the secondary date is only a year or a month, the date is not exact
            if len(self.secondary_date.split("-")) < 3:
                msg = f"{self.id} - Secondary date {self.secondary_date} has only year or year-month. Setting extract_date=False."
                print(msg)
                self._notes.append(msg)
                is_exact_date = False
            else:
                self._notes.append(f"Secondary date {self.secondary_date}")

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": False,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "iiif_manifest_uri": self.iiif_manifest,
            "is_exact_date": is_exact_date,
            "n": self._notes,
        }

        if self.media_title_variant is not None:
            # the media title variant is defined if it is found in the manifest json file
            self.issue_data["media_title_variant"] = self.media_title_variant

    @property
    def xml(self) -> None:
        """Override the XML property of the parent class since we don't have METS files."""

    def _parse_mets(self) -> None:
        pass

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`BnfCanonicalPage` instances are added to :attr:`pages`.

        Raises:
            e: Instantiation of a page or adding it to :attr:`pages` failed.
        """
        manifest_path = os.path.join(self.path, "manifest.json")
        # TODO extract size and width from the manifest file
        with open(manifest_path, "r", encoding="utf-8") as fin:
            manifest_contents = json.load(fin)

        # take the opportunity to define the media title variant
        for m_dict in manifest_contents["metadata"]:
            if m_dict["label"]["fr"] == ["Titre"]:
                self.media_title_variant = m_dict["value"]["fr"][0]

        pages = [
            (file, int(file.split(".")[0][-4:]))
            for file in os.listdir(self.path)
            if not file.startswith(".") and ".xml" in file
        ]

        # sort the pages
        page_filenames, page_numbers = zip(*sorted(pages, key=lambda x: x[1]))

        self.pages = []
        for filename, page_no in zip(page_filenames, page_numbers):
            page_id = filename.split(".")[0]
            # directly fetch the page width and height from the iiif presentation API
            page_w = manifest_contents["items"][page_no - 1]["width"]
            page_h = manifest_contents["items"][page_no - 1]["height"]
            try:
                self.pages.append(
                    BnfNewspaperPage(page_id, page_no, filename, self.path, (page_w, page_h))
                )
            except Exception as e:
                logger.error(
                    "Adding page %s %s %s raised following exception: %s",
                    page_no,
                    page_id,
                    filename,
                    e,
                )
                raise e

    def _find_content_items(self) -> list[dict[str, Any]]:
        """Extract content items from Alto files for this issue.

        For BNF data without OLR (Optical Layout Recognition at article level),
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

        # Process each page to extract content items
        for page in self.pages:
            page_num_str = str(page.number).zfill(4)

            # Use page.xml property instead of opening file manually
            try:
                alto_soup = page.xml
            except Exception as e:
                msg = f"Failed to parse Alto file for page {page.number}: {e}"
                self._notes.append(msg)
                logger.error(msg)
                continue

            # Get the PrintSpace element which contains the page content
            print_space = alto_soup.find("PrintSpace")
            if not print_space:
                msg = f"No PrintSpace found in Alto file for page {page.number} - {page.local_path}"
                self._notes.append(msg)
                logger.error(msg)
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
                    "id": f"{self.ark_id}/f{page.number}",
                    # List of parts (ALTO elements) composing this CI
                    "parts": [
                        {
                            "comp_id": tb.get("ID"),  # TextBlock ID from Alto
                            "comp_role": "body",  # role: body text
                            "comp_fileid": page.file_id,
                            "comp_page_no": page.number,  # page number
                        }
                        for tb in text_blocks
                        if tb.get("ID")
                    ],
                    # Source METS filename
                    "src_files": {
                        # TODO add the mets if it ends up existing
                        # "mets_xml": os.path.basename(self.mets_file),
                        "presentation_manifest": self.iiif_manifest,
                        "alto_xml": page.filename,
                    },
                    # Additional BNF-specific identifiers
                    "ark_id": self.ark_id,  # Issue ark_id
                    "title_ark_id": self.title_ark_id,  # Title-level ark_id
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
                        "iiif_link": os.path.join(
                            IIIF_IMAGE_URI, self.ark_id, f"f{page.number}", "info.json"
                        ),
                    },
                    # Legacy section
                    "l": {
                        # Composite ID format for images
                        "id": f"{self.ark_id}/f{page.number}",
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
                            "presentation_manifest": self.iiif_manifest,
                            "image_tif": page.filename.replace("xml", "tif"),
                        },
                        "ark_id": self.ark_id,
                        "title_ark_id": self.title_ark_id,
                    },
                }

                # Add coordinates if available
                if coords:
                    image_ci["c"] = coords

                content_items.append(image_ci)

            # === 3. Create content items for Tables ===
            for text_block in print_space.find_all(["ComposedBlock", "TextBlock"], TYPE="table"):
                # block_type = text_block.get("TYPE", "")
                # if block_type.lower() == "table":
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
                        "id": f"{self.ark_id}/f{page.number}",
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
                            "presentation_manifest": self.iiif_manifest,
                            "alto_xml": page.filename,
                        },
                        "ark_id": self.ark_id,
                        "title_ark_id": self.title_ark_id,
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

        # now setting the reading order for each of them
        reading_order_dict = get_reading_order(content_items)
        # add the reading order
        for ci in content_items:
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        return content_items
