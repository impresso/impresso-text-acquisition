"""This module contains the definition of BNF-EN importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the BNF-Europeana version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
from time import strftime
from typing import Any, Optional

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from impresso_essentials.utils import IssueDir, SourceMedium, SourceType, timestamp

from text_preparation.importers import (
    CONTENTITEM_TYPES,
    CONTENTITEM_TYPE_ADVERTISEMENT,
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_TABLE,
)
from text_preparation.importers.bnf.helpers import BNF_CONTENT_TYPES
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://gallica.bnf.fr/iiif/ark:/12148/"
IIIF_SUFFIX = "info.json"
IIIF_MANIFEST_SUFFIX = "manifest.json"
SECTION_TYPE = "section"

type_translation = {
    "illustration": CONTENTITEM_TYPE_IMAGE,
    "advertisement": CONTENTITEM_TYPE_ADVERTISEMENT,
}


class BnfEnNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in BNF-EN (Mets/Alto) format.

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

    def __init__(self, _id: str, number: int, filename: str, basedir: str) -> None:
        super().__init__(_id, number, filename, basedir)

        page_tag = self.xml.find("Page")
        self.page_width = float(page_tag.get("WIDTH"))
        # TODO add page width & heigh

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        self.issue = issue
        ark = issue.ark_link
        self.page_data["iiif_img_base_uri"] = os.path.join(
            IIIF_ENDPOINT_URI, ark, f"f{self.number}"
        )


class BnfEnNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in BNF-EN (Mets/Alto) format.

    All functions defined in this child class are specific to parsing
    BNF-Europeana Mets/Alto format.

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
        ark_link (str): IIIF Ark Id for this issue fetched on the Gallica API.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        self.ark_link = issue_dir.ark_link
        super().__init__(issue_dir)

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created CanonicalPage instances are added to pages.

        Raises:
            e: Creating a BnfEnlNewspaperPage` raised an exception.
        """
        alto_path = os.path.join(self.path, "ALTO")

        if not os.path.exists(alto_path):
            msg = f"Could not find pages for {self.id}, non-existing path: {alto_path}"
            logger.critical(msg)
            raise ValueError(msg)

        page_file_names = [
            file for file in os.listdir(alto_path) if not file.startswith(".") and ".xml" in file
        ]

        page_numbers = []

        for fname in page_file_names:
            page_no = fname.split(".")[0].split("-")[1]
            page_numbers.append(int(page_no))

        page_canonical_names = [f"{self.id}-p{str(page_n).zfill(4)}" for page_n in page_numbers]

        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                self.pages.append(BnfEnNewspaperPage(page_id, page_no, filename, alto_path))
            except Exception as e:
                msg = f"Adding page {page_no} {page_id} {filename} raised following exception: {e}"
                logger.error(msg)
                raise Exception(msg) from e

    def _parse_content_parts(self, content_div: Tag) -> list[dict[str, Any]]:
        """Parse given div's children tags to create legacy `parts` component.

        Given the div of a content item, this function parses the children and
        constructs the legacy `parts` component.

        Args:
            content_div (Tag): The div containing the content item.

        Returns:
            list[dict[str, Any]]: List of dicts of each content part of item.
        """
        parts = []
        for child in content_div.children:

            if isinstance(child, NavigableString):
                continue
            elif isinstance(child, Tag):
                type_attr = child.get("TYPE")
                comp_role = type_attr.lower() if type_attr else None
                areas = child.findAll("area")
                for area in areas:
                    comp_id = area.get("BEGIN")
                    comp_fileid = area.get("FILEID")
                    comp_page_no = int(comp_fileid.replace("ALTO", ""))

                    parts.append(
                        {
                            "comp_role": comp_role,
                            "comp_id": comp_id,
                            "comp_fileid": comp_fileid,
                            "comp_page_no": comp_page_no,
                        }
                    )
        return parts

    def _get_ci_language(self, dmdid: str, mets_doc: BeautifulSoup) -> Optional[str]:
        """Find the language code of the CI with given DMDID.

        Languages are usually in a dmdSec at the beginning of a METS file.

        Args:
            dmdid (str): Identifier of the content item in the dmd section.
            mets_doc (BeautifulSoup): Contents of the Mets XML file.

        Returns:
            Optional[str]: Language or None if not present in Mets file.
        """
        lang = mets_doc.find("dmdSec", {"ID": dmdid})
        if lang is None:
            return None
        lang = lang.find("mods:languageTerm")
        if lang is None:
            return None
        return lang.text

    def _parse_content_item(
        self, item_div: Tag, counter: int, mets_doc: BeautifulSoup
    ) -> dict[str, Any]:
        """Parse a content item div and returns the dictionary representing it.

        Args:
            item_div (Tag): Div of content item.
            counter (int): Number of content items already added (needed to
                generate canonical id).
            mets_doc (BeautifulSoup): Contents of the Mets XML file.

        Returns:
            dict[str, Any]: Resulting content item in canonical format.
        """
        div_type = item_div.get("TYPE").lower()
        # Translate types
        if div_type in type_translation:
            div_type = type_translation[div_type]

        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning("Found new content item type: %s", div_type)

        metadata = {
            "id": f"{self.id}-i{str(counter).zfill(4)}",
            "tp": div_type,
            "pp": [],
            "t": item_div.get("LABEL"),
        }

        # Get CI language
        language = self._get_ci_language(item_div.get("DMDID"), mets_doc)
        if language is not None:
            metadata["lg"] = language

        content_item = {
            "m": metadata,
            "l": {
                "id": item_div.get("ID"),
                "parts": self._parse_content_parts(item_div),
            },
        }
        for p in content_item["l"]["parts"]:
            pge_no = p["comp_page_no"]
            if pge_no not in content_item["m"]["pp"]:
                content_item["m"]["pp"].append(pge_no)

        if div_type in [CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPE_TABLE]:
            content_item["c"], content_item["m"]["iiif_link"] = self._get_image_info(content_item)

        return content_item

    def _decompose_section(self, div: Tag) -> list[Tag]:
        """Recursively decompose the given `section` div into individual items.

        In Mets, sometimes textblocks and images are withing `Section` tags.
        Those need to be recursively decomposed to reach the content item divs.

        Args:
            div (Tag): The `Section` div to decompose.

        Returns:
            list[Tag]: List of all children divs not of type `Section`.
        """
        logger.info("Decomposing section type")
        section_divs = [d for d in div.findAll("div") if d.get("TYPE").lower() in BNF_CONTENT_TYPES]
        # Sort to get same IDS
        section_divs = sorted(section_divs, key=lambda x: x.get("ID").lower())

        final_divs = []
        # Now do it recursively
        for d in section_divs:
            d_type = d.get("TYPE")
            if d_type is not None:
                if d_type.lower() == SECTION_TYPE:
                    # Recursively decompose
                    final_divs += self._decompose_section(d)
                else:
                    final_divs.append(d)

        return final_divs

    def _parse_content_items(self) -> list[dict[str, Any]]:
        """Extract content item elements from the issue's Mets XML file.

        Returns:
            list[dict[str, Any]]: List of content items in canonical format.
        """
        content_items = []
        doc = self.xml

        dmd_sections = doc.findAll("dmdSec")
        struct_map = doc.find("div", {"TYPE": "CONTENT"})

        # Sort to have same namings - reading order is added separately later
        sorted_divs = sorted(dmd_sections, key=lambda x: x.get("ID").lower())

        counter = 1
        for d in sorted_divs:
            div = struct_map.findAll("div", {"DMDID": d.get("ID")})
            if len(div) == 0:
                continue
            if len(div) > 1:
                logger.warning(
                    "Multiple divs matching %s in structmap for %s",
                    d.get("ID"),
                    self.id,
                )
            else:
                div = div[0]
                div_type = div.get("TYPE").lower()
                if div_type == SECTION_TYPE:
                    section_divs = self._decompose_section(div)
                    for sd in section_divs:
                        content_items.append(self._parse_content_item(sd, counter, doc))
                        counter += 1
                else:
                    content_items.append(self._parse_content_item(div, counter, doc))
                    counter += 1

        # add the reading order to the items metadata
        reading_order_dict = get_reading_order(content_items)
        for item in content_items:
            item["m"]["ro"] = reading_order_dict[item["m"]["id"]]

        return content_items

    def _get_image_info(self, content_item: dict[str, Any]) -> tuple[list[int], str]:
        """Given an image content item, get its coordinates and iiif url.

        TODO: Find an approach to reduce the number of calls to page.xml

        Args:
            content_item (dict[str, Any]): Content item in canonical format.

        Returns:
            tuple[list[int], str]: Content item coordinates and iiif url.
        """
        # Fetch the legacy parts

        # Images cannot be on multiple pages
        num_pages = len(content_item["m"]["pp"])
        assert num_pages == 1, "Image is on more than one page"

        page_nb = content_item["m"]["pp"][0]
        page = [p for p in self.pages if p.number == page_nb][0]
        parts = content_item["l"]["parts"]

        assert len(parts) >= 1, f"No parts for image {content_item['m']['id']}"

        if len(parts) > 1:
            logger.info(
                "Found multiple parts for image %s, selecting largest one",
                content_item["m"]["id"],
            )

        page_doc = page.xml
        coords = None
        max_area = 0
        # Image can have multiple parts, choose largest one (with max area)
        for part in parts:
            comp_id = part["comp_id"]

            elements = page_doc.findAll(["ComposedBlock", "TextBlock"], {"ID": comp_id})
            assert len(elements) <= 1, "Image comp_id matches multiple TextBlock tags"
            if len(elements) == 0:
                continue

            element = elements[0]
            hpos, vpos = element.get("HPOS"), element.get("VPOS")
            width, height = element.get("WIDTH"), element.get("HEIGHT")

            # Select largest image
            area = int(float(width)) * int(float(height))
            if area > max_area:
                max_area = area
                coords = [
                    int(float(hpos)),
                    int(float(vpos)),
                    int(float(width)),
                    int(float(height)),
                ]

        # coords = convert_coordinates(coords, self.image_properties[page.number], page.page_width)
        iiif_link = os.path.join(IIIF_ENDPOINT_URI, self.ark_link, f"f{page.number}", IIIF_SUFFIX)

        return coords, iiif_link

    def _parse_mets(self) -> None:
        content_items = self._parse_content_items()

        iiif_manifest = os.path.join(IIIF_ENDPOINT_URI, self.ark_link, IIIF_MANIFEST_SUFFIX)

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "i": content_items,
            "pp": [p.id for p in self.pages],
            "iiif_manifest_uri": iiif_manifest,
        }
