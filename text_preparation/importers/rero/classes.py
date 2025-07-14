"""This module contains the definition of the RERO importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the RERO version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
from time import strftime
from typing import Any

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from impresso_essentials.utils import SourceMedium, SourceType, timestamp
from text_preparation.importers import CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPES
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
    parse_mets_amdsec,
)
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = "info.json"
IIIF_IMAGE_SUFFIX = "full/full/0/default.jpg"

# Types used in RERO2/RERO3 that are not in impresso schema
SECTION_TYPE = "section"
PICTURE_TYPE = "picture"
ILLUSTRATION_TYPE = "illustration"


def convert_coordinates(
    coords: list[float], resolution: dict[str, float], page_width: float
) -> list[int]:
    """Convert the coordinates using true and coordinate system resolutions.

    The coordinate system resolution is not necessarily the same as the true
    resolution of the image. A conversion, or rescaling can thus be necessary.
    Essentially computes fact = coordinate_width / true_width,
    and converts using x/fact.

    Args:
        coords (list[float]): List of coordinates to convert
        resolution (dict[str, float]): True resolution of the images (keys
            `x_resolution` and `y_resolution` of the dict).
        page_width (float): The page width used for the coordinate system.

    Returns:
        list[int]: The coordinates rescaled to match the true image resolution.
    """
    if resolution["x_resolution"] == 0 or resolution["y_resolution"] == 0:
        return coords
    factor = page_width / resolution["x_resolution"]
    return [int(c / factor) for c in coords]


class ReroNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in RERO (Mets/Alto) format.

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
        page_width (float): The page width used for the coordinate system.
    """

    def __init__(self, _id: str, number: int, filename: str, basedir: str) -> None:
        super().__init__(_id, number, filename, basedir)

        page_tag = self.xml.find("Page")
        self.page_width = float(page_tag.get("WIDTH"))
        # TODO add page with & height

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        self.issue = issue
        self.page_data["iiif_img_base_uri"] = os.path.join(IIIF_ENDPOINT_URI, self.id)

    # no coordinate conversion needed, but keeping it here for now
    def _convert_coordinates(self, page_regions: list[dict]) -> tuple[bool, list[dict]]:
        """Convert region coordinates to iiif format if possible.

        Note:
            Currently, no conversion of coordinates is needed.

        Args:
            page_regions (list[dict[str, Any]]): Page regions from canonical
                page format.

        Returns:
            tuple[bool, list[dict[str, Any]]]: Whether the region coordinates
                are in iiif format and page regions.
        """
        if self.issue is None:
            logger.critical("Cannot convert coordinates if issue is unknown")

        # Get page real resolution
        image_properties = self.issue.image_properties[self.number]
        x_res = image_properties["x_resolution"]
        y_res = image_properties["y_resolution"]

        # Those fields are 0 For RERO2
        if x_res == 0 or y_res == 0:
            return True, page_regions

        # Then convert coordinates of all regions/paragraphs/lines/tokens
        success = False
        try:
            for region in page_regions:
                region["c"] = convert_coordinates(region["c"], image_properties, self.page_width)
                for paragraph in region["p"]:
                    paragraph["c"] = convert_coordinates(
                        paragraph["c"], image_properties, self.page_width
                    )
                    for line in paragraph["l"]:
                        line["c"] = convert_coordinates(
                            line["c"], image_properties, self.page_width
                        )
                        for token in line["t"]:
                            token["c"] = convert_coordinates(
                                token["c"], image_properties, self.page_width
                            )
            success = True
        except Exception as e:
            logger.error("Error %s occurred when converting coordinates for %s", e, self.id)
        return success, page_regions


class ReroNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in RERO (Mets/Alto) format.

    All functions defined in this child class are specific to parsing RERO
    Mets/Alto format.

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
    """

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`ReroNewspaperPage` instances are added to :attr:`pages`.

        Raises:
            e: Instantiation of a page or adding it to :attr:`pages` failed.
        """
        alto_path = os.path.join(self.path, "ALTO")

        page_file_names = self._find_alto_files_or_retry(alto_path)

        page_numbers = []

        for fname in page_file_names:
            page_no = fname.split(".")[0]
            page_numbers.append(int(page_no))

        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers
        ]

        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                self.pages.append(ReroNewspaperPage(page_id, page_no, filename, alto_path))
            except Exception as e:
                msg = (
                    f"Adding page {page_no} {page_id} {filename}",
                    f"raised following exception: {e}",
                )
                logger.error(msg)
                raise e

    def _find_alto_files_or_retry(self, alto_path: str) -> list[str]:
        """List XML files present in given page file dir, retry up to 3 times.

        During the processing, some IO errors can randomly happen when listing
        the contents of the ALTO directory, preventing the correct parsing of
        the issue. The error is raised after the third try.
        If the Alto directory does not exist, only try once.

        Args:
            alto_path (str): Path to the directory with the Alto XML files.

        Raises:
            e: Given directory does not exist, or listing its contents failed
                three times in a row.

        Returns:
            list[str]: List of paths of the pages' Alto XML files.
        """
        if not os.path.exists(alto_path):
            logger.critical("Could not find pages for %s", self.id)
            tries = 1

        tries = 3
        for i in range(tries):
            try:
                page_file_names = [
                    file
                    for file in os.listdir(alto_path)
                    if not file.startswith(".") and ".xml" in file
                ]
                return page_file_names
            except IOError as e:
                if i < tries - 1:  # i is zero indexed
                    msg = (
                        f"Caught error for {self.id}, retrying (up to {tries} times) "
                        f"to find pages. Error: {e}."
                    )
                    logger.warning(msg)
                    continue
                else:
                    logger.warning("Reached maximum amount of errors for %s.", self.id)
                    raise e

    def _parse_content_parts(self, div: Tag) -> list[dict[str, str | int]]:
        """Parse the children of a content item div for its legacy `parts`.

        The `parts` are article-level metadata about the content item from the
        ORC and OLR processes. This information is located in an `<area>` tag.

        Args:
            div (Tag): The div containing the content item

        Returns:
            list[dict[str, str | int]]: information on different parts for the
                content item (role, id, fileid, page)
        """
        parts = []
        for child in div.children:

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

    def _get_ci_language(self, dmdid: str, mets_doc: BeautifulSoup) -> str | None:
        """Find the language code of the content item with given a `DMDID`.

        Languages are usually in a `<dmdSec>` at the beginning of a METS file,
        corresponding to the descriptive metadata.

        Args:
            dmdid (str): Descriptive metadata id of a content item.
            mets_doc (BeautifulSoup): Contents of Mets XML file.

        Returns:
            str | None: Language if defined in the file else `None`.
        """
        lang = mets_doc.find("dmdSec", {"ID": dmdid})
        if lang is None:
            return None
        lang = lang.find("MODS:languageTerm")
        if lang is None:
            return None
        return lang.text

    def _parse_content_item(
        self, item_div: Tag, counter: int, mets_doc: BeautifulSoup
    ) -> dict[str, Any]:
        """Parse a content item div and create the dictionary representing it.

        The dictionary corresponding to a content item needs to be of a precise
        format, following the canonical Issue schema.
        The counter is used to generate the canonical ID for the content item.

        Args:
            item_div (Tag): Div of content item.
            counter (int): Number of content items already added to the issue.
            mets_doc (BeautifulSoup): Contents of Mets XML file.

        Returns:
            dict[str, Any]: Content item in canonical format.
        """
        div_type = item_div.get("TYPE").lower()

        if div_type == PICTURE_TYPE or div_type == ILLUSTRATION_TYPE:
            div_type = CONTENTITEM_TYPE_IMAGE

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

        if content_item["m"]["tp"] == CONTENTITEM_TYPE_IMAGE:
            (content_item["c"], content_item["m"]["iiif_link"]) = self._get_image_info(content_item)
        return content_item

    def _decompose_section(self, div: Tag) -> list[Tag]:
        """Recursively decompose `Section` tags into a flat list of div tags.

        In RERO3, sometimes textblocks and images are withing `Section` tags.
        Those need to be recursively decomposed for all the content items to
        be parsed.

        Args:
            div (Tag): Tag of type `Section` containing other divs to extract.

        Returns:
            list[Tag]: Flat list of div tags to parse.
        """
        logger.info("Decomposing section type")
        # Only consider those with DMDID
        section_divs = [d for d in div.findAll("div") if d.get("DMDID") is not None]
        # Sort to get same IDS
        section_divs = sorted(section_divs, key=lambda x: x.get("ID").lower())

        final_divs = []
        # Now do it recursively
        for d in section_divs:
            d_type = d.get("TYPE")
            if d_type is not None:
                if d_type.lower() == SECTION_TYPE:
                    # Recursively decompose if contents are also of Sections.
                    final_divs += self._decompose_section(d)
                else:
                    final_divs.append(d)
        return final_divs

    def _parse_content_items(self, mets_doc: BeautifulSoup) -> list[dict[str, Any]]:
        """Extract content item elements from the issue's Mets XML file.

        Args:
            mets_doc (BeautifulSoup): Mets document as BeautifulSoup object.

        Returns:
            list[dict[str, Any]]: Issue's content items in canonical format.
        """
        content_items = []
        divs = mets_doc.find("div", {"TYPE": "CONTENT"}).findChildren(
            "div", recursive=False
        )  # Children of "Content" tag

        # Sort to have same naming
        sorted_divs = sorted(divs, key=lambda x: x.get("ID").lower())

        counter = 1
        for div in sorted_divs:
            # Parse Each contentitem
            div_type = div.get("TYPE")
            # To parse divs of type SECTION, need to get sub types with DMDID
            if div_type is not None and (div_type.lower() == SECTION_TYPE):
                section_divs = self._decompose_section(div)
                for d in section_divs:
                    content_items.append(self._parse_content_item(d, counter, mets_doc))
                    counter += 1
            else:
                content_items.append(self._parse_content_item(div, counter, mets_doc))
                counter += 1

        # add the reading order to the items metadata
        reading_order_dict = get_reading_order(content_items)
        for item in content_items:
            item["m"]["ro"] = reading_order_dict[item["m"]["id"]]

        return content_items

    def _parse_mets(self) -> None:
        """Parse the Mets XML file corresponding to this issue.

        Once the :attr:`issue_data` is created, containing all the relevant
        information in the canonical Issue format, the `ReroNewspaperIssue`
        instance is ready for serialization.
        """
        mets_doc = self.xml

        self.image_properties = parse_mets_amdsec(
            mets_doc,
            x_res="ImageWidth",
            y_res="ImageLength",
            x_res_default=0,
            y_res_default=0,
        )  # Parse the resolution of page images

        # Parse all the content items
        content_items = self._parse_content_items(mets_doc)

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "i": content_items,
            "pp": [p.id for p in self.pages],
        }

    def _get_image_info(self, content_item: dict[str, Any]) -> tuple[list[int], str]:
        """Recover the coordinates and iiif link for an image content item.

        The iiif link is embedded with the coordinates to directly crop the
        newspaper page to the image.

        Args:
            content_item (dict[str, Any]): Content item of an image.

        Returns:
            tuple[list[int], str]: Coordinates on the page and iiif link
        """
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

        coords = None
        max_area = 0
        # Image can have multiple parts, choose largest one (with max area)
        for part in parts:
            comp_id = part["comp_id"]

            elements = page.xml.findAll(["ComposedBlock", "TextBlock"], {"ID": comp_id})
            assert_msg = "Image comp_id matches multiple TextBlock tags"
            assert len(elements) <= 1, assert_msg
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

        coords = convert_coordinates(coords, self.image_properties[page.number], page.page_width)

        iiif_link = os.path.join(IIIF_ENDPOINT_URI, page.id, IIIF_SUFFIX)

        return coords, iiif_link
