"""This module contains the definition of the Luxembourg importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the BNL (Blibliotheque Nationale du Luxembourg) version of the Mets/Alto format
to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
import re
from time import strftime
from typing import Any

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from impresso_essentials.utils import SourceMedium, SourceType, timestamp
from text_preparation.importers import (
    CONTENTITEM_TYPE_ADVERTISEMENT,
    CONTENTITEM_TYPE_ARTICLE,
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_OBITUARY,
    CONTENTITEM_TYPE_TABLE,
    CONTENTITEM_TYPE_WEATHER,
)
from text_preparation.importers.lux.helpers import (
    convert_coordinates,
    encode_ark,
    section_is_article,
    div_has_body,
    find_section_articles,
    remove_section_cis,
)
from text_preparation.importers.mets_alto.alto import parse_style
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
    parse_mets_amdsec,
)
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URI = "https://iiif.eluxemburgensia.lu/image/iiif/2"
IIIF_SUFFIX = "info.json"


class LuxNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in BNL (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file.
    """

    def _parse_font_styles(self) -> None:
        """Parse section `<TextStyle>` of the XML file to extract the fonts."""
        style_divs = self.xml.findAll("TextStyle")

        styles = []
        for d in style_divs:
            styles.append(parse_style(d))

        self.page_data["s"] = styles

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        self.issue = issue
        encoded_ark_id = encode_ark(self.issue.ark_id)
        iiif_base_link = f"{IIIF_ENDPOINT_URI}/{encoded_ark_id}"
        iiif_link = f"{iiif_base_link}%2fpages%2f{self.number}"
        self.page_data["iiif_img_base_uri"] = iiif_link
        self._parse_font_styles()

    def _convert_coordinates(self, page_regions: list[dict]) -> tuple[bool, list[dict]]:
        success = False
        try:
            # TODO add width & height
            img_props = self.issue.image_properties[self.number]
            x_res = img_props["x_resolution"]
            y_res = img_props["y_resolution"]

            for region in page_regions:

                x, y, w, h = region["c"]
                region["c"] = convert_coordinates(x, y, w, h, x_res, y_res)

                msg = f"Page {self.number}: {x},{y},{w},{h} => {region['c']}"
                logger.debug(msg)

                for paragraph in region["p"]:

                    x, y, w, h = paragraph["c"]
                    paragraph["c"] = convert_coordinates(x, y, w, h, x_res, y_res)

                    msg = f"(para) Page {self.number}: {x},{y},{w},{h} => {paragraph['c']}"
                    logger.debug(msg)

                    for line in paragraph["l"]:

                        x, y, w, h = line["c"]
                        line["c"] = convert_coordinates(x, y, w, h, x_res, y_res)

                        msg = f"(line) Page {self.number}: {x},{y},{w},{h} => {paragraph['c']}"
                        logger.debug(msg)

                        for token in line["t"]:
                            x, y, w, h = token["c"]
                            token["c"] = convert_coordinates(x, y, w, h, x_res, y_res)

                            msg = f"(token) Page {self.number}: {x},{y},{w},{h} => {token['c']}"
                            logger.debug(msg)
            success = True
        except Exception as e:
            logger.error("Error %s occurred when converting coordinates for %s", e, self.id)

        return success, page_regions


class LuxNewspaperIssue(MetsAltoCanonicalIssue):
    """Class representing an issue in BNL data.

    All functions defined in this child class are specific to parsing BNL
    (Luxembourg National Library) Mets/Alto format.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict): Issue data according to canonical Issue format.
        pages (list): List of :obj:`CanonicalPage` instances from this issue.
        image_properties (dict): metadata allowing to convert region OCR/OLR
            coordinates to iiif format compliant ones.
        ark_id (int): Issue ARK identifier, for the issue's pages' iiif links.
    """

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`LuxoNewspaperPage` instances are added to :attr:`pages`.

        Raises:
            e: Instantiation of a page or adding it to :attr:`pages` failed.
        """
        # get the canonical names for pages in the newspaper issue by
        # visiting the `text` sub-folder with the alto XML files
        text_path = os.path.join(self.path, "text")
        page_file_names = [
            file for file in os.listdir(text_path) if not file.startswith(".") and ".xml" in file
        ]

        page_numbers = []
        page_match_exp = r"(.*?)(\d{5})(.*)"

        for fname in page_file_names:
            g = re.match(page_match_exp, fname)
            page_no = g.group(2)
            page_numbers.append(int(page_no))

        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers
        ]

        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                self.pages.append(LuxNewspaperPage(page_id, page_no, filename, text_path))
            except Exception as e:
                msg = (
                    f"Adding page {page_no} {page_id} {filename}",
                    f"raised following exception: {e}",
                )
                logger.error(msg)
                raise e

    def _parse_mets_div(self, div: Tag) -> list[dict[str, str | int]]:
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

    def _parse_dmdsec(self, mets_doc: BeautifulSoup) -> tuple[list[dict[str, Any]], int]:
        """Parse `<dmdSec>` tags of Mets file to find some content items.

        Only articles and pictures are in this section and are identified
        here. Those tags' `ID` attribute should contain `ARTICLE` or `PICT`.

        The numbering of the content items (used for the canonical IDs) is
        generated based on the sorting of the ID strings of the sections.

        Args:
            mets_doc (BeautifulSoup): Contents of Mets XML file.

        Returns:
            tuple[list[dict[str, Any]], int]: Parsed CI's and counter to keep
                track of the item numbers.
        """
        content_items = []
        sections = mets_doc.findAll("dmdSec")

        # sort based on the ID string to pinpoint the generated canonical IDs
        sections = sorted(sections, key=lambda elem: elem.get("ID").split("_")[1])
        counter = 1
        for section in sections:
            section_id = section.get("ID")

            if "ARTICLE" in section_id or "PICT" in section_id:
                # Get title Info
                title_elements = section.find_all("titleInfo")
                item_title = (
                    title_elements[0].getText().replace("\n", " ").strip()
                    if len(title_elements) > 0
                    else None
                )

                # Prepare ci metadata
                metadata = {
                    "id": f"{self.id}-i{str(counter).zfill(4)}",
                    "pp": [],
                    "tp": (
                        CONTENTITEM_TYPE_ARTICLE
                        if "ARTICLE" in section_id
                        else CONTENTITEM_TYPE_IMAGE
                    ),
                }

                # Find the parts
                try:
                    item_div = mets_doc.findAll("div", {"DMDID": section_id})[0]
                    parts = self._parse_mets_div(item_div)
                except IndexError:
                    err_msg = f"<div DMID={section_id}> not found {self.path}"
                    self._notes.append(err_msg)
                    logger.error(err_msg)
                    parts = []
                    item_div = None

                if item_title:
                    metadata["t"] = item_title

                # Finalize the item
                item = {"m": metadata, "l": {"id": section_id, "parts": parts}}

                # TODO: keep language (there may be more than one)
                if item["m"]["tp"] == CONTENTITEM_TYPE_ARTICLE:
                    lang = section.find_all("languageTerm")[0].getText()
                    item["m"]["lg"] = lang

                # This has been added to not consider ads as pictures
                if not (
                    (item_div is not None)
                    and ("PICT" in section_id)
                    and (item_div.get("TYPE") == "ADVERTISEMENT")
                ):
                    content_items.append(item)
                counter += 1

        return content_items, counter

    def _parse_structmap_divs(
        self, start_counter: int, mets_doc: BeautifulSoup
    ) -> tuple[list[dict[str, Any]], int]:
        """Parse content items only in the Logical `<structMap>` of Mets file.

        Args:
            start_counter (int): item number to start with for the CIs found
            mets_doc (BeautifulSoup): Contents of Mets XML file.

        Returns:
            tuple[list[dict[str, Any]], int]: Parsed CI's and updated counter
                to keep track of the item numbers.
        """
        content_items = []
        counter = start_counter
        element = mets_doc.find("structMap", {"TYPE": "LOGICAL"})

        allowed_types = ["ADVERTISEMENT", "DEATH_NOTICE", "WEATHER"]
        divs = []

        for div_type in allowed_types:
            divs += element.findAll("div", {"TYPE": div_type})

        sorted_divs = sorted(divs, key=lambda elem: elem.get("ID"))

        for div in sorted_divs:

            div_type = div.get("TYPE").lower()
            if div_type == "advertisement":
                content_item_type = CONTENTITEM_TYPE_ADVERTISEMENT
            elif div_type == "weather":
                content_item_type = CONTENTITEM_TYPE_WEATHER
            elif div_type == "death_notice":
                content_item_type = CONTENTITEM_TYPE_OBITUARY
            else:
                continue

            # The language of those CI should be in
            # the DMDSEC of their parent section.
            metadata = {
                "id": f"{self.id}-i{str(counter).zfill(4)}",
                "tp": content_item_type,
                "pp": [],
                "t": div.get("LABEL"),
            }

            item = {
                "m": metadata,
                "l": {
                    "parts": self._parse_mets_div(div),  # Parse the parts
                    "id": div.get("ID"),
                },
            }
            content_items.append(item)
            counter += 1

        return content_items, counter

    def _process_image_ci(self, ci: dict[str, Any], mets_doc: BeautifulSoup) -> None:
        """Process an image content item to complete its information.

        Args:
            ci (dict[str, Any]): Image content item to be processed.
            mets_doc (BeautifulSoup): Contents of Mets XML file.
        """
        item_div = mets_doc.findAll("div", {"DMDID": ci["l"]["id"]})
        if len(item_div) > 0:
            item_div = item_div[0]
        else:
            return
        legacy_id = item_div.get("ID")
        # Image is actually table

        if item_div.get("TYPE").lower() == "table":
            ci["m"]["tp"] = CONTENTITEM_TYPE_TABLE
            for part in ci["l"]["parts"]:
                page_no = part["comp_page_no"]
                if page_no not in ci["m"]["pp"]:
                    ci["m"]["pp"].append(page_no)

        elif item_div.get("TYPE").lower() == "illustration":

            # filter content item part that is the actual image
            # the other part is the caption
            try:
                part = [part for part in ci["l"]["parts"] if part["comp_role"] == "image"][0]
            except IndexError as e:
                err_msg = f"{legacy_id} without image subpart"
                err_msg += f"; {legacy_id} has {ci['l']['parts']}"
                logger.error(err_msg)
                self._notes.append(err_msg)
                logger.exception(e)
                return

            # for each "part" open the XML file of corresponding page
            # get the coordinates and convert them
            # some imgs are in fact tables (meaning they have text
            # recognized)

            # find the corresponding page where it's located
            curr_page = None
            for page in self.pages:
                if page.number == part["comp_page_no"]:
                    curr_page = page

            # add the page number to the content item
            assert curr_page is not None
            if curr_page.number not in ci["m"]["pp"]:
                ci["m"]["pp"].append(curr_page.number)

            try:
                # parse the Alto file to fetch the coordinates
                page_xml = curr_page.xml

                composed_block = page_xml.find("ComposedBlock", {"ID": part["comp_id"]})

                if composed_block:
                    graphic_el = composed_block.find("GraphicalElement")

                    if graphic_el is None:
                        graphic_el = page_xml.find("Illustration")
                else:
                    graphic_el = page_xml.find("Illustration", {"ID": part["comp_id"]})

                hpos = int(graphic_el.get("HPOS"))
                vpos = int(graphic_el.get("VPOS"))
                width = int(graphic_el.get("WIDTH"))
                height = int(graphic_el.get("HEIGHT"))
                img_props = self.image_properties[curr_page.number]
                x_resolution = img_props["x_resolution"]
                y_resolution = img_props["y_resolution"]
                # order should be: hpos, vpos, width, height
                coordinates = convert_coordinates(
                    hpos, vpos, width, height, x_resolution, y_resolution
                )
                encoded_ark_id = encode_ark(self.ark_id)
                iiif_base_link = f"{IIIF_ENDPOINT_URI}/{encoded_ark_id}"
                ci["m"]["iiif_link"] = f"{iiif_base_link}%2fpages%2f{curr_page.number}/info.json"
                ci["c"] = list(coordinates)
                del ci["l"]["parts"]
            except Exception as e:
                err_msg = f"An error occurred with {os.path.join(curr_page.basedir, curr_page.filename)}. "
                err_msg += f"<ComposedBlock> @ID {part['comp_id']} not found"
                logger.error(err_msg)
                self._notes.append(err_msg)
                logger.exception(e)

    def _parse_section(
        self,
        section: Tag,
        section_div: Tag,
        content_items: list[dict[str, Any]],
        counter: int,
    ) -> dict[str, Any]:
        """Reconstruct the section using the div and previously created CIs.

        In the `l` field of the ci, an additional field `canonical_parts`
        points to articles that were added to this section.
        (Bugfix done by Edoardo)

        Args:
            section (Tag): `<dmdSec>` section of the Mets XML file.
            section_div (Tag): `<div>` section with corresponding DMDID.
            content_items (list[dict[str, Any]]): Incomplete content items.
            counter (int): Content item counter.

        Returns:
            dict[str, Any]: Content item of the reconstructed section.
        """
        title_elements = section.find_all("titleInfo")

        item_title = (
            title_elements[0].getText().replace("\n", " ").strip()
            if len(title_elements) > 0
            else None
        )

        metadata = {
            "id": f"{self.id}-i{str(counter).zfill(4)}",
            "pp": [],
            "tp": CONTENTITEM_TYPE_ARTICLE,
        }
        if item_title:
            metadata["t"] = item_title

        parts = self._parse_mets_div(section_div)

        old_cis = find_section_articles(section_div, content_items)
        item = {
            "m": metadata,
            "l": {
                "id": section_div.get("DMDID"),
                "parts": parts,
                "canonical_parts": old_cis,
            },
        }
        return item

    def _parse_sections(
        self,
        content_items: list[dict[str, Any]],
        start_counter: int,
        mets_doc: BeautifulSoup,
    ) -> list[dict[str, Any]]:
        """Reconstruct all the sections from the METS file (bugfix by Edoardo).

        Args:
            content_items (list[dict[str, Any]]): Current content items.
            start_counter (int):  Content item counter.
            mets_doc (BeautifulSoup): Contents of Mets XML file.

        Returns:
            list[dict[str, Any]]: Updated content items
        """
        counter = start_counter
        sections = mets_doc.findAll("dmdSec")

        sections = sorted(sections, key=lambda elem: elem.get("ID").split("_")[1])
        # First look for sections and get their ID
        new_sections = []
        for section in sections:
            section_id = section.get("ID")
            if "SECT" in section_id:
                div = mets_doc.find("div", {"DMDID": section_id})  # Get div
                if div is None:
                    err_msg = f"<div [DMID]={section_id}> not found {self.path}"
                    self._notes.append(err_msg)
                    logger.error(err_msg)
                    continue
                if div_has_body(div) and section_is_article(div):
                    new_section = self._parse_section(section, div, content_items, counter)
                    new_sections.append(new_section)
                    counter += 1
        return new_sections

    def _parse_mets(self) -> None:
        """Parse the Mets XML file corresponding to this issue.

        Once the :attr:`issue_data` is created, containing all the relevant
        information in the canonical Issue format, the `LuxNewspaperIssue`
        instance is ready for serialization.

        TODO: correct parsing to prevent need of reconstruction (if possible).
        """
        mets_doc = self.xml

        # explain
        self.image_properties = parse_mets_amdsec(
            mets_doc, x_res="xOpticalResolution", y_res="yOpticalResolution"
        )

        # First find `ARTICLE` and `PICTURE` content items
        content_items, counter = self._parse_dmdsec(mets_doc)
        # Then find other content items
        new_cis, counter = self._parse_structmap_divs(counter, mets_doc)
        content_items += new_cis

        # Reconstruct sections
        section_cis = self._parse_sections(content_items, counter, mets_doc)
        # Remove cis that are contained in sections
        content_items, removed = remove_section_cis(content_items, section_cis)

        logger.debug("Removed %s as they are in sections", removed)
        # Add sections to CIs
        content_items += section_cis
        # Set ark_id
        ark_link = mets_doc.find("mets").get("OBJID")
        self.ark_id = ark_link.replace("https://persist.lu/ark:/", "ark:")

        # compute the reading order for the issue's items
        reading_order_dict = get_reading_order(content_items)

        for ci in content_items:

            # ci['l']['parts'] = self._parse_mets_div(item_div)

            if ci["m"]["tp"] == "image":
                self._process_image_ci(ci, mets_doc)
            elif ci["m"]["tp"]:
                for part in ci["l"]["parts"]:
                    page_no = part["comp_page_no"]
                    if page_no not in ci["m"]["pp"]:
                        ci["m"]["pp"].append(page_no)

            # add the reading order
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "i": content_items,
            "pp": [p.id for p in self.pages],
        }

        if self._notes:
            self.issue_data["n"] = "\n".join(self._notes)
