"""This module contains the definition of BNF importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the BNF version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import gzip
import logging
import os
from glob import glob
from time import strftime

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
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.importers.mets_alto.alto import distill_coordinates, parse_style
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://gallica.bnf.fr/iiif"
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

    def __init__(self, _id: str, number: int, filename: str, basedir: str) -> None:

        self.is_gzip = filename.endswith("gz")
        super().__init__(_id, number, filename, basedir)
        self.ark_link = self.xml.find("fileIdentifier").getText()

    def _parse_font_styles(self) -> None:
        """Parse the styles at the page level."""
        style_divs = self.xml.findAll("TextStyle")

        styles = []
        for d in style_divs:
            styles.append(parse_style(d))

        self.page_data["s"] = styles

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        self.issue = issue
        self.page_data["iiif_img_base_uri"] = os.path.join(IIIF_ENDPOINT_URI, self.ark_link)
        self._parse_font_styles()

    def parse(self) -> None:
        doc = self.xml

        mappings = {}
        for ci in self.issue.issue_data["i"]:
            ci_id = ci["m"]["id"]
            if "parts" in ci["l"]:
                for part in ci["l"]["parts"]:
                    mappings[part["comp_id"]] = ci_id

        pselement = doc.find("PrintSpace")
        page_data, notes = parse_printspace(pselement, mappings)
        self.page_data["cc"], self.page_data["r"] = self._convert_coordinates(page_data)
        if len(notes) > 0:
            self.page_data["n"] = notes

    @property
    def xml(self) -> BeautifulSoup:
        """Read Alto XML file of the page and create a BeautifulSoup object.

        Redefined function as for some issues, the pages are in gz format.

        Returns:
            BeautifulSoup: BeautifulSoup object with Alto XML of the page.
        """
        if not self.is_gzip:
            return super(BnfNewspaperPage, self).xml
        else:
            alto_xml_path = os.path.join(self.basedir, self.filename)
            with gzip.open(alto_xml_path, "r") as f:
                raw_xml = f.read()

            alto_doc = BeautifulSoup(raw_xml, "xml")
            return alto_doc


class BnfNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in BNF (Mets/Alto) format.

    All functions defined in this child class are specific to parsing BNF
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
        issue_uid (str): Basename of the Mets XML file of this issue.
        secondary_date (datetime.date): Potential secondary date of issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        self.issue_uid = os.path.basename(issue_dir.path)
        self.secondary_date = issue_dir.secondary_date
        super().__init__(issue_dir)
        # TODO add page width & height

    @property
    def xml(self) -> BeautifulSoup:
        """Read Mets XML file of the issue and create a BeautifulSoup object.

        Returns:
            BeautifulSoup: BeautifulSoup object with Mets XML of the issue.
        """
        mets_regex = os.path.join(self.path, "toc", f"*{self.issue_uid}.xml")
        mets_file = glob(mets_regex)
        if len(mets_file) == 0:
            logger.critical("Could not find METS file in %s", self.path)
            return None
        mets_file = mets_file[0]
        with open(mets_file, "r", encoding="utf-8") as f:
            raw_xml = f.read()

        mets_doc = BeautifulSoup(raw_xml, "xml")
        return mets_doc

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`BnfCanonicalPage` instances are added to :attr:`pages`.

        Raises:
            e: Instantiation of a page or adding it to :attr:`pages` failed.
        """
        ocr_path = os.path.join(self.path, "ocr")  # Pages in `ocr` folder

        pages = [
            (file, int(file.split(".")[0][1:]))
            for file in os.listdir(ocr_path)
            if not file.startswith(".") and ".xml" in file
        ]

        page_filenames, page_numbers = zip(*pages)

        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers
        ]

        self.pages = {}
        for filename, page_no, page_id in zip(page_filenames, page_numbers, page_canonical_names):
            try:
                self.pages[page_no] = BnfNewspaperPage(page_id, page_no, filename, ocr_path)
            except Exception as e:
                logger.error(
                    "Adding page %s %s %s raised following exception: %s",
                    page_no,
                    page_id,
                    filename,
                    e,
                )
                raise e

    def _get_divs_by_type(self, mets: BeautifulSoup) -> dict[str, list[tuple[str, str]]]:
        """Parse `div` tags, flatten them and sort them by type.

        First, parse the `dmdSec` tags, and sort them by type.
        Then, search for `div` tags in the `content` of the `structMap` that
        don't have the `DMDID` attribute, and for which the type is in
        `BNF_CONTENT_TYPES`.
        Finally, flatten the sections into what they actually contain, and add
        the flattened sections to the return dict.

        Args:
            mets (BeautifulSoup): Contents of the Mets XML file.

        Returns:
            dict[str, list[tuple[str, str]]]: All the `div` sorted by type, the
                values are the List of (div_id, div_label)
        """
        dmd_sections = [x for x in mets.findAll("dmdSec") if x.find("mods")]
        struct_map = mets.find("structMap", {"TYPE": "logical"})
        struct_content = struct_map.find("div", {"TYPE": "CONTENT"})

        by_type = {}

        # First parse DMD section and keep DIV IDs of referenced items
        for s in dmd_sections:  # Iterate on the DMD section
            divs = struct_map.findAll("div", {"DMDID": s.get("ID")})

            if len(divs) > 1:  # Means this DMDID is a class of objects
                if s.find("mods:classification") is not None:
                    _type = s.find("mods:classification").getText().lower()
                    for d in divs:
                        by_type = add_div(by_type, _type, d.get("ID"), d.get("LABEL"))
                else:
                    logger.warning("MultiDiv with no classification for %s", self.id)
            else:
                div = divs[0]
                _type = div.get("TYPE").lower()
                by_type = add_div(by_type, _type, div.get("ID"), div.get("LABEL"))

        # Parse div sections that are direct children of CONTENT in the
        # logical structMap, and keep the ones without DMDID
        for c in struct_content.findChildren("div", recursive=False):
            if c.get("DMDID") is None and c.get("TYPE") is not None:
                _type = c.get("TYPE").lower()
                by_type = add_div(by_type, _type, c.get("ID"), c.get("LABEL"))

        if "section" in by_type:
            by_type = self._flatten_sections(by_type, struct_content)

        return by_type

    def _flatten_sections(self, by_type: dict, struct_content) -> dict[str, list[tuple[str, str]]]:
        """Flatten the sections of the issue.

        This means making the children parts standalone CIs.

        Args:
            by_type (dict): Parsed `div` tags separated by type
            struct_content (_type_): _description_

        Returns:
            dict[str, list[tuple[str, str]]]: _description_
        """
        # Flatten the sections
        for div_id, lab in by_type["section"]:
            # Get all divs of this section
            div = struct_content.find("div", {"ID": div_id})
            for d in div.findChildren("div", recursive=False):
                dmdid = d.get("DMDID")
                div_id = d.get("ID")
                ci_type = d.get("TYPE").lower()
                d_label = d.get("LABEL")
                # This div needs to be added to the content items
                if dmdid is None and ci_type in BNF_CONTENT_TYPES:
                    by_type = add_div(by_type, ci_type, div_id, d_label or lab)
                elif dmdid is None:
                    logging.debug(
                        " %s: %s of type %s within section is not in CONTENT_TYPES",
                        self.id,
                        div_id,
                        ci_type,
                    )
        del by_type["section"]
        return by_type

    def _parse_div(
        self,
        div_id: str,
        div_type: str,
        label: str,
        item_counter: int,
        mets_doc: BeautifulSoup,
    ) -> tuple[list[dict], int]:
        """Parse the given `div_id` from the `structMap` of the METS file.

        Args:
            div_id (str): Unique ID of the div to parse
            div_type (str): Type of the div (should be in `BNF_CONTENT_TYPES`)
            label (str): Label of the div (title)
            item_counter (int): The current counter for CI IDs
            mets_doc (BeautifulSoup): Contents of the Mets XML file.

        Returns:
            tuple[list[dict], int]: _description_
        """
        article_div = mets_doc.find("div", {"ID": div_id})  # Get the tag
        # Try to get the body if there is one (we discard headings)
        article_div = article_div.find("div", {"TYPE": "BODY"}) or article_div
        parts = parse_div_parts(article_div)  # Parse the parts of the tag
        metadata, ci = None, None
        # If parts were found, create content item for this DIV
        if len(parts) > 0:
            article_id = "{}-i{}".format(self.id, str(item_counter).zfill(4))
            metadata = {
                "id": article_id,
                "tp": type_translation[div_type],
                "pp": [],
            }
            if label is not None:
                metadata["t"] = label
            ci = {"m": metadata, "l": {"parts": parts}}
            item_counter += 1
        else:  # Otherwise, only parse embedded CIs
            article_id = None

        embedded, item_counter = parse_embedded_cis(
            article_div, label, self.id, article_id, item_counter
        )

        if metadata is not None:
            embedded.append(ci)

        return embedded, item_counter

    def _get_image_iiif_link(self, ci_id: str, parts: list) -> tuple[list[int], str]:
        """Get the image coordinates and iiif info uri given the ID of the CI.
        Args:
            ci_id (str): The ID of the image CI
            parts (list): Parts of the image
        Returns:
            tuple[list[int], str]: The image coordinated and iiif uri to the
                info.json for the page's image.
        """
        image_part = [p for p in parts if p["comp_role"] == CONTENTITEM_TYPE_IMAGE]
        iiif_link, coords = None, None
        if len(image_part) == 0:
            message = (
                f"Content item {ci_id} of type "
                f"{CONTENTITEM_TYPE_IMAGE} does not have image part."
            )
            logger.warning(message)
        elif len(image_part) > 1:
            message = (
                f"Content item {ci_id} of type "
                f"{CONTENTITEM_TYPE_IMAGE} has multiple image parts."
            )
            logger.warning(message)
        else:

            image_part_id = image_part[0]["comp_id"]
            page = self.pages[image_part[0]["comp_page_no"]]
            block = page.xml.find("Illustration", {"ID": image_part_id})
            if block is None:
                logger.warning("Could not find image %s for CI %s", image_part_id, ci_id)
            else:
                coords = distill_coordinates(block)
                iiif_link = os.path.join(IIIF_ENDPOINT_URI, page.ark_link, IIIF_SUFFIX)

        return coords, iiif_link

    def _parse_mets(self) -> None:
        """Parse the Mets XML file corresponding to this issue.

        Once the :attr:`issue_data` is created, containing all the relevant
        information in the canonical Issue format, the `BnfNewspaperIssue`
        instance is ready for serialization.
        """

        mets_doc = self.xml
        # First get all the divs by type
        by_type = self._get_divs_by_type(mets_doc)
        item_counter = 1
        content_items = []

        # Then start parsing them
        for div_type, divs in by_type.items():
            for div_id, div_label in divs:
                cis, item_counter = self._parse_div(
                    div_id, div_type, div_label, item_counter, mets_doc
                )
                content_items += cis

        # Finally add the pages and iiif link
        for x in content_items:
            x["m"]["pp"] = list(set(c["comp_page_no"] for c in x["l"]["parts"]))
            if x["m"]["tp"] == CONTENTITEM_TYPE_IMAGE:
                x["c"], x["m"]["iiif_link"] = self._get_image_iiif_link(
                    x["m"]["id"], x["l"]["parts"]
                )

        # once the pages are added to the metadata, compute & add the reading order
        reading_order_dict = get_reading_order(content_items)
        for item in content_items:
            item["m"]["ro"] = reading_order_dict[item["m"]["id"]]

        self.pages = list(self.pages.values())

        # Issue manifest iiif URI is in format {iiif_prefix}/{ark_id}/manifest.json
        # By default, the ark id contains the page number,
        iiif_manifest = os.path.join(
            IIIF_ENDPOINT_URI,
            os.path.dirname(self.pages[0].ark_link),
            IIIF_MANIFEST_SUFFIX,
        )

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
        # Note for newspapers with two dates (197 cases)
        if self.secondary_date is not None:
            self.issue_data["n"] = [f"Secondary date {self.secondary_date}"]
