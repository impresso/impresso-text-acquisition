"""This module contains the definition of BL importer classes for the OmniPage format.

The classes define newspaper Issues and Pages objects which convert OCR data in
the BL version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
import json
from time import strftime
from typing import Any

from bs4.element import Tag
from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers import (
    CONTENTITEM_TYPES,
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_ADVERTISEMENT,
)
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.importers.bl.detect import BlIssueDir
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
BL_TITLES_FILE = "BL_all_titles.json"
BL_ISSUES_FILE = "BL_OmniPage-NLP_issues.json"
RENAMING_INFO_FILE = "renaming_info.json"
BL_IMG_TYPE = "illustration"
BL_AD_TYPE = "advert"
BL_CAPTION_TYPE = "caption"


class BlOmniNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in BL (Mets/Alto) OmniPage-NLP format.

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
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.
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

        # add the facsimile height and width to the page data
        self.page_data["fw"] = page_size[0]
        self.page_data["fh"] = page_size[1]

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        """Add the given `BlNewspaperIssue` as an attribute for this class.

        Args:
            issue (MetsAltoCanonicalIssue): Issue this page is from
        """
        self.issue = issue
        self.page_data["iiif_img_base_uri"] = os.path.join(IIIF_ENDPOINT_URI, self.id)


class BlOmniNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in BL (Mets/Alto) OmniPage-NLP format.

    All functions defined in this child class are specific to parsing BL
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

    def __init__(self, issue_dir: BlIssueDir) -> None:

        # assign the NLP to the issue
        self.nlp = issue_dir.nlp
        # extract the BL base_dir from the issue_dir path
        # the path is "[BL base_dir]/[alias]/[nlp]/[yyyy]/[mm]/[dd]
        self.bl_base_dir = issue_dir.path.split(issue_dir.alias)[0].rstrip("/")
        # initialize attributes to prevent errors
        self.var_title, self.bl_work_title, self.norm_title = None, None, None

        super().__init__(issue_dir)

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created `BlNewspaperPage` instances are added to the `pages` attribute.

        Raises:
            e: Creating a `BlNewspaperPage` raised an exception.
        """
        page_file_names = [
            file
            for file in os.listdir(self.path)
            if (not file.startswith(".") and file.endswith(".xml") and "mets" not in file)
        ]
        page_numbers = sorted(
            int(os.path.splitext(fname)[0].split("_")[-1]) for fname in page_file_names
        )

        page_canonical_names = [f"{self.id}-p{str(page_n).zfill(4)}" for page_n in page_numbers]

        # look for the renaming info file to get the images width and height
        with open(os.path.join(self.path, RENAMING_INFO_FILE), "r", encoding="utf-8") as fin:
            renaming_info = json.load(fin)

        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                page_width = renaming_info[str(page_no)]["width"]
                page_height = renaming_info[str(page_no)]["height"]
                self.pages.append(
                    BlOmniNewspaperPage(
                        page_id, page_no, filename, self.path, (page_width, page_height)
                    )
                )
            except Exception as e:
                msg = (
                    f"Adding page {page_no} {page_id} {filename}",
                    f"raised following exception: {e}",
                )
                logger.error(msg)
                raise e

    def _get_part_dict(self, div: Tag, comp_role: str | None) -> dict[str, Any]:
        """Construct the parts for a certain div entry of METS.

        Args:
            div (Tag): Content item div
            comp_role (str | None): Role of the component

        Returns:
            dict[str, Any]: Parts dict for given div.
        """
        comp_fileid = div.find("area", {"BETYPE": "IDREF"}).get("FILEID")
        comp_id = div.get("ID")
        comp_page_no = int(div.parent.get("ORDER"))
        if comp_role is None:
            type_attr = div.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None

        return {
            "comp_role": comp_role,
            "comp_id": comp_id,
            "comp_fileid": comp_fileid,
            "comp_page_no": int(comp_page_no),
        }

    def _parse_content_parts_and_images(
        self, item_div: Tag, phys_map: Tag, structlink: Tag
    ) -> list[dict[str, Any]]:
        """Parse parts of issue's physical structure relating to the given item.

        Also identify any illustrations that might be present in these parts,
        along with their coordinates and potential captions.
        Identifying them then allows to ensure that the image is linked to its article.

        Args:
            item_div (Tag): The div corresponding to the item
            phys_map (Tag): The physical structure of the Issue
            structlink (Tag): The structlink element of Mets file.

        Returns:
            list[dict[str, Any]]: List of dicts of each content part of item.
        """
        # Find all parts and their IDS
        tag = f"#{item_div.get('ID')}"
        linkgrp = structlink.find("smLocatorLink", {"xlink:href": tag}).parent

        # Remove `#` from xlink:href
        parts_ids = [
            x.get("xlink:href")[1:]
            for x in linkgrp.findAll("smLocatorLink")
            if x.get("xlink:href") != tag
        ]

        parts = []
        image_parts = {}
        last_img_part_id = None
        last_img_part_idx = None
        for idx, p in enumerate(parts_ids):
            # Get element in physical map
            div = phys_map.find("div", {"ID": p})
            type_attr = div.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None

            # In that case, need to add all parts
            if comp_role == "page":
                for x in div.findAll("div"):
                    div_parts = self._get_part_dict(x, None)
            else:
                div_parts = self._get_part_dict(div, comp_role)

            # for each illustration, store its coordinates and any potential caption
            if div.get("LABEL").lower() == BL_IMG_TYPE:
                image_parts[p] = {
                    "legacy_parts": div_parts,
                    "coords": div.find("area", {"SHAPE": "RECT"}).get("COORDS"),
                }
                # keep track of which illustration it is to make sure we can connect them back after
                last_img_part_id = p
                last_img_part_idx = idx

            # if the next element is a caption, attach it directly
            if div.get("LABEL").lower() == BL_CAPTION_TYPE:
                if idx - 1 == last_img_part_idx:
                    image_parts[last_img_part_id]["caption_parts"] = div_parts
                else:
                    msg = f"self.id, {div_parts['comp_page_no']} - caption {div.get('ID')} does not follow an illustration!"
                    print(msg)
                    self._notes.append(msg)

            parts.append(div_parts)

        return parts, image_parts

    def _parse_content_item(
        self,
        item_div: Tag,
        counter: int,
        phys_structmap: Tag,
        structlink: Tag,
        item_dmd_sec: Tag,
    ) -> dict[str, Any]:
        """Parse the given content item.

        Doing this parsing means searching for all parts and
        constructing unique IDs for each item.

        Args:
            item_div (Tag): The div of the content item.
            counter (int): The counter to get unique ordered IDs.
            phys_structmap (Tag): The physical structmap element of Mets file.
            structlink (Tag): The structlink element of Mets file.
            item_dmd_sec (Tag): Dmd section of Mets file of this specific item.

        Returns:
            dict[str, Any]: Canonical representation of the content item.
        """
        div_type = item_div.get("TYPE").lower()

        # TODO --> when there are images, it not at this level that's it's given!
        if div_type == BL_IMG_TYPE:
            div_type = CONTENTITEM_TYPE_IMAGE
        elif div_type == BL_AD_TYPE:
            div_type = CONTENTITEM_TYPE_ADVERTISEMENT

        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning("Found new content item type: %s", div_type)

        metadata = {
            "id": f"{self.id}-i{str(counter).zfill(4)}",
            "tp": div_type,
            "pp": [],
        }
        # Get content item's language
        lang = item_dmd_sec.findChild("languageTerm")
        if lang is not None:
            metadata["lg"] = lang.text

        ci_parts, image_parts = self._parse_content_parts_and_images(
            item_div, phys_structmap, structlink
        )

        # Load physical struct map, and find all parts in physical map
        content_item = {
            "m": metadata,
            "l": {
                "bl_nlp": self.nlp,
                "id": item_div.get("ID"),
                "parts": ci_parts,
            },
        }
        for p in content_item["l"]["parts"]:
            pge_no = p["comp_page_no"]
            if pge_no not in content_item["m"]["pp"]:
                content_item["m"]["pp"].append(pge_no)

        # TODO: add coordinates for images as well as iiif_link
        # + update approach for handling images
        return content_item

    def _parse_content_items(self) -> list[dict[str, Any]]:
        """Extract content item elements from a Mets XML file.

        Returns:
            list[dict[str, Any]]: List of all content items and the relevant
                information in canonical format for each one.
        """
        mets_doc = self.xml
        content_items = []
        # Get logical structure of issue
        divs = (
            mets_doc.find("structMap", {"TYPE": "LOGICAL"})
            .find("div", {"TYPE": "ISSUE"})
            .findChildren("div")
        )

        # Sort to have same naming
        sorted_divs = sorted(divs, key=lambda x: x.get("DMDID").lower())

        # Get all CI types
        found_types = set(x.get("TYPE") for x in sorted_divs)

        phys_structmap = mets_doc.find("structMap", {"TYPE": "PHYSICAL"})
        structlink = mets_doc.find("structLink")

        counter = 1
        for div in sorted_divs:
            # Parse Each contentitem
            dmd_sec = mets_doc.find("dmdSec", {"ID": div.get("DMDID")})
            content_items.append(
                self._parse_content_item(div, counter, phys_structmap, structlink, dmd_sec)
            )
            # TODO here process the image parts found in CI and add as other CIs
            counter += 1

        # compute the reading order for the issue's items
        reading_order_dict = get_reading_order(content_items)

        for ci in content_items:
            # add the reading order
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        return content_items

    def _find_variant_title(self) -> None:

        with open(os.path.join(self.bl_base_dir, BL_TITLES_FILE), "r", encoding="utf-8") as fin:
            titles = json.load(fin)

        titles_for_alias_nlp = titles["-".join([self.alias, self.nlp])]

        for str_period, title_dict in titles_for_alias_nlp.items():
            period = [int(y) for y in str_period.split("-")]
            # ensure that this issue is indeed in the period listed for the given title
            if self.date.year in range(period[0], period[1] + 1):
                self.var_title = title_dict["Variant Title"]
                self.bl_work_title = title_dict["Working title (BL)"]
                # not used at the moment
                self.norm_title = title_dict["Normalized Working Title"]
            else:
                msg = f"{self.id} ({self.nlp}) - Issue year doesn't match the period {period} for the variant title!"
                print(msg)
                logger.warning(msg)

    def _parse_mets(self) -> None:

        self._find_variant_title()

        # TODO add the images (illsutrations) No image properties in BL data

        # Parse all the content items
        content_items = self._parse_content_items()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": True,
            "i": content_items,
            "pp": [p.id for p in sorted(self.pages, key=lambda x: x.number)],
            "n": self._notes,
        }
