"""This module contains the definition of BL importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the BL version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import logging
import os
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
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"

BL_PICTURE_TYPE = "picture"
BL_AD_TYPE = "advert"


class BlNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in BL (Mets/Alto) format.

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

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        """Add the given `BlNewspaperIssue` as an attribute for this class.

        Args:
            issue (MetsAltoCanonicalIssue): Issue this page is from
        """
        self.issue = issue
        self.page_data["iiif_img_base_uri"] = os.path.join(IIIF_ENDPOINT_URI, self.id)


class BlNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in BL (Mets/Alto) format.

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

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created `BlNewspaperPage` instances are added to the `pages` attribute.

        Raises:
            e: Creating a `BlNewspaperPage` raised an exception.
        """
        page_file_names = [
            file
            for file in os.listdir(self.path)
            if (not file.startswith(".") and ".xml" in file and "mets" not in file)
        ]
        page_numbers = [int(os.path.splitext(fname)[0].split("_")[-1]) for fname in page_file_names]

        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers
        ]

        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                self.pages.append(BlNewspaperPage(page_id, page_no, filename, self.path))
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

    def _parse_content_parts(
        self, item_div: Tag, phys_map: Tag, structlink: Tag
    ) -> list[dict[str, Any]]:
        """Parse parts of issue's physical structure relating to the given item.

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
        for p in parts_ids:
            # Get element in physical map
            div = phys_map.find("div", {"ID": p})
            type_attr = div.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None

            # In that case, need to add all parts
            if comp_role == "page":
                for x in div.findAll("div"):
                    parts.append(self._get_part_dict(x, None))
            else:
                parts.append(self._get_part_dict(div, comp_role))

        return parts

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

        if div_type == BL_PICTURE_TYPE:
            div_type = CONTENTITEM_TYPE_IMAGE
        elif div_type == BL_AD_TYPE:
            div_type = CONTENTITEM_TYPE_ADVERTISEMENT

        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning("Found new content item type: %s", div_type)

        metadata = {
            "id": "{}-i{}".format(self.id, str(counter).zfill(4)),
            "tp": div_type,
            "pp": [],
        }
        # Get content item's language
        lang = item_dmd_sec.findChild("languageTerm")
        if lang is not None:
            metadata["lg"] = lang.text

        # Load physical struct map, and find all parts in physical map
        content_item = {
            "m": metadata,
            "l": {
                "id": item_div.get("ID"),
                "parts": self._parse_content_parts(item_div, phys_structmap, structlink),
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
            counter += 1

        # compute the reading order for the issue's items
        reading_order_dict = get_reading_order(content_items)

        for ci in content_items:
            # add the reading order
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        return content_items

    def _parse_mets(self) -> None:

        # No image properties in BL data

        # Parse all the content items
        content_items = self._parse_content_items()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "i": content_items,
            "pp": [p.id for p in self.pages],
        }
