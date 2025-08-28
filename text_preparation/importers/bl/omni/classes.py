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

from bs4.element import Tag, NavigableString
from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers import (
    CONTENTITEM_TYPES,
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_ADVERTISEMENT,
)
from text_preparation.importers.mets_alto import MetsAltoCanonicalIssue, MetsAltoCanonicalPage, alto
from text_preparation.importers.bl.detect import BlIssueDir
from text_preparation.utils import get_reading_order, coords_to_xywh

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = "info.json"
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
        page_file_names = sorted(
            [
                file
                for file in os.listdir(self.path)
                if (not file.startswith(".") and file.endswith(".xml") and "mets" not in file)
            ],
            key=lambda f: int(os.path.splitext(f)[0].split("_")[-1]),
        )

        page_numbers = [int(os.path.splitext(fname)[0].split("_")[-1]) for fname in page_file_names]
        page_canonical_names = [f"{self.id}-p{str(page_n).zfill(4)}" for page_n in page_numbers]

        # look for the renaming info file to get the images width and height
        with open(os.path.join(self.path, RENAMING_INFO_FILE), "r", encoding="utf-8") as fin:
            renaming_info = json.load(fin)

        self.pages = []
        self.page_filenames = {}
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            # print(f"Adding page {page_no} {page_id} {filename}")
            try:
                page_width = renaming_info[str(page_no)]["width"]
                page_height = renaming_info[str(page_no)]["height"]
                self.pages.append(
                    BlOmniNewspaperPage(
                        page_id, page_no, filename, self.path, (page_width, page_height)
                    )
                )
                self.page_filenames[page_no] = renaming_info[str(page_no)]["original_filename"]
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
        # This is where illustrations and captions will be identified
        comp_label = div.get("LABEL").lower()
        if comp_role is None:
            type_attr = div.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None

        return {
            "comp_role": comp_role,
            "comp_id": comp_id,
            "comp_label": comp_label,
            "comp_fileid": comp_fileid,
            "comp_page_no": int(comp_page_no),
        }

    def _get_image_and_captions(
        self, div, part_id, div_parts, curr_ci_parts, ci_image_parts, last_img_part_id
    ):
        # for each illustration, store its coordinates and any potential caption
        if div.get("LABEL").lower() == BL_IMG_TYPE:
            img_xy_coords = div.find("area", {"SHAPE": "RECT"}).get("COORDS")
            # directly convert the coordinates to the wanted xywh format
            div_parts["coords"] = coords_to_xywh([int(c) for c in img_xy_coords.split(",")])
            if part_id not in ci_image_parts:
                ci_image_parts[part_id] = [div_parts]
            else:
                ci_image_parts[part_id].append(div_parts)
            # keep track of which illustration it is to make sure we can connect them back after
            last_img_part_id = part_id

        # if the next element is a caption, attach it directly
        if div.get("LABEL").lower() == BL_CAPTION_TYPE:
            if curr_ci_parts[-1]["comp_id"] == last_img_part_id:
                cap_xy_coords = div.find("area", {"SHAPE": "RECT"}).get("COORDS")
                # directly convert the coordinates to the wanted xywh format
                div_parts["coords"] = coords_to_xywh([int(c) for c in cap_xy_coords.split(",")])
                # add the div parts of the caption to the last image - normally the corresponding one
                ci_image_parts[last_img_part_id].append(div_parts)
            else:
                msg = (
                    f"{self.id}, {div_parts['comp_page_no']} - caption {div.get('ID')} "
                    "does not follow an illustration!"
                )
                print(msg)
                self._notes.append(msg)

        return ci_image_parts, last_img_part_id

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

        ci_parts = []
        ci_image_parts = {}
        last_img_part_id = None
        for idx, p in enumerate(parts_ids):
            # Get element in physical map
            div = phys_map.find("div", {"ID": p})
            type_attr = div.get("TYPE")
            comp_role = type_attr.lower() if type_attr else None

            if comp_role == "page":
                # when the div is a page, need to add all parts
                for sub_div in div.findAll("div"):
                    subdiv_part_dict = self._get_part_dict(sub_div, None)
                    subdiv_part_id = sub_div.get("ID")

                    # verify if the div/sub_div is an image or its caption to keep track of them
                    ci_image_parts, last_img_part_id = self._get_image_and_captions(
                        sub_div,
                        subdiv_part_id,
                        subdiv_part_dict,
                        ci_parts,
                        ci_image_parts,
                        last_img_part_id,
                    )
                    ci_parts.append(subdiv_part_dict)

            else:
                div_part_dict = self._get_part_dict(div, comp_role)
                # verify if the div/sub_div is an image or its caption to keep track of them
                ci_image_parts, last_img_part_id = self._get_image_and_captions(
                    div, p, div_part_dict, ci_parts, ci_image_parts, last_img_part_id
                )
                ci_parts.append(div_part_dict)

        return ci_parts, ci_image_parts

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
            msg = f"{self.id}-i{str(counter).zfill(4)} - Warning! The CI div type is image and not handled as such! item_div ID={item_div.get('ID')}"
            print(msg)
            self._notes.append(msg)
        elif div_type == BL_AD_TYPE:
            div_type = CONTENTITEM_TYPE_ADVERTISEMENT

        # Check if new content item is found (or if we need more translation)
        if div_type not in CONTENTITEM_TYPES:
            logger.warning("Found new content item type: %s", div_type)

        metadata = {
            "id": f"{self.id}-i{str(counter).zfill(4)}",
            "tp": div_type,
            "pp": [],
            "var_t": self.var_title,
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
                "src_files": {
                    "mets_xml": os.path.basename(self.mets_file),
                    "alto_xml": [],
                    "page_image": [],
                },
                "id": item_div.get("ID"),
                "parts": ci_parts,
            },
        }
        for p in content_item["l"]["parts"]:
            pge_no = p["comp_page_no"]
            if pge_no not in content_item["m"]["pp"]:
                content_item["m"]["pp"].append(pge_no)
                content_item["l"]["src_files"]["alto_xml"].append(
                    os.path.basename(self.mets_file).replace("mets", str(pge_no).zfill(4))
                )
                content_item["l"]["src_files"]["page_image"].append(self.page_filenames[pge_no])

        return content_item, image_parts

    def _parse_image_cis_in_div(self, image_parts, corresp_ci, counter) -> list[dict[str, Any]]:
        img_cis = []

        # first go through each page to find illustrations not associated to existing CIs.
        # TODO some illustrations not attached to elements are lost!!
        for img_comp_id, parts in image_parts.items():
            if parts[0]["comp_label"] == BL_IMG_TYPE and parts[0]["comp_id"] == img_comp_id:
                # ensure that the element is indeed an illustration
                pg_nums = list(set(p["comp_page_no"] for p in parts))
                assert (
                    len(pg_nums) == 1
                ), f"{corresp_ci['m']['id']}, image with part_id {img_comp_id}, is on more than one page"
                content_item = {
                    "m": {
                        "id": f"{self.id}-i{str(counter).zfill(4)}",
                        "tp": CONTENTITEM_TYPE_IMAGE,
                        "pp": [pg_nums[0]],
                        "iiif_link": os.path.join(
                            IIIF_ENDPOINT_URI, f"{self.id}-p{str(pg_nums[0]).zfill(4)}", IIIF_SUFFIX
                        ),
                        "var_t": self.var_title,
                    },
                    "l": {
                        "bl_nlp": self.nlp,
                        "src_files": {
                            "mets_xml": os.path.basename(self.mets_file),
                            "alto_xml": [
                                os.path.basename(self.mets_file).replace(
                                    "mets", str(pg_nums[0]).zfill(4)
                                )
                            ],
                            "page_image": [self.page_filenames[pg_nums[0]]],
                        },
                        "id": img_comp_id,
                        "parts": parts,
                    },
                    "c": parts[0]["coords"],
                    # ensure to keep track of the CI this image is attached to
                    "pOf": corresp_ci["m"]["id"],
                }

                img_cis.append(content_item)
                counter += 1

        return img_cis, counter

    def find_unlinked_image_cis(self, structlink: Tag, ci_counter: int) -> list[dict[str, Any]]:
        # extract the list of all regions/blocks listed in the mets file
        all_linked_regions = [
            e.get("xlink:href").lstrip("#") for e in structlink.find_all("smLocatorLink")
        ]
        image_cis = []

        for page in self.pages:

            pg_xml = page.xml
            pt_space = pg_xml.find("PrintSpace")

            for block in pt_space.children:
                if isinstance(block, NavigableString):
                    continue

                # if the block is an illustration which was not attached to an existing CI, create a CI for it.
                if (
                    block.get("TYPE")
                    and block.get("TYPE").lower() in [BL_IMG_TYPE, "image"]
                    and block.get("ID") not in all_linked_regions
                ):
                    coords = alto.distill_coordinates(block)

                    content_item = {
                        "m": {
                            "id": f"{self.id}-i{str(ci_counter).zfill(4)}",
                            "tp": CONTENTITEM_TYPE_IMAGE,
                            "pp": [page.number],
                            "iiif_link": os.path.join(
                                IIIF_ENDPOINT_URI,
                                f"{self.id}-p{str(page.number).zfill(4)}",
                                IIIF_SUFFIX,
                            ),
                            "var_t": self.var_title,
                        },
                        "l": {
                            "bl_nlp": self.nlp,
                            "src_files": {
                                "mets_xml": os.path.basename(self.mets_file),
                                "alto_xml": [
                                    os.path.basename(self.mets_file).replace(
                                        "mets", str(page.number).zfill(4)
                                    )
                                ],
                                "page_image": [self.page_filenames[page.number]],
                            },
                            "id": block.get("ID"),
                            "parts": [
                                {
                                    "comp_id": block.get("ID"),
                                    "comp_label": block.get("TYPE").lower(),
                                    "comp_fileid": f"img{str(page.number).zfill(3)}-alto",
                                    "comp_page_no": page.number,
                                }
                            ],
                        },
                        "c": coords,
                    }
                    msg = (
                        f"page {page.number} -> found an unlinked illustration: {block.get('ID')}, "
                        f"coords = {coords}, adding the CI: {self.id}-i{str(ci_counter).zfill(4)}"
                    )
                    print(msg)
                    self._notes.append(msg)

                    image_cis.append(content_item)
                    ci_counter += 1

        return image_cis

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

        # Sort to have same naming TODO remove because ordering is already fixed
        # sorted_divs = sorted(divs, key=lambda x: x.get("DMDID").lower())

        # Get all CI types
        found_types = set(x.get("TYPE") for x in divs)

        phys_structmap = mets_doc.find("structMap", {"TYPE": "PHYSICAL"})
        structlink = mets_doc.find("structLink")

        counter = 1
        # page_num_of_last_ci = None
        # for page in self.pages:
        for div in divs:
            # Parse Each contentitem
            dmd_sec = mets_doc.find("dmdSec", {"ID": div.get("DMDID")})
            parsed_ci, image_parts = self._parse_content_item(
                div, counter, phys_structmap, structlink, dmd_sec
            )

            content_items.append(parsed_ci)
            counter += 1

            if len(image_parts) > 0:
                # process any image parts found in CI and add as other CIs, increase counter accordingly
                image_cis, counter = self._parse_image_cis_in_div(image_parts, parsed_ci, counter)
                content_items.extend(image_cis)

        # Now recognize all the images present in the pages' alto files,
        # not associated to any article
        unlinked_img_cis = self.find_unlinked_image_cis(structlink, counter)
        content_items.extend(unlinked_img_cis)

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
