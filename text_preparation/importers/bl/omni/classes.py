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
from text_preparation.tokenization import insert_whitespace

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = "info.json"
BL_TITLES_FILE = "BL_all_titles.json"
BL_ISSUES_FILE = "BL_OmniPage-NLP_issues.json"
RENAMING_INFO_FILE = "renaming_info.json"
BL_IMG_TYPE = "illustration"
BL_AD_TYPE = "advert"
BL_CAPTION_TYPE = "caption"
BL_TITLE_TYPE = "headline"


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
        self.page_xmls = {}

        super().__init__(issue_dir)

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created `BlNewspaperPage` instances are added to the `pages` attribute.

        Raises:
            e: Creating a `BlNewspaperPage` raised an exception.
        """
        # look for the renaming info file to get the precomputed page xml filenames
        # as well as images width and height
        with open(os.path.join(self.path, RENAMING_INFO_FILE), "r", encoding="utf-8") as fin:
            renaming_info = json.load(fin)

        all_xml_files = renaming_info["ocr_formats"]["OmniPage-NLP"]
        self.mets_file = os.path.join(
            self.path, [file for file in all_xml_files if "mets" in file][0]
        )

        page_file_names = sorted(
            [file for file in all_xml_files if "mets" not in file],
            key=lambda f: int(os.path.splitext(f)[0].split("_")[-1]),
        )
        # The exact list of pages in the right format are already present in the renaming info file.
        """page_file_names = sorted(
            [
                file
                for file in os.listdir(self.path)
                if (
                    self.nlp
                    in file  # omnipage format files include the nlp eg: 0000268_18100108_0004.xml  0000268_18100108_mets.xml
                    and not file.startswith(".")
                    and file.endswith(".xml")
                    and "mets" not in file
                )
            ],
            key=lambda f: int(os.path.splitext(f)[0].split("_")[-1]),
        )

        if any(x not in page_file_names for x in pre_comp_page_file_names):
            msg = f"{self.id}: Warning, pre-computed page file names don't match the ones found on the fly! path = {self.path}"
        """

        page_numbers = [int(os.path.splitext(fname)[0].split("_")[-1]) for fname in page_file_names]
        page_canonical_names = [f"{self.id}-p{str(page_n).zfill(4)}" for page_n in page_numbers]

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
        self,
        div: Tag,
        part_id: str,
        div_parts: dict[str, Any],
        curr_ci_parts: list[dict],
        ci_image_parts: dict[str, Any],
        last_img_part_id: str,
    ) -> tuple[dict[str, Any], str]:
        """Extract image or caption information from a div and update ci_image_parts.

        Args:
            div (Tag): The BeautifulSoup tag representing the current div element.
            part_id (str): The component ID of the current div.
            div_parts (dict[str, Any]): The extracted attributes of the current div.
            curr_ci_parts (list[dict]): All parts belonging to the current content item.
            ci_image_parts (dict[str, Any]): Mapping of image part_id → [image div, caption divs].
            last_img_part_id (str): ID of the most recently processed image part.

        Returns:
            tuple[dict[str, Any], str]:
                - Updated ci_image_parts dictionary.
                - The most recent image part_id (last_img_part_id).

        Notes:
            - If the div is an image (LABEL == BL_IMG_TYPE), stores coordinates
            and initializes an entry in ci_image_parts.
            - If the div is a caption (LABEL == BL_CAPTION_TYPE), attaches it
            to the last seen image if consistent.
            - Captions not directly following an image trigger a warning note.
        """
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
            if curr_ci_parts and curr_ci_parts[-1]["comp_id"] == last_img_part_id:
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

        # Get content item's title and language
        title = item_dmd_sec.findChild("title")
        if title is not None:
            metadata["t"] = title.text
        lang = item_dmd_sec.findChild("languageTerm")
        if lang is not None:
            metadata["lg"] = lang.text

        ci_parts, image_parts = self._parse_content_parts_and_images(
            item_div, phys_structmap, structlink
        )

        # Load physical struct map, and find all parts in physical map
        content_item = {
            "m": metadata,
            # full legacy information for potential return cards
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

    def _parse_img_caption(
        self, img_parts: list[dict], page_num: int, page_pt_space: Tag, lang: str | None = None
    ) -> str | None:
        """Extract the text of a caption associated with an image.

        Args:
            img_parts (list[dict]): The list of image parts (including captions).
            page_num (int): The page number where the image appears.
            page_pt_space (Tag): BeautifulSoup tag representing the page text space.
            lang (str | None, optional): Language code used for whitespace insertion rules.

        Returns:
            str | None: The extracted caption text, or None if no caption is found.

        Notes:
            - If multiple caption parts exist, only the first is parsed.
            - Issues are logged and added to ``self._notes``.
        """
        # Find caption parts
        caption_part = [part for part in img_parts if part["comp_label"] == BL_CAPTION_TYPE]
        if len(caption_part) == 0:
            # No caption for this image
            return None

        if len(caption_part) > 1:
            msg = f"{self.id} - page {page_num} - Warning! Mulitple caption parts!!"
            print(msg)
            self._notes.append(msg)

        caption_part = caption_part[0]

        # Locate the caption block
        block = page_pt_space.find("TextBlock", {"ID": caption_part["comp_id"]})
        if block is None:
            msg = (
                f"{self.id} - page {page_num} - "
                f"Missing TextBlock for caption {caption_part.get('comp_id')}"
            )
            logger.warning(msg)
            self._notes.append(msg)
            return None

        cap_text = []
        for line in block.find_all("TextLine"):
            all_strings = line.find_all("String")
            for i, s in enumerate(all_strings):
                token = s.get("CONTENT")
                if len(all_strings) == 1 or i == len(all_strings) - 1:
                    insert_ws = False
                elif i == 0 and i != len(all_strings) - 1:
                    insert_ws = insert_whitespace(
                        token, all_strings[i + 1].get("CONTENT"), None, lang
                    )
                else:
                    insert_ws = insert_whitespace(
                        token,
                        all_strings[i + 1].get("CONTENT"),
                        all_strings[i - 1].get("CONTENT"),
                        lang,
                    )

                cap_text.append(f"{token} " if insert_ws else token)

        return "".join(cap_text)

    def _make_image_ci(
        self,
        ci_id: str,
        ci_type: str,
        page_num: int,
        parts: list[dict],
        coords: list[int],
        corresp_ci: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Helper to construct a Content Item (CI) of type image in canonical format.

        Args:
            ci_id (str): Unique ID for this CI.
            ci_type (str): Type of content item (e.g., article, image).
            page_num (int): Page number this CI belongs to.
            parts (list[dict]): List of parts associated with this CI.
            coords (list[int]): Optional coordinates for the CI.
            corresp_ci (dict[str, Any] | None): Optional parent CI to inherit metadata (e.g. language).

        Returns:
            dict[str, Any]: Canonical CI dictionary.
        """
        ci = {
            "m": {
                "id": ci_id,
                "tp": ci_type,
                "pp": [page_num],
                "iiif_link": os.path.join(
                    IIIF_ENDPOINT_URI, f"{self.id}-p{str(page_num).zfill(4)}", IIIF_SUFFIX
                ),
                "var_t": self.var_title,
            },
            "c": coords,
            "l": {
                "bl_nlp": self.nlp,
                "src_files": {
                    "mets_xml": os.path.basename(self.mets_file),
                    "alto_xml": [
                        os.path.basename(self.mets_file).replace("mets", str(page_num).zfill(4))
                    ],
                    "page_image": [self.page_filenames[page_num]],
                },
                "id": parts[0]["comp_id"] if parts else None,
                "parts": parts,
            },
        }

        if corresp_ci:
            ci["pOf"] = corresp_ci["m"]["id"]
            if "lg" in corresp_ci["m"]:
                ci["m"]["lg"] = corresp_ci["m"]["lg"]

        return ci

    def _parse_image_cis_in_div(
        self,
        image_parts: list[dict],
        corresp_ci: dict[str, Any],
        counter: int,
        page_xmls: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int]:
        """Parse image content items (CIs) linked to a given content item.

        Iterates over image parts, validates that each corresponds to a single page,
        and constructs a content item (CI) dictionary for each image, optionally
        attaching a caption if available.

        Args:
            image_parts (dict[str, list[dict]]): Mapping of image component IDs
                to their associated parts.
            corresp_ci (dict[str, Any]): The corresponding content item to which
                images are attached.
            counter (int): Counter used to generate unique image CI IDs.
            page_xmls (dict[str, Any]): Mapping of page numbers to parsed XML content.

        Returns:
            tuple[list[dict[str, Any]], int]:
                - List of parsed image CIs.
                - Updated counter after processing.

        Raises:
            ValueError: If an image part is found on more than one page.
        """
        img_cis = []

        # first go through each page to find illustrations not associated to existing CIs.
        for img_comp_id, parts in image_parts.items():
            if parts[0]["comp_label"] == BL_IMG_TYPE and parts[0]["comp_id"] == img_comp_id:
                # ensure that the element is indeed an illustration
                pg_nums = list(set(p["comp_page_no"] for p in parts))
                if len(pg_nums) != 1:
                    msg = (
                        f"{corresp_ci['m']['id']} - Image with part_id {img_comp_id} "
                        f"is associated with multiple pages: {pg_nums}, selecting the first."
                    )
                    logger.error(msg)
                    self._notes.append(msg)

                content_item = self._make_image_ci(
                    ci_id=f"{self.id}-i{str(counter).zfill(4)}",
                    ci_type=CONTENTITEM_TYPE_IMAGE,
                    page_num=pg_nums[0],
                    parts=parts,
                    coords=parts[0]["coords"],
                    corresp_ci=corresp_ci,
                )

                # add the caption from the image as CI title
                caption = self._parse_img_caption(
                    parts, pg_nums[0], page_xmls[str(pg_nums[0])], corresp_ci["m"].get("lg")
                )
                if caption:
                    content_item["m"]["t"] = caption

                if "lg" in corresp_ci["m"]:
                    content_item["m"]["lg"] = corresp_ci["m"]["lg"]

                img_cis.append(content_item)
                counter += 1

        return img_cis, counter

    def find_unlinked_image_cis(
        self, structlink: Tag, ci_counter: int, page_xmls: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Find illustrations in ALTO pages that are not linked in the METS structlink.

        Iterates through all pages, checking for image/illustration blocks (`TYPE` = "illustration" or "image").
        If such blocks are not referenced in the structlink, creates new content items (CIs) for them.

        Args:
            structlink (Tag): The METS structLink element containing linked regions.
            ci_counter (int): Counter used to generate unique CI IDs.
            page_xmls (dict[str, Any]): Mapping of page numbers to parsed ALTO XML documents.

        Returns:
            list[dict[str, Any]]: List of newly created image content items.
        """
        # extract the list of all regions/blocks listed in the METS file
        all_linked_regions = {
            e.get("xlink:href").lstrip("#") for e in structlink.find_all("smLocatorLink")
        }
        image_cis = []

        for page in self.pages:

            # fetch the xml for this page which was already read
            pt_space = page_xmls[str(page.number)]

            for block in pt_space.children:
                if isinstance(block, NavigableString):
                    continue

                block_id = block.get("ID")
                block_type = block.get("TYPE")

                # if the block is an illustration which was not attached to an existing CI, create a CI for it.
                if (
                    block_type
                    and block_type.lower() in [BL_IMG_TYPE, "image"]
                    and block_id not in all_linked_regions
                ):
                    coords = alto.distill_coordinates(block)
                    ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"

                    # since it's not linked to any other block we cannot have a caption/title
                    content_item = self._make_image_ci(
                        ci_id=f"{self.id}-i{str(ci_counter).zfill(4)}",
                        ci_type=CONTENTITEM_TYPE_IMAGE,
                        page_num=page.number,
                        parts=[
                            {
                                "comp_id": block_id,
                                "comp_label": block_type.lower(),
                                "comp_fileid": f"img{str(page.number).zfill(3)}-alto",
                                "comp_page_no": page.number,
                            }
                        ],
                        coords=coords,
                    )

                    msg = (
                        f"{self.id} page {page.number} -> found an unlinked illustration: {block_id}, "
                        f"coords = {coords}, adding the CI: {ci_id}"
                    )
                    self._notes.append(msg)

                    image_cis.append(content_item)
                    ci_counter += 1

        return image_cis

    def _parse_content_items(self) -> list[dict[str, Any]]:
        """Extract and normalize all content items (CIs) from a METS XML file.

        The function parses the logical structure map (`structMap/LOGICAL`) to find
        all content items (articles, illustrations, etc.), enriches them with metadata,
        and attaches any unlinked illustrations found in the ALTO page files.
        It also computes and assigns the reading order for the issue.

        Returns:
            list[dict[str, Any]]: List of all parsed content items in canonical format.
        """
        mets_doc = self.xml
        content_items = []
        # Get logical structure of issue
        divs = (
            mets_doc.find("structMap", {"TYPE": "LOGICAL"})
            .find("div", {"TYPE": "ISSUE"})
            .findChildren("div")
        )

        phys_structmap = mets_doc.find("structMap", {"TYPE": "PHYSICAL"})
        structlink = mets_doc.find("structLink")

        # Preload PrintSpace XMLs for all pages
        page_xmls = {str(page.number): page.xml.find("PrintSpace") for page in self.pages}

        counter = 1
        for div in divs:
            # Parse Each contentitem
            dmd_sec = mets_doc.find("dmdSec", {"ID": div.get("DMDID")})
            if not dmd_sec:
                msg = f'Warning: {self.id} ({self.mets_file}): --- issue div={mets_doc.find("structMap", {"TYPE": "LOGICAL"}).find("div", {"TYPE": "ISSUE"}).get("ID")}'
                print(msg)
                self._notes.append(msg)
            parsed_ci, image_parts = self._parse_content_item(
                div, counter, phys_structmap, structlink, dmd_sec
            )

            content_items.append(parsed_ci)
            counter += 1

            if image_parts:
                # Attach illustrations linked to this CI as standalone CIs
                image_cis, counter = self._parse_image_cis_in_div(
                    image_parts, parsed_ci, counter, page_xmls
                )
                content_items.extend(image_cis)

        # Detect standalone illustrations in ALTO pages not linked to any CI
        unlinked_img_cis = self.find_unlinked_image_cis(structlink, counter, page_xmls)
        content_items.extend(unlinked_img_cis)

        # Compute and assign reading order
        reading_order_dict = get_reading_order(content_items)
        for ci in content_items:
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        return content_items

    def _find_variant_title(self) -> None:
        """Find and assign the variant title for this issue from the BL titles file."""

        with open(os.path.join(self.bl_base_dir, BL_TITLES_FILE), "r", encoding="utf-8") as fin:
            titles = json.load(fin)

        titles_for_alias_nlp = titles["-".join([self.alias, self.nlp])]

        found = False
        for str_period, title_dict in titles_for_alias_nlp.items():
            period = [int(y) for y in str_period.split("-")]
            # ensure that this issue is indeed in the period listed for the given title
            if self.date.year in range(period[0], period[1] + 1):
                self.var_title = title_dict.get("Variant Title")
                self.bl_work_title = title_dict.get("Working title (BL)")
                self.norm_title = title_dict.get("Normalized Working Title")  # not used yet
                found = True
                break  # stop at first matching period

        if not found:
            available_periods = ", ".join(titles_for_alias_nlp.keys())
            msg = (
                f"{self.id} ({self.nlp}) - Issue year {self.date.year} does not match "
                f"any available title periods: {available_periods}"
            )
            print(msg)
            logger.warning(msg)
            self._notes.append(msg)

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
