"""This module contains the definition of the BCUL importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the ABBYY format to a unified canoncial format.
"""

import codecs
import logging
import os
import time
from time import strftime
from typing import Any
import json

import requests
from bs4 import BeautifulSoup, Tag

import certifi, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from impresso_essentials.utils import SourceMedium, SourceType, timestamp
from text_preparation.importers.bcul.helpers import (
    find_mit_file,
    get_page_number,
    get_div_coords,
    parse_textblock,
    verify_issue_has_ocr_files,
    find_page_file_in_dir,
)
from text_preparation.importers.classes import CanonicalIssue, CanonicalPage
from text_preparation.importers import CONTENTITEM_TYPE_IMAGE, CONTENTITEM_TYPE_TABLE
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

BCUL_IMAGE_TYPE = "Picture"
BCUL_TABLE_TYPE = "Table"
BCUL_CI_TYPES = {BCUL_IMAGE_TYPE, BCUL_TABLE_TYPE}
BCUL_CI_TRANSLATION = {
    BCUL_IMAGE_TYPE: CONTENTITEM_TYPE_IMAGE,
    BCUL_TABLE_TYPE: CONTENTITEM_TYPE_TABLE,
}
IIIF_PRES_BASE_URI = "https://scriptorium.bcu-lausanne.ch/api/iiif"
IIIF_IMG_BASE_URI = f"{IIIF_PRES_BASE_URI}-img"
IIIF_SUFFIX = "info.json"
IIIF_MANIFEST_SUFFIX = "manifest"


class BculNewspaperPage(CanonicalPage):
    """Newspaper page in BCUL (Abbyy) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        page_path (str): Path to the Abby XML page file.
        iiif_uri (str): URI to image IIIF of this page.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Issue this page is from.
        path (str): Path to the Abby XML page file.
        iiif_base_uri (str): URI to image IIIF of this page.
    """

    def __init__(
        self,
        _id: str,
        number: int,
        page_path: str,
        iiif_uri: str,
        width: int | None = None,
        height: int | None = None,
    ) -> None:
        super().__init__(_id, number)
        self.path = page_path
        self.iiif_base_uri = iiif_uri
        self.width = width
        self.height = height
        self.page_data = {
            "id": _id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "r": [],  # here go the page regions
            "iiif_img_base_uri": iiif_uri,
        }

        # # Add width and height if available
        if self.width is not None and self.height is not None:
            self.page_data["fw"] = self.width
            self.page_data["fh"] = self.height

    @property
    def xml(self) -> BeautifulSoup | None:
        xml = None
        if self.path.endswith("bz2"):
            with codecs.open(self.path, encoding="bz2") as f:
                xml = BeautifulSoup(f, "xml")
        elif self.path.endswith("xml"):
            with open(self.path, encoding="utf-8") as f:
                xml = BeautifulSoup(f, "xml")
        else:
            msg = f"{self.id} - self.path ({self.path}) does not end with 'bz2' or 'xml'!"
            logger.error(msg)
            print(msg)
            raise AttributeError(msg)

        return xml

    @property
    def ci_id(self) -> str:
        """Create and return the content item ID of the page.

        Given that BCUL data do not entail article-level segmentation,
        each page is considered as a content item. Thus, to mint the content
        item ID we take the canonical page ID and simply replace the "p"
        prefix with "i".

        Returns:
            str: Content item id.
        """
        split = self.id.split("-")
        split[-1] = split[-1].replace("p", "i")
        return "-".join(split)

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue

    def get_ci_divs(self) -> list[Tag]:
        """Fetch and return the divs of tables and pictures from this page.

        While BCUL does not entail article-level segmentation, tables and
        pictures are still segmented. They can thus have their own content item
        objects.

        Returns:
            list[Tag]: List of segmented table and picture elements.
        """
        return self.xml.findAll("block", {"blockType": lambda x: x in BCUL_CI_TYPES})

    def parse(self) -> None:
        doc = self.xml
        text_blocks = doc.findAll("block", {"blockType": "Text"})
        page_data = [parse_textblock(tb, self.ci_id) for tb in text_blocks]
        self.page_data["cc"] = True
        self.page_data["r"] = page_data


class BculNewspaperIssue(CanonicalIssue):
    """Newspaper Issue in BCUL (Abby) format.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj: `CanonicalPage` instances from this issue.
        mit_file (str): Path to the ABBY 'mit' file that contains the OLR.
        is_json (bool): Whether the `mit_file` has the `json` file extension.
        is_xml (bool): Whether the `mit_file` has the `xml` file extension.
        iiif_manifest (str): Presentation iiif manifest for this issue.
        content_items (list[dict]): List of content items in this issue.
    """

    def __init__(self, issue_dir) -> None:
        super().__init__(issue_dir)
        self.mit_file = find_mit_file(issue_dir.path)
        self.is_json = issue_dir.mit_file_type == "json"
        self.is_xml = issue_dir.mit_file_type == "xml"

        if self.path is None:
            if issue_dir.path is None:
                raise ValueError(f"{self.id}: provided path is None")
        # Check if MIT file in one of the two required formats
        if not (self.is_json or self.is_xml):
            err_msg = f"Mit file {self.mit_file} is not JSON nor XML"
            logger.error(err_msg)
            raise ValueError(err_msg)

        # issue manifest, identifier is directory name
        self.iiif_manifest = os.path.join(
            IIIF_PRES_BASE_URI, self.path.split("/")[-1], IIIF_MANIFEST_SUFFIX
        )
        self.content_items = []

        self.iiif_canvases = self.query_iiif_api()

        self._find_pages()
        self._find_content_items()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "iiif_manifest_uri": self.iiif_manifest,
            "n": self._notes,
        }

    def _get_iiif_link_json(self, page_path: str) -> str:
        """Return iiif image base uri in case `mit` file is in JSON.

        In this case, the page identifier is simply the page XML file's name.

        Args:
            page_path (str): Path to the page XML file the content item is on.

        Returns:
            str: IIIF image base uri (without suffix).
        """
        # when mit file is a JSON, each page file's name is the iiif identifier
        page_identifier = os.path.basename(page_path).split(".")[0]
        return os.path.join(IIIF_IMG_BASE_URI, page_identifier)

    def query_iiif_api(
        self, num_tries: int = 0, max_retries: int = 3, sleep_seconds: int = 2
    ) -> dict[str, Any]:
        """Query the Scriptorium IIIF API for the issue's manifest data.

        TODO: implement the retry approach with `celery` package or similar.

        Args:
            num_tries (int, optional): Number of retry attempts. Defaults to 0.
            max_retries (int, optional): Maximum number of attempts. Defaults to 3.

        Returns:
            dict[str, Any]: Issue's IIIF "canvases" for each page.

        Raises:
            Exception: If the maximum number of retry attempts is reached.
        """
        try:
            logger.info("Submitting request to iiif API for %s: %s", self.id, self.iiif_manifest)
            response = requests.get(self.iiif_manifest, timeout=60, verify=False)
            data = response.json()
            if response.status_code == 200:
                if "sequences" in data and data["sequences"]:
                    return data["sequences"][0]["canvases"]
                elif "items" in data and data["items"]:
                    return data["items"]
                else:
                    msg = (
                        f"{self.id}: IIIF manifest format not recognized. Keys: {list(data.keys())}"
                    )
                    raise requests.exceptions.HTTPError(msg)

            if response.status_code == 404:
                msg = (
                    f"{self.id}: Request failed with response code: {response.status_code}. "
                    "Issue will not be processed."
                )
                raise requests.exceptions.HTTPError(msg)
            else:
                msg = f"{self.id}: Request failed with response code: {response.status_code}"
        except Exception as e:
            msg = f"Error while querying IIIF API for {self.id} (iiif: {self.iiif_manifest}): {e}."

        if num_tries < max_retries:
            msg += f" Retrying {3-num_tries} times."
            logger.error(msg)
            return self.query_iiif_api(num_tries + 1)
            # logger.info("Retrying in %s seconds... (%s/%s)", sleep_seconds, num_tries + 1, max_retries)
            # time.sleep(sleep_seconds)
            # return self.query_iiif_api(num_tries + 1, max_retries, sleep_seconds)

        msg += f" Max number of retries reached, {self.id} will not be processed."
        logger.error(msg)
        raise requests.exceptions.HTTPError(msg)

    def _get_iiif_link_xml(self, page_number: int, canvases: dict[str, Any]) -> str | None:
        """Return iiif image base uri in case `mit` file is in XML.

        In this case, the iiif URI to images needs to be fetched from the iiif
        presentation API, in the issue's manifest.

        Args:
            page_number (int): Page number for which to fetch the iiif URI.
            canvases (dict[str, Any]): Page canvases from the IIIF issue manifest.

        Returns:
            str | None: IIIF image base uri (no suffix) if found in manifest else None.
        """

        page_canvas = None

        for c in canvases:
            label = c.get("label")

            # IIIF v3: {"en": ["Page 1"]}
            if isinstance(label, dict):
                label_val = next(iter(label.values()))[0] if label.values() else None

            # IIIF v2: "1" or "Page 1"
            else:
                label_val = label
            if label_val:
                try:
                    num_in_label = int("".join(ch for ch in str(label_val) if ch.isdigit()))
                except ValueError:
                    num_in_label = None
            else:
                num_in_label = None

            if num_in_label == page_number:
                page_canvas = c
                break

        if page_canvas is None:
            logger.warning("%s: Page %s not found in IIIF manifest.", self.id, page_number)
            return None

        # --- IIIF v2 or v3: extract the image URL ---
        if "images" in page_canvas:  # IIIF v2
            iiif = page_canvas["images"][0]["resource"]["@id"]
        elif "items" in page_canvas:  # IIIF v3
            # In v3, each canvas has items -> annotations -> body -> id
            try:
                iiif = page_canvas["items"][0]["items"][0]["body"]["id"]
            except Exception as e:
                logger.error("Could not extract IIIF image for page %s: %s", page_number, e)
                return None
        else:
            logger.error("No IIIF image data found for page %s.", page_number)
            return None

        # Return image base URI (remove suffix)
        return "/".join(iiif.split("/")[:-4])

    def _find_pages_xml(self) -> None:
        """Finds the pages when the format for the `mit_file` is XML.

        In this case, the IIIF links for each page first need to be fetched using
        the BCUL's IIIF presentation API, once per issue.
        Any page for which the iiif link is missing will not be instantiated, and
        the error will be logged.
        """
        # List all files and get the filenames from the MIT file
        files = os.listdir(self.path)

        with open(self.mit_file, encoding="utf-8") as f:
            mit = BeautifulSoup(f, "xml")

        pages = sorted([os.path.basename(x.get("xml")) for x in mit.findAll("image")])
        for p in pages:
            found = False
            # Since files are in .xml.bz2 format,
            # need to check which one corresponds
            for f in files:
                if p in f:
                    page_path = os.path.join(self.path, f)
                    page_no = int(f.split(".")[0].split("_")[-1])
                    page_id = "{}-p{}".format(self.id, str(page_no).zfill(4))
                    page_iiif = self._get_iiif_link_xml(page_no, self.iiif_canvases)
                    if page_iiif is None:
                        logger.error(
                            "%s: No iiif link found for Page %s on API (manifest: %s)",
                            self.id,
                            page_no,
                            self.iiif_manifest,
                        )
                        continue
                    
                    canvas = self.iiif_canvases[page_no - 1]
                    width = canvas.get("width")
                    height = canvas.get("height")
                    if width is None or height is None:
                        try:
                            ann_body = canvas["items"][0]["items"][0]["body"]
                            width = width or ann_body.get("width")
                            height = height or ann_body.get("height")
                        except (KeyError, IndexError, TypeError):
                            pass
                    if width is None or height is None:
                        logger.warning(
                            "%s: Width or height missing for Page %s on IIIF manifest.",
                            self.id,
                            page_no,
                        )

                    page = BculNewspaperPage(page_id, page_no, page_path, page_iiif, width, height)
                    self.pages.append(page)
                    found = True
            if not found:
                logger.error("Page %s not found in %s", p, self.path)
                self._notes.append(f"Page {p} missing: not found in {self.path} or on API.")

    def _find_pages_json(self) -> None:
        """Finds the pages when the format for the `mit_file` is JSON.

        In this case, the filename for each page also corresponds to its IIIF
        unique identifier, and can be used directly without using the API.
        """
        # Ensure issue has OCR data files before processing it.
        verify_issue_has_ocr_files(self.path)

        # If it does, get all exif files
        files = [
            os.path.join(self.path, x)
            for x in os.listdir(self.path)
            if os.path.splitext(x)[0].endswith("exif")
        ]

        for f in files:

            with open(f, "r", encoding="utf-8") as jf:
                exif_list = json.load(jf)
            exif_data = exif_list[0]
            jpeg_info = exif_data.get("Jpeg2000", {})
            width = jpeg_info.get("ImageWidth")
            height = jpeg_info.get("ImageHeight")
            # Page file is the same name without `_exif`
            file_id = os.path.splitext(os.path.basename(f))[0].replace("_exif", "")
            # check the page xml file exists
            page_path = find_page_file_in_dir(self.path, file_id)
            if page_path is None:
                # if the page does not exist, skip this page
                self._notes.append(f"Couldn't find the page corresponding to {file_id}")
                logger.info("%s: Did not find the page for %s skipping.", self.id, file_id)
                continue
            page_no = get_page_number(f)
            page_id = "{}-p{}".format(self.id, str(page_no).zfill(4))
            page_iiif = self._get_iiif_link_json(page_path)
            page = BculNewspaperPage(page_id, page_no, page_path, page_iiif, width, height)
            self.pages.append(page)

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created CanonicalPage instances are added to pages. Since BCUL has two
        different formats for the issue level, both need to be handled.
        """
        if self.is_json:
            self._find_pages_json()
        elif self.is_xml:
            self._find_pages_xml()


    def _find_content_items(self) -> None:
        """Find all content items (pages, images, tables) in this Newspaper issue.

        - Loops once over all pages.
        - Each page gets one content item with all text block IDs in comp_id.
        - Each image/table block on that page gets its own content item.
        - Adds 'issue_id' and 'page_id' in legacy metadata.
        """
        mit_basename = os.path.basename(self.mit_file) if self.mit_file else None

        # Extract numeric issue_id from IIIF manifest URL
        issue_id = None
        try:
            issue_id = int(self.iiif_manifest.split("/")[-2])
        except Exception:
            logger.warning("Could not extract issue_id from manifest %s", self.iiif_manifest)

        canvas_id_map = {}
        for canvas in self.iiif_canvases:
            canvas_url = canvas.get("id") or canvas.get("@id")
            if not canvas_url:
                continue
            try:
                page_num = None
                label = canvas.get("label")
                if isinstance(label, dict):  # IIIF v3
                    label_val = next(iter(label.values()))[0]
                else:
                    label_val = label
                if label_val:
                    page_num = int("".join(ch for ch in str(label_val) if ch.isdigit()))
                page_id_num = int(canvas_url.split("/")[-1])
                if page_num:
                    canvas_id_map[page_num] = page_id_num
            except Exception:
                continue

        # Loop once on all pages
        ci_counter = 1
        ci_counter_intern = 0
        nb_pages = len(self.pages)

        for page in sorted(self.pages, key=lambda x: x.number):
            page_file_basename = (
                os.path.basename(page.path) if getattr(page, "path", None) else None
            )
            # ---- PAGE CONTENT ITEM ----
            page_xml = page.xml
            text_blocks = page_xml.findAll("block", {"blockType": "Text"})
            text_ids = [tb.get("pageElemId") for tb in text_blocks if tb.get("pageElemId")]

            page_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"
            page_id = canvas_id_map.get(page.number)

            legacy_page = {
                "issue_id": issue_id,
                "page_id": page_id,
                "parts": [
                    {
                        "comp_role": "text blocks",
                        "comp_id": text_ids,  # list of text block IDs
                        "comp_fileid": page_file_basename,
                        "comp_page_no": page.number,
                    }
                ],
                "source": mit_basename,
            }

            page_ci = {
                "m": {
                    "id": page_ci_id,
                    "pp": [page.number],
                    "tp": "page",
                },
                "l": legacy_page,
            }
            self.content_items.append(page_ci)

            counter_temp = 0
            
            ci_divs = page_xml.findAll("block", {"blockType": lambda x: x in BCUL_CI_TYPES})

            for div_tag in ci_divs:
                block_type = div_tag.get("blockType")
                coords = get_div_coords(div_tag)
                ci_type = BCUL_CI_TRANSLATION.get(block_type, "unknown")
                comp_id = div_tag.get("pageElemId")
                add_nb = ci_counter + nb_pages + ci_counter_intern + counter_temp
                ci_id = f"{self.id}-i{str(add_nb).zfill(4)}"

                part = {
                    "comp_role": ci_type,
                    "comp_fileid": page_file_basename,
                    "comp_page_no": page.number,
                }
                # don't define the comp_id if it's None (does not pass schema validation)
                if comp_id:
                    part["comp_id"] = comp_id

                if coords:
                    part["coords"] = coords

                legacy = {
                    # "file_id": file_id,
                    "issue_id": issue_id,
                    "page_id": canvas_id_map.get(page.number),
                    "parts": [part],
                    "source": mit_basename,
                }

                ci = {
                    "m": {
                        "id": ci_id,
                        "pp": [page.number],
                        "tp": ci_type,
                    },
                    "l": legacy,
                }

                if ci_type == CONTENTITEM_TYPE_IMAGE and coords:
                    ci["c"] = coords
                    if getattr(page, "iiif_base_uri", None):
                        ci["m"]["iiif_link"] = os.path.join(page.iiif_base_uri, IIIF_SUFFIX)

                self.content_items.append(ci)
                counter_temp += 1

            ci_counter_intern += counter_temp - 1
            ci_counter += 1

        # ---- ADD READING ORDER ----
        reading_order_dict = get_reading_order(self.content_items)
        for item in self.content_items:
            item["m"]["ro"] = reading_order_dict[item["m"]["id"]]
