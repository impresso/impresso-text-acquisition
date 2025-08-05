"""This module contains the definition of the SWISSINFO importer classes."""

import os
import logging
import json
from typing import Any
from statistics import mean

from text_preparation.utils import coords_to_xywh
from text_preparation.importers.classes import CanonicalIssue, CanonicalPage
from text_preparation.importers.swissinfo.helpers import parse_lines, compute_agg_coords
from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = "info.json"
IIIF_IMAGE_SUFFIX = "full/full/0/default.jpg"
METADATA_FILENAME = "SOC_rb_metadata.json"

# declare the constant values for the bulletins - CI type and channel
SWISSINFO_CI_TYPE = "chronicle"
DEFAULT_RB_TYPE = "radio_bulletin"
SWISSINFO_RB_CHANNEL = "SOC (KWD)"


class SwissInfoRadioBulletinPage(CanonicalPage):
    """Radio-Bulletin Page for SWISSINFO's OCR format.

    Args:
        _id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.

    Attributes:
        id (str): Canonical Page ID (e.g. ``SOC_CJ-1940-01-05-a-p0001``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Radio Bulleting issue this page is from.
        path (str): Path to the jp2 page file.
    """

    def __init__(self, _id: str, number: int) -> None:
        super().__init__(_id, number)
        self.iiif_base_uri = os.path.join(IIIF_ENDPOINT_URI, self.id, IIIF_SUFFIX)
        self.notes = []
        self.avg_par_size = None
        self.split_page_blocks = None
        self.page_data = {
            "id": self.id,
            "ts": timestamp(),
            "r": [],  # here go the page regions
            "iiif_img_base_uri": self.iiif_base_uri,
            "st": SourceType.RB.value,
            "sm": SourceMedium.TPS.value,
            "cc": True,  # all Swissinfo data has had rescaled coords
        }

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue

    def parse(self) -> None:
        # go from the "ocr_pages" format of the json file to the canonical page regions etc
        # page numbering starts at 1
        ocr_json = self.issue.page_jsons[self.number - 1]

        # add the facsimile width and height from the rescaling
        self.page_data["fw"] = ocr_json["jp2_img_size"][0]
        self.page_data["fh"] = ocr_json["jp2_img_size"][1]

        # add the page regions to the page data
        page_regions = self._extract_regions(
            ocr_json
        )  # [parse_textblock(tb, self.ci_id) for tb in text_blocks]
        self.page_data["r"] = page_regions
        self.page_data["n"] = self.notes
        self.page_data["parag_avg_size"] = self.avg_par_size

    def _extract_regions(self, ocr_json: dict[str, Any]) -> list[dict[str, Any]]:

        all_blocks_xy_coords, paragraphs = parse_lines(
            ocr_json["blocks_with_lines"],
            self.id,
            self.notes,
        )

        if len(all_blocks_xy_coords) == 0:
            msg = (
                f"{self.id} - Warning! No line coords to merge! len(ocr_json['blocks_with_lines'])={len(ocr_json['blocks_with_lines'])}, len(paragraphs)={len(paragraphs)}. Returning empty region."
                f"checking if there are other blocks: len(ocr_json['blocks_without_lines'])={len(ocr_json['blocks_without_lines'])}, ocr_json['blocks_without_lines']={ocr_json['blocks_without_lines']}, ocr_json['jp2_img_size']={ocr_json['jp2_img_size']}"
            )
            print(msg)
            logger.info(msg)
            return []
        else:
            # easier to merge all the coords if they stay in x1yx2y2 format
            region_coords = coords_to_xywh(compute_agg_coords(all_blocks_xy_coords))

        if self.issue.split_page_blocks:
            # merging all the lines of various paragraphs into one paragraph
            merged_paragraph_lines = []

            for p in paragraphs:
                merged_paragraph_lines.extend(p["l"])
            # there is now one paragraph = to the region, so same coordinates
            paragraphs = [{"c": region_coords, "l": merged_paragraph_lines}]

        # return the constructed region
        return [
            {
                "c": region_coords,
                "p": paragraphs,
                "pOf": self.issue.content_items[0]["m"]["id"],
            }
        ]


class SwissInfoRadioBulletinIssue(CanonicalIssue):
    """Radio-Bulletin Issue for SWISSINFO's OCR format.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``SOC_CJ-1940-01-05-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Media title unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj: `SwissInfoRadioBulletinPage` instances from this issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        super().__init__(issue_dir)
        self.json_file = self._find_json_file()
        self.metadata_file = issue_dir.metadata_file
        self._notes = []
        self.pages = []
        self.has_pages = True

        self._find_pages()
        self._compose_content_item()
        self._add_bulletin_metadata()

        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.RB.value,
            "sm": SourceMedium.TPS.value,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "rc": SWISSINFO_RB_CHANNEL,
        }

        if self.program:
            self.issue_data["rp"] = self.program

        self.issue_data["n"] = self._notes

    def _find_json_file(
        self,
    ) -> str | None:
        """Ensure the path to the issue considered contains a JSON file.

        Args:
            path (str): Path to the issue considered

        Raises:
            FileNotFoundError: No JSON OCR file was found in the path.
        """
        json_file_path = os.path.join(self.path, f"{self.id}.json")
        if not os.path.exists(json_file_path):
            msg = (
                f"{self.id} - The issue's folder {self.path} does not contain any the "
                "required json file . Issue cannot be processed as a result."
            )
            raise FileNotFoundError(msg)

        else:
            return json_file_path

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`NewspaperPage` instances are added to :attr:`pages`.
        """

        # open the JSON issue file
        with open(self.json_file, encoding="utf-8") as f:
            bulletin_json = json.load(f)

        self.bulletin_lang = bulletin_json["lang"]
        # remove the local part of the path
        self.src_pdf_file = "/".join(bulletin_json["original_path"].split("/")[-3:])

        self.page_jsons = []
        missing_pages = []
        # whether the page blocks should be split into lines or kept to create paragraphs
        self.split_page_blocks = False

        for page in bulletin_json["ocr_pages"]:
            page_no = int(page["page_num"]) + 1
            if len(page["blocks_with_lines"]) == 0:
                missing_pages.append(page_no)
                msg = (
                    f"{self.id}, page {page_no} has no block with lines, it will not contain text."
                )
                # print(msg)
                # logger.info(msg)
                self._notes.append(msg)
            else:
                par_sizes = [len(block["lines"]) for block in page["blocks_with_lines"]]
                # print(f"")
                # if the blocks of any of the pages are split, they are split for all pages.
                self.split_page_blocks = self.split_page_blocks or (
                    mean(par_sizes) < 3.5 or len(par_sizes) > 20
                )

            page_img_file = bulletin_json["jp2_full_paths"][page["page_num"]]
            page_id = "{}-p{}".format(self.id, str(page_no).zfill(4))
            page_img_name = page_img_file.split("/")[-1].split(".")[0]
            # ensure the page numbering is correct
            assert (
                page_img_name == page_id
            ), f"{self.id} problem with page numbering/naming, page_img_name ({page_img_name}) != page_id ({page_id})"

            self._notes.append(
                f"Page {page_no}: page size within OCR before coord rescaling: {page['ocr_page_size']}"
            )

            # format the page json for future use
            self.page_jsons.append(page)

            # create page object and add it to the list of pages
            page = SwissInfoRadioBulletinPage(page_id, page_no)
            self.pages.append(page)

            # TODO maybe - extract fonts

        if len(self.pages) == len(missing_pages):
            msg = f"{self.id}, No OCR in any of the pages! This issue won't be ingested."
            print(msg)
            logger.warning(msg)
            self.has_pages = False
        elif len(missing_pages) != 0:
            msg = f"{self.id}, some of the pages ({missing_pages}) had no OCR but not all!"
            print(msg)
            logger.warning(msg)

    def _compose_content_item(self) -> None:

        ci_metadata = {
            "id": f"{self.id}-i{str(1).zfill(4)}",
            "lg": self.bulletin_lang,
            "pp": [p.number for p in self.pages],
            # only this type for now
            "tp": "radio_bulletin",
            "ro": 1,
        }

        # the only legacy we can provide is the original pdf filename
        ci_legacy = {"source": self.src_pdf_file}
        self.content_items = [{"m": ci_metadata, "l": ci_legacy}]

    def _add_bulletin_metadata(self) -> None:
        # The metadata file is in the top-most directory of swissinfo json data
        # metadata_file_path = "/".join(self.path.split("/")[:-5] + [METADATA_FILENAME])
        with open(self.metadata_file, encoding="utf-8") as f:
            rb_metadata = json.load(f)

        # fetch the metadata for the current bulletin
        archive_key = os.path.splitext(os.path.basename(self.src_pdf_file))[0]
        bulletin_metadata = [p for p in rb_metadata if p["archive_key"] == archive_key]

        # not all bulletins had metadata in the swi.xml file
        if len(bulletin_metadata) > 0:
            self.content_items[0]["m"]["t"] = bulletin_metadata[0]["segment_title"]
            self.content_items[0]["m"]["tp"] = SWISSINFO_CI_TYPE
            self.content_items[0]["m"]["var_t"] = bulletin_metadata[0]["program_title"]
            self.program = bulletin_metadata[0]["program_title"]
            if bulletin_metadata[0]["program_subtitle"] != "":
                self.program += f" - {bulletin_metadata[0]['program_subtitle']}"
        else:
            self.content_items[0]["m"]["tp"] = DEFAULT_RB_TYPE
            self.program = None
