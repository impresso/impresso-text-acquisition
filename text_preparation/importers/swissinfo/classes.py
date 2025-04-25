import json
import logging
import os
import shutil
from time import strftime
from typing import Any

from zipfile import ZipFile

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium
from text_preparation.importers.classes import CanonicalIssue, CanonicalPage
from text_preparation.importers.swissinfo.detect import SwissInfoIssueDir
from text_preparation.importers.swissinfo.helpers import parse_lines, compute_paragraph_coords
from impresso_essentials.io.fs_utils import canonical_path

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
        page_dir (PageDir): Identifying information about the page.

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
        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
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

    def _extract_regions(self, ocr_json: dict[str, Any]) -> list[dict[str, Any]]:

        all_line_xy_coords, lines = parse_lines(ocr_json["blocks_with_lines"])

        # easier to merge all the coords if they stay in x1yx2y2 format
        para_coords = compute_paragraph_coords(all_line_xy_coords)

        # in SWISSINFO rb, we have 1 block=1 line.
        # we decided to keep the paragraphs equal to the regions, so 1 paragraph and 1 region
        paragraph = {
            "c": para_coords,
            "l": lines,
        }

        # return the constructed region
        return {
            "c": para_coords,
            "p": [paragraph],
            "pOf": self.issue.content_items[0]["m"]["id"],
        }


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
        self.json_file = self.find_json_file()
        self.metadata_file = issue_dir.metadata_file
        self._notes = []
        self.pages = []

        self._find_pages()
        self._compose_content_item()
        self._add_bulletin_metadata()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "st": SourceType.RB.value,
            "sm": SourceMedium.TPS.value,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "rc": SWISSINFO_RB_CHANNEL,
        }

        if self.program:
            self.issue_data["rp"] = self.program

        self.issue_data["n"] = self._notes

    def find_json_file(
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

        for page in bulletin_json["ocr_pages"]:

            page_img_file = bulletin_json["jp2_full_paths"][page["page_num"]]
            page_no = int(page["page_num"]) + 1
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

    def _compose_content_item(self) -> None:

        ci_metadata = {
            "id": "{}-i{}".format(self.id, str(1).zfill(4)),
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
            self.program = bulletin_metadata[0]["program_title"]
            if bulletin_metadata[0]["program_subtitle"] != "":
                self.program += f" - {bulletin_metadata[0]['program_subtitle']}"
        else:
            self.content_items[0]["m"]["tp"] = DEFAULT_RB_TYPE
            self.program = None
