"""This module contains the definition of BNF importer classes for segmented data.

The classes define newspaper Issues and Pages objects which convert OCR and OLR data in
the BNF version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

import gzip
import logging
import os
from glob import glob
from time import strftime
from typing import Optional

from bs4 import BeautifulSoup
from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

from text_preparation.importers import CONTENTITEM_TYPE_IMAGE
from text_preparation.importers.bnf.helpers import (
    BNF_CONTENT_TYPES,
    add_div,
    get_manifest_info,
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

IIIF_IMAGE_URI = "https://openapi.bnf.fr/iiif/image/v3/ark:/12148/"
IIIF_PRES_URI = "https://openapi.bnf.fr/iiif/presentation/v3/ark:/12148/"
IIIF_MANIFEST_SUFFIX = "manifest.json"
IIIF_SUFFIX = "info.json"


class BnfMpNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in BNF "Marché Presse" OLR (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        manifest_dims (Optional[tuple[int, int]]): Facsimile (width, height)
            of this page as found in the issue's `manifest.json`, if any.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.
        is_gzip (bool): Whether the page's corresponding file is in .gzip.
        page_width (float): Width in pixels of the page, from the ALTO file.
        page_height (float): Height in pixels of the page, from the ALTO file.
    """

    def __init__(
        self,
        _id: str,
        number: int,
        filename: str,
        basedir: str,
        manifest_dims: Optional[tuple[int, int]] = None,
    ) -> None:

        self.is_gzip = filename.endswith("gz")
        super().__init__(_id, number, filename, basedir)

        page_tag = self.xml.find("Page")
        self.page_width = float(page_tag.get("WIDTH"))
        self.page_height = float(page_tag.get("HEIGHT"))
        alto_fw, alto_fh = int(self.page_width), int(self.page_height)

        # `fw`/`fh` are preferably read from the issue's manifest.json (passed
        # in from `BnfMpNewspaperIssue._find_pages`); when unavailable (the
        # case for all current legacy mp_olr issues), fall back to the ALTO
        # Page tag's WIDTH/HEIGHT attributes read above.
        self._dim_mismatch_note = None
        if manifest_dims is not None:
            mft_w, mft_h = manifest_dims
            self.page_data["fw"] = int(mft_w)
            self.page_data["fh"] = int(mft_h)
            if int(mft_w) != alto_fw or int(mft_h) != alto_fh:
                self._dim_mismatch_note = (
                    f"{_id} - facsimile dims mismatch between manifest.json "
                    f"({mft_w}x{mft_h}) and ALTO Page tag ({alto_fw}x{alto_fh})."
                )
        else:
            self.page_data["fw"] = alto_fw
            self.page_data["fh"] = alto_fh

    def _parse_font_styles(self) -> None:
        """Parse the styles at the page level."""
        style_divs = self.xml.findAll("TextStyle")

        styles = []
        for d in style_divs:
            styles.append(parse_style(d))

        self.page_data["s"] = styles

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        self.issue = issue
        self.iiif_img_base_uri = os.path.join(IIIF_IMAGE_URI, self.issue.ark_id, f"f{self.number}")
        self.page_data["iiif_img_base_uri"] = self.iiif_img_base_uri
        # self._parse_font_styles()
        if self._dim_mismatch_note is not None:
            self.issue._notes.append(self._dim_mismatch_note)

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
            return super(BnfMpNewspaperPage, self).xml
        else:
            alto_xml_path = os.path.join(self.basedir, self.filename)
            with gzip.open(alto_xml_path, "r") as f:
                raw_xml = f.read()

            alto_doc = BeautifulSoup(raw_xml, "xml")
            return alto_doc


class BnfMpNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in BNF "Marché Presse" OLR (Mets/Alto) format.

    All functions defined in this child class are specific to parsing BNF's
    "Marché Presse" OLR Mets/Alto format.

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
        ark_id (str): Issue ARK identifier, for the issue's pages' iiif links.
        title_ark_id (str): Title-level ARK identifier for the newspaper.
        issue_uid (str): Basename of the Mets XML file of this issue.
        secondary_date (str): Potential secondary date of issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        # TODO handle legacy vs new batch cases for the contents of the issue
        self.secondary_date = issue_dir.secondary_date
        self.ark_id = issue_dir.ark_id
        self.title_ark_id = issue_dir.title_ark

        # collect the data batch from the issueDir
        # This impacts the expected format of the directory content.
        # its value can be "BNF_MP_old" (legacy titles) or "BNF_API_NEW" new data
        self.new_data_batch = issue_dir.batch == "BNF_API_NEW"

        # both format store the information relative to the image dimensions in a "manifest" file
        self.manifest_filename = f"manifest.{'json' if self.new_data_batch else 'xml'}"

        # Issue manifest iiif URI is in format {iiif_prefix}/{ark_id}/manifest.json
        self.iiif_manifest = os.path.join(IIIF_PRES_URI, self.ark_id, IIIF_MANIFEST_SUFFIX)

        # initialize the media title variant in the case it's defined
        self.media_title_variant = None
        super().__init__(issue_dir)

    @property
    def xml(self) -> BeautifulSoup:
        """Read Mets XML file of the issue and create a BeautifulSoup object.

        Returns:
            BeautifulSoup: BeautifulSoup object with Mets XML of the issue.
        """
        if not self.mets_file:
            if self.new_data_batch:
                # first case: new data - the mets is named after the issue ark
                mets_regex = os.path.join(self.path, f"*{self.ark_id}_olr.xml")
                mets_file = glob(mets_regex)
                if len(mets_file) == 0:
                    logger.critical("Could not find METS file in %s", self.path)
                    return None
            else:
                # second case: legacy data - the mets file is named after the issue's dirname
                issue_uid = os.path.basename(self.path)
                mets_regex = os.path.join(self.path, "toc", f"*{issue_uid}.xml")
                mets_file = glob(mets_regex)
                if len(mets_file) == 0:
                    logger.critical("Could not find METS file in %s", self.path)
                    return None

        self.mets_file = mets_file[0]

        with open(self.mets_file, "r", encoding="utf-8") as f:
            raw_xml = f.read()

        mets_doc = BeautifulSoup(raw_xml, "xml")
        return mets_doc

    def get_legacy_manifest_info(self, manifest_path) -> tuple[dict[int, tuple[int, int]], str]:
        """Read the issue's legacy `manifest.xml` (METS) file if present.

        Mirrors `get_manifest_info()` (for the new API's `manifest.json`) but
        for the Impresso I batch's legacy `manifest.xml`, which is itself a
        METS document. Page facsimile dimensions live in
        `<techMD MDTYPE="NISOIMG">` sections (`mix:imageWidth`/
        `mix:imageHeight`), each linked to a page's `<file ID="master.{page_no}">`
        entry via that file's `ADMID` attribute (space-separated list of AMD
        ids, one of which is the NISOIMG techMD).

        Note:
            Unlike the new manifest.json (which has a "Titre" metadata entry
            for the media's variant title), this legacy manifest's only
            title-like field (`dc:title` in `DMD.2`) is the issue's own dated
            title (e.g. "1937-03-05 (Année 0, Numéro 1)"), not a media title
            variant, so there is no equivalent field to extract here.

        Args:
            manifest_path (str): Path to the issue's `manifest.xml` file.

        Returns:
            tuple[dict[int, tuple[int, int]], str]: Mapping from page number
                to (width, height) in pixels, and an empty title (no
                equivalent field exists in this format). Empty dict if the
                file is missing or couldn't be parsed as expected.
        """
        page_dims: dict[int, tuple[int, int]] = {}
        title_variant = ""

        if not os.path.exists(manifest_path):
            return page_dims, title_variant

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                raw_xml = f.read()

            manifest_contents = BeautifulSoup(raw_xml, "xml")

            # Map each NISOIMG techMD's ID to its (width, height).
            dims_by_amdid: dict[str, tuple[int, int]] = {}
            for tech_md in manifest_contents.find_all("techMD"):
                md_wrap = tech_md.find("mdWrap")
                if md_wrap is None or md_wrap.get("MDTYPE") != "NISOIMG":
                    continue
                width_tag = tech_md.find("mix:imageWidth")
                height_tag = tech_md.find("mix:imageHeight")
                if width_tag is None or height_tag is None:
                    continue
                amd_id = tech_md.get("ID")
                dims_by_amdid[amd_id] = (
                    int(width_tag.get_text(strip=True)),
                    int(height_tag.get_text(strip=True)),
                )

            # Match each page's master file to its NISOIMG techMD via ADMID.
            for file_tag in manifest_contents.find_all("file", {"ID": True}):
                file_id = file_tag.get("ID")
                if not file_id.startswith("master."):
                    continue
                page_no = int(file_id.split(".")[1])
                for amd_id in (file_tag.get("ADMID") or "").split():
                    if amd_id in dims_by_amdid:
                        page_dims[page_no] = dims_by_amdid[amd_id]
                        break
        except (OSError, ValueError, AttributeError) as e:
            logger.warning("Could not parse legacy manifest.xml at %s: %s", manifest_path, e)

        return page_dims, title_variant

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        Created :obj:`BnfCanonicalPage` instances are added to :attr:`pages`.

        Raises:
            e: Instantiation of a page or adding it to :attr:`pages` failed.
        """
        ocr_path = os.path.join(self.path, "ocr")  # Pages in `ocr` folder

        manifest_path = os.path.join(self.path, self.manifest_filename)
        if self.new_data_batch:
            # Read the facsimile dimensions of each page from the issue's `manifest.json`
            manifest_page_dims, self.media_title_variant = get_manifest_info(manifest_path)
        else:
            # Read the facsimile dimensions of each page from the `manifest.xml` file.
            manifest_page_dims, self.media_title_variant = self.get_legacy_manifest_info(
                manifest_path
            )

        pages = [
            (file, int(file.split(".")[0][1:]))
            for file in os.listdir(ocr_path)
            if not file.startswith(".") and ".xml" in file
        ]

        # sort the pages
        page_filenames, page_numbers = zip(*sorted(pages, key=lambda x: x[1]))

        self.pages = {}
        self.page_files_by_number = {}
        for filename, page_no in zip(page_filenames, page_numbers):
            page_id = f"{self.id}-p{str(page_no).zfill(4)}"
            try:
                self.pages[page_no] = BnfMpNewspaperPage(
                    page_id, page_no, filename, ocr_path, manifest_page_dims.get(page_no)
                )
                self.page_files_by_number[page_no] = filename
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
        # define issue-level legacy info which will be repeated
        issue_level_legacy = {
            "src_files": {
                "mets_xml": os.path.basename(self.mets_file),
                "alto_xml": [],
                "manifest_file": self.manifest_filename,
            },
            "ark_id": self.ark_id,
            "title_ark_id": self.title_ark_id,
        }
        article_div = mets_doc.find("div", {"ID": div_id})  # Get the tag
        # Try to get the body if there is one (we discard headings)
        article_div = article_div.find("div", {"TYPE": "BODY"}) or article_div
        parts = parse_div_parts(article_div)  # Parse the parts of the tag
        metadata, ci = None, None
        # If parts were found, create content item for this DIV
        if len(parts) > 0:

            article_id = f"{self.id}-i{str(item_counter).zfill(4)}"

            # first create the CI skeleton
            metadata = {
                "id": article_id,
                "tp": type_translation[div_type],
                "pp": [],
            }
            if label is not None:
                metadata["t"] = label
            ci = {
                "m": metadata,
                "l": {
                    # Composite ID format for tables
                    "id": div_id,
                    "parts": parts,
                    # add the issue-level legacy
                },
            }
            # add the issue-level legacy
            ci["l"].update(issue_level_legacy)

            # add the source files and pages from the parts.
            for part in ci["l"]["parts"]:
                page_no = part["comp_page_no"]
                if page_no not in ci["m"]["pp"]:
                    ci["m"]["pp"].append(page_no)
                    ci["l"]["src_files"]["alto_xml"].append(self.page_files_by_number[page_no])

            item_counter += 1
        else:  # Otherwise, only parse embedded CIs
            article_id = None

        embedded, item_counter = parse_embedded_cis(
            article_div,
            label,
            self.id,
            article_id,
            item_counter,
            issue_level_legacy,
            self.page_files_by_number,
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
                iiif_link = os.path.join(
                    IIIF_IMAGE_URI, self.ark_id, f"f{page.number}", IIIF_SUFFIX
                )

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
            # Additional BNF-specific identifiers, added for every content
            # item (both the ones created directly in `_parse_div`, and the
            # ones produced by the shared `parse_embedded_cis`).
            x["l"]["ark_id"] = self.ark_id
            x["l"]["title_ark_id"] = self.title_ark_id

        # once the pages are added to the metadata, compute & add the reading order
        reading_order_dict = get_reading_order(content_items)
        for item in content_items:
            item["m"]["ro"] = reading_order_dict[item["m"]["id"]]

        self.pages = list(self.pages.values())

        # by default the date is considered to be exact
        is_exact_date = True

        # Note for newspapers with two dates (197 cases)
        if self.secondary_date is not None:
            # when the secondary date is only a year or a month, the date is not exact
            if len(self.secondary_date.split("-")) < 3:
                msg = (
                    f"{self.id} - Secondary date {self.secondary_date} has only year or "
                    "year-month. Setting exact_date=False."
                )
                logger.info(msg)
                self._notes.append(msg)
                is_exact_date = False
            else:
                self._notes.append(f"Secondary date {self.secondary_date}")

        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": True,
            "i": content_items,
            "pp": [p.id for p in self.pages],
            "iiif_manifest_uri": self.iiif_manifest,
            "is_exact_date": is_exact_date,
            "n": self._notes,
        }

        # TODO maybe add media_title_variant based on content of manifest or mets file
        if self.media_title_variant:
            # the media title variant is defined if it is found in the manifest json file
            self.issue_data["media_title_variant"] = self.media_title_variant
