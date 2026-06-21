"""This module contains the definition of Chronicling America importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the Chronicling America version of the Mets/Alto format to a unified canonical format.
These classes are subclasses of generic Mets/Alto importer classes.
"""

import os
import re
import logging
from time import strftime
from typing import Any
from bs4 import BeautifulSoup
from bs4.element import Tag

from impresso_essentials.utils import SourceType, SourceMedium, timestamp
from text_preparation.importers import (
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_TABLE,
    CONTENTITEM_TYPE_ARTICLE,
)
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)
from text_preparation.importers.mets_alto import alto
from text_preparation.utils import get_reading_order

logger = logging.getLogger(__name__)

# Working pixel resolution that Impresso's IIIF proxy serves images at. All ALTO
# coordinates (expressed in physical units) are normalized to this DPI so that
# region boxes line up with the served image. See the project knowledge base
# (.cursor/docs/chronicling-america.md §coordinates) for why this is 400.
TARGET_DPI = 400

# ComposedBlock TYPE values (lowercased) that denote a picture rather than text.
IMG_COMPOSED_TYPES = {"illustration", "image"}

# Default coordinate divisor when the ALTO measurement unit is unknown: NDNP
# ALTO is overwhelmingly inch1200, which at 400 DPI divides by 3.
DEFAULT_COORD_DIVISOR = 1200.0 / TARGET_DPI

# Spelled-out language names occasionally found in MODS languageTerm.
_LANG_NAME_TO_ISO = {
    "english": "en",
    "french": "fr",
    "german": "de",
    "spanish": "es",
    "italian": "it",
    "dutch": "nl",
    "portuguese": "pt",
    "polish": "pl",
    "swedish": "sv",
    "norwegian": "no",
    "danish": "da",
    "finnish": "fi",
    "czech": "cs",
}


def measurement_divisor(unit: str | None, target_dpi: int = TARGET_DPI) -> float:
    """Value to divide raw ALTO coordinates by to reach ``target_dpi`` pixels.

    NDNP ALTO is usually in ``inch1200`` (1/1200 inch); some batches use
    ``mm10`` (1/10 mm) or ``pixel``. ``pixel`` coordinates are already in image
    pixels at the (unknown) scan resolution, so they cannot be rescaled without
    the source DPI — we leave them as-is (divisor 1.0) and rely on internal
    consistency with the page dimensions, which are in the same unit.
    """
    u = (unit or "").strip().lower()
    if u == "inch1200":
        return 1200.0 / target_dpi
    if u == "mm10":
        # 1 inch = 25.4 mm = 254 units of 1/10 mm
        return 254.0 / target_dpi
    if u in ("pixel", "pixels"):
        return 1.0
    if u in ("", "<none>"):
        return DEFAULT_COORD_DIVISOR
    logger.warning("Unknown ALTO MeasurementUnit %r; assuming inch1200.", unit)
    return DEFAULT_COORD_DIVISOR


def alto_measurement_unit(alto_soup: BeautifulSoup) -> str | None:
    """Read the ``<MeasurementUnit>`` value from an ALTO document, if present."""
    mu = alto_soup.find("MeasurementUnit")
    if mu and mu.text and mu.text.strip():
        return mu.text.strip()
    return None


def normalize_language(value: str | None) -> str | None:
    """Map an ALTO/MODS language string to a 2-3 letter ISO-639 code or None."""
    if not value:
        return None
    v = value.strip().lower()
    if re.fullmatch(r"[a-z]{2,3}", v):
        return v
    return _LANG_NAME_TO_ISO.get(v)


def _has_image_ancestor(tag: Tag) -> bool:
    """True if ``tag`` is nested inside an Illustration / image ComposedBlock.

    Used to avoid emitting a duplicate image content item for a
    ``<GraphicalElement>`` that already lives inside a captured image block.
    """
    parent = tag.parent
    while parent is not None and getattr(parent, "name", None):
        if parent.name == "Illustration":
            return True
        if parent.name == "ComposedBlock" and (parent.get("TYPE") or "").lower() in IMG_COMPOSED_TYPES:
            return True
        parent = parent.parent
    return False


def collect_image_elements(print_space: Tag) -> list[Tag]:
    """Return all picture regions on a page, deduplicated by element ID.

    Captures native ``<Illustration>`` elements, ``<ComposedBlock TYPE=...>``
    blocks whose type denotes a picture, and standalone ``<GraphicalElement>``
    regions that are not already inside one of the above.
    """
    elements: list[Tag] = []
    seen_ids: set[str] = set()

    def _add(el: Tag) -> None:
        el_id = el.get("ID")
        if el_id and el_id in seen_ids:
            return
        if el_id:
            seen_ids.add(el_id)
        elements.append(el)

    for illus in print_space.find_all("Illustration"):
        _add(illus)
    for cb in print_space.find_all("ComposedBlock"):
        if (cb.get("TYPE") or "").lower() in IMG_COMPOSED_TYPES:
            _add(cb)
    for ge in print_space.find_all("GraphicalElement"):
        if not _has_image_ancestor(ge):
            _add(ge)
    return elements

class ChroniclingAmericaNewspaperPage(MetsAltoCanonicalPage):
    """Newspaper page in Chronicling America (Mets/Alto) format."""

    def __init__(
        self,
        _id: str,
        number: int,
        filename: str,
        basedir: str,
        file_id: str,
        encoding: str = "utf-8",
    ) -> None:
        super().__init__(_id, number, filename, basedir, encoding)
        self.file_id = file_id
        # Divisor to normalize this page's coordinates to TARGET_DPI pixels.
        # Resolved from the ALTO MeasurementUnit in add_issue(); defaults to the
        # inch1200 value so behaviour is unchanged when the unit is missing.
        self.coord_divisor = DEFAULT_COORD_DIVISOR

    def add_issue(self, issue: "ChroniclingAmericaNewspaperIssue") -> None:
        """Add the parent issue and extract page dimensions from ALTO XML Page tag."""
        self.issue = issue
        IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
        self.page_data["iiif_img_base_uri"] = os.path.join(IIIF_ENDPOINT_URI, self.id)
        try:
            doc = self.xml
            self.coord_divisor = measurement_divisor(alto_measurement_unit(doc))
            page_tag = doc.find("Page")
            if page_tag:
                raw_w = int(float(page_tag.get("WIDTH", 0)))
                raw_h = int(float(page_tag.get("HEIGHT", 0)))
                # Normalize physical units to TARGET_DPI pixels.
                self.page_data["fw"] = round(raw_w / self.coord_divisor)
                self.page_data["fh"] = round(raw_h / self.coord_divisor)
            else:
                self.page_data["fw"] = 0
                self.page_data["fh"] = 0
        except Exception as e:
            logger.error(f"Error getting page dimensions for page {self.number}: {e}")
            self.page_data["fw"] = 0
            self.page_data["fh"] = 0

    def _convert_coordinates(
        self, page_regions: list[dict[str, Any]]
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Normalize region coordinates to TARGET_DPI pixels (see coord_divisor)."""
        d = self.coord_divisor
        for region in page_regions:
            if "c" in region:
                region["c"] = [round(val / d) for val in region["c"]]
            if "p" in region:
                for paragraph in region["p"]:
                    if "c" in paragraph:
                        paragraph["c"] = [round(val / d) for val in paragraph["c"]]
                    if "l" in paragraph:
                        for line in paragraph["l"]:
                            if "c" in line:
                                line["c"] = [round(val / d) for val in line["c"]]
                            if "t" in line:
                                for token in line["t"]:
                                    if "c" in token:
                                        token["c"] = [round(val / d) for val in token["c"]]
        return True, page_regions

class ChroniclingAmericaNewspaperIssue(MetsAltoCanonicalIssue):
    """Newspaper Issue in Chronicling America (Mets/Alto) format."""

    def __init__(self, issue_dir: Any) -> None:
        super().__init__(issue_dir)

    def _find_pages(self) -> None:
        """Parse METS structural map to detect issue pages and instantiate page objects."""
        # Resolve the main METS file inside the issue directory
        # Chronicling America METS file is typically [issue_dir_name].xml (e.g. 1932062001.xml)
        self.mets_file = None
        xml_files = [f for f in os.listdir(self.path) if f.endswith(".xml")]
        for f in xml_files:
            # Look for the 10-digit issue ID name (e.g. 1932062001.xml)
            if len(f) == 14 and f[:-4].isdigit():
                self.mets_file = os.path.join(self.path, f)
                break
        if not self.mets_file and xml_files:
            self.mets_file = os.path.join(self.path, xml_files[0])
        if not self.mets_file:
            raise FileNotFoundError(
                f"No METS XML file found in issue directory: {self.path}"
            )

        mets_doc = self.xml
        
        # 1. Map file IDs to filenames from fileSec
        file_map = {}
        file_sec = mets_doc.find("fileSec")
        if file_sec:
            for file_tag in file_sec.find_all("file"):
                file_id = file_tag.get("ID")
                flocat = file_tag.find("FLocat")
                if file_id and flocat:
                    href = flocat.get("xlink:href", "")
                    filename = os.path.basename(href)
                    file_map[file_id] = filename

        # 2. Extract page sequence and numbers from structMap
        struct_map = mets_doc.find("structMap")
        page_files = []
        if struct_map:
            # Find all page divs
            page_divs = struct_map.find_all("div", {"TYPE": lambda x: x and 'page' in x.lower()})
            for idx, div in enumerate(page_divs, start=1):
                ocr_fptr = div.find("fptr", {"FILEID": lambda x: x and x.startswith("ocrFile")})
                if ocr_fptr:
                    file_id = ocr_fptr.get("FILEID")
                    filename = file_map.get(file_id)
                    if filename:
                        page_files.append((idx, filename, file_id))
                        
        # Fallback to directory scan if structMap fails
        if not page_files:
            text_path = os.path.join(self.path, "alto")
            if not os.path.exists(text_path):
                text_path = self.path
            xml_files = sorted([f for f in os.listdir(text_path) if f.endswith(".xml") and not f.endswith("mets.xml")])
            for idx, filename in enumerate(xml_files, start=1):
                page_files.append((idx, filename, f"ocrFile{idx}"))

        self.pages = []
        basedir = os.path.join(self.path, "alto")
        if not os.path.exists(basedir):
            basedir = self.path
            
        for page_num, filename, file_id in sorted(page_files, key=lambda x: x[0]):
            page_id = f"{self.id}-p{str(page_num).zfill(4)}"
            self.pages.append(
                ChroniclingAmericaNewspaperPage(
                    _id=page_id,
                    number=page_num,
                    filename=filename,
                    basedir=basedir,
                    file_id=file_id,
                )
            )

    def _find_content_items(self) -> list[dict[str, Any]]:
        """Extract content items from Alto pages.
        
        Since there is no logical OLR (article structure) in Chronicling America,
        we group all TextBlocks on a page into a single page-level 'page' content item,
        and create separate content items for illustrations ('image') and tables.
        """
        content_items = []
        ci_counter = len(self.pages)

        for page in sorted(self.pages, key=lambda x: x.number):
            page_num_str = str(page.number).zfill(4)
            try:
                alto_soup = page.xml
            except Exception as e:
                logger.error(f"Failed to parse Alto file for page {page.number}: {e}")
                continue

            print_space = alto_soup.find("PrintSpace")
            if not print_space:
                continue

            # Coordinate divisor for this page's measurement unit.
            divisor = measurement_divisor(alto_measurement_unit(alto_soup))

            # 1. Create page-level content item
            text_blocks = []
            for text_block in print_space.find_all("TextBlock"):
                block_type = text_block.get("TYPE", "")
                if block_type.lower() != "table":
                    text_blocks.append(text_block)

            # Resolve language: ALTO <Page language>, then issue-level MODS
            # language, then the conventional English fallback for US titles.
            page_tag = alto_soup.find("Page")
            page_lang = normalize_language(page_tag.get("language") if page_tag else None)
            page_lang = page_lang or self._issue_language or "en"

            page_ci_id = f"{self.id}-i{page_num_str}"
            page_ci = {
                "m": {
                    "id": page_ci_id,
                    "tp": "page",
                    "lg": page_lang,
                    "pp": [page.number],
                },
                "l": {
                    "id": f"{self.alias}-{page.filename}",
                    "parts": [
                        {
                            "comp_id": tb.get("ID"),
                            "comp_role": "body",
                            "comp_fileid": page.file_id,
                            "comp_page_no": page.number,
                        }
                        for tb in text_blocks
                        if tb.get("ID")
                    ],
                    "src_files": {
                        "mets_xml": os.path.basename(self.mets_file),
                        "alto_xml": page.filename,
                    },
                },
            }
            content_items.append(page_ci)

            # 2. Extract pictures: native <Illustration>, image ComposedBlocks,
            #    and standalone <GraphicalElement> (deduplicated, no nesting).
            for image_el in collect_image_elements(print_space):
                illus_id = image_el.get("ID")
                if not illus_id:
                    continue

                coords = None
                try:
                    coords = alto.distill_coordinates(image_el)
                    coords = [round(val / divisor) for val in coords]
                except Exception:
                    pass

                ci_counter += 1
                image_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"
                image_ci = {
                    "m": {
                        "id": image_ci_id,
                        "tp": "image",
                        "lg": None,
                        "pp": [page.number],
                    },
                    "l": {
                        "id": f"{self.alias}-{page.filename}",
                        "parts": [
                            {
                                "comp_id": illus_id,
                                "comp_role": "image",
                                "comp_fileid": page.file_id,
                                "comp_page_no": page.number,
                            }
                        ],
                        "src_files": {
                            "mets_xml": os.path.basename(self.mets_file),
                            "alto_xml": page.filename,
                        },
                    },
                }
                if coords:
                    image_ci["c"] = coords
                content_items.append(image_ci)

            # 3. Extract Tables
            for block in print_space.find_all("ComposedBlock"):
                block_type = block.get("TYPE", "")
                if block_type.lower() == "table":
                    table_id = block.get("ID")
                    if not table_id:
                        continue

                    coords = None
                    try:
                        coords = alto.distill_coordinates(block)
                        coords = [round(val / divisor) for val in coords]
                    except Exception:
                        pass

                    ci_counter += 1
                    table_ci_id = f"{self.id}-i{str(ci_counter).zfill(4)}"
                    table_ci = {
                        "m": {
                            "id": table_ci_id,
                            "tp": "table",
                            "lg": None,
                            "pp": [page.number],
                        },
                        "l": {
                            "id": f"{self.alias}-{page.filename}",
                            "parts": [
                                {
                                    "comp_id": table_id,
                                    "comp_role": "table",
                                    "comp_fileid": page.file_id,
                                    "comp_page_no": page.number,
                                }
                            ],
                            "src_files": {
                                "mets_xml": os.path.basename(self.mets_file),
                                "alto_xml": page.filename,
                            },
                        },
                    }
                    if coords:
                        table_ci["c"] = coords
                    content_items.append(table_ci)

        return content_items

    def _parse_mets(self) -> None:
        """Parse METS to construct the canonical issue representation."""
        mets_soup = self.xml

        # Issue-level language from MODS <languageTerm> (used as page-CI default).
        lang_tag = mets_soup.find("languageTerm")
        self._issue_language = normalize_language(lang_tag.text if lang_tag else None)

        # Title variant: prefer a MODS <title> element (namespace-agnostic),
        # else derive from the METS LABEL by stripping a trailing date.
        title = None
        title_tag = mets_soup.find(
            lambda t: getattr(t, "name", None) == "title" and t.get_text(strip=True)
        )
        if title_tag:
            title = title_tag.get_text(strip=True)
        else:
            mets_tag = mets_soup.find("mets")
            label = mets_tag.get("LABEL") if mets_tag else None
            if label:
                title = re.sub(r",\s*\d{4}(-\d{2}-\d{2})?\s*$", "", label).strip() or None

        # Extract content items
        content_items = self._find_content_items()

        # Compute reading order using project utils
        reading_order_dict = get_reading_order(content_items)
        for ci in content_items:
            ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]

        # Build issue_data dict
        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "olr": False,
            "i": content_items,
            "pp": [p.id for p in sorted(self.pages, key=lambda x: x.number)],
            "n": self._notes,
        }
        if title:
            self.issue_data["media_title_variant"] = title

        logger.info(f"Successfully parsed Chronicling America issue: {self.id}")
