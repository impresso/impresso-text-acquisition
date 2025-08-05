"""This module contains the definition of the Olive importer classes.

The classes define newspaper Issues and Pages objects which convert OCR
data in the Olive XML format to a unified canoncial format.
"""

import json
import logging
import os
from collections import deque
from time import strftime
from typing import Any
from zipfile import ZipFile

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp
from impresso_essentials.io.fs_utils import canonical_path

from text_preparation.importers.classes import CanonicalIssue, CanonicalPage, ZipArchive
from text_preparation.importers.olive.helpers import (
    combine_article_parts,
    convert_image_coordinates,
    convert_page_coordinates,
    get_clusters,
    recompose_page,
    recompose_ToC,
)
from text_preparation.importers.olive.parsers import (
    olive_image_parser,
    olive_parser,
    olive_toc_parser,
    parse_styles,
)

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"


class OliveNewspaperPage(CanonicalPage):
    """Newspaper page in Olive format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        toc_data (dict): Metadata about content items in the newspaper issue.
        page_info (dict): Metadata about the page image.
        page_xml (str): Path to the Olive XML file of the page.

    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (CanonicalIssue | None): Issue this page is from.
        toc_data (dict): Metadata about content items in the newspaper issue.
        image_info (dict): Metadata about the page image.
        page_xml (str): Path to the Olive XML file of the page.
        archive (ZipArchive): Archive of the issue this page is from.
    """

    def __init__(
        self, _id: str, number: int, toc_data: dict, image_info: dict, page_xml: str
    ) -> None:
        super().__init__(_id, number)
        self.toc_data = toc_data
        self.page_data = None
        self.image_info = image_info
        self.page_xml = page_xml
        self.archive = None

    def parse(self) -> None:
        """Process the page XML file and transform into canonical Page format.

        Note:
            This lazy behavior means that the page contents are not processed
            upon creation of the page object, but only once the ``parse()``
            method is called.

        Raises:
            ValueError: No Newspaper issue has been added to this page.
        """
        if self.issue is None:
            raise ValueError(f"No CanonicalIssue for {self.id}")

        element_ids = self.toc_data.keys()
        elements = {
            el["legacy"]["id"]: el
            for el in json.loads(self.issue.content_elements)
            if (el["legacy"]["id"] in element_ids)
        }

        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "r": [],
            "iiif_img_base_uri": os.path.join(IIIF_ENDPOINT_URI, self.id),
        }
        # TODO add page width & height

        recomposed_page = recompose_page(self.id, self.toc_data, elements, self.issue.clusters)
        self.page_data.update(recomposed_page)

        if len(self.page_data["r"]) == 0:
            logger.warning("Page %s has not OCR text", self.id)

        self._convert_page_coords()

        # if all(p.page_data is not None for p in self.issue.pages):
        #     # Means issue has been fully processed, can cleanup
        #     self.archive.cleanup()

    def _convert_page_coords(self) -> None:
        """Convert page coordinates to the desired iiif format if possible.

        The conversion is attempted if `self.image_info` is defined, otherwise
        the page is simply skipped.
        """
        self.page_data["cc"] = False
        if self.image_info is not None:
            try:
                box_strategy = self.image_info["strat"]
                image_name = self.image_info["s"]
                was_converted = convert_page_coordinates(
                    self.page_data,
                    self.archive.read(self.page_xml),
                    image_name,
                    self.archive,
                    box_strategy,
                    self.issue,
                )
                if was_converted:
                    self.page_data["cc"] = True
            except Exception as e:
                logger.error("Page %s raised error: %s", self.id, e)
                logger.error("Couldn't convert coordinates in p. %s", self.id)
        else:
            logger.debug("Image %s does not have image info", self.id)

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue
        self.archive = issue.archive


class OliveNewspaperIssue(CanonicalIssue):
    """Newspaper Issue in Olive format.

    Upon object initialization the following things happen:
        - The Zip archive containing the issue is uncompressed.
        - The ToC file is parsed to determine the logical structure of the issue.
        - Page objects (instances of ``OliveNewspaperPage``) are initialized.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.
        image_dirs (str): Path to the directory with the page images.
            Multiple paths should be separated by comma (",").
        temp_dir (str): Temporary directory to unpack ZipArchive objects.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``GDL-1900-01-02-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Newspaper unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`NewspaperPage` instances from this issue.
        image_dirs (str): Path to the directory with the page images.
            Multiple paths should be separated by comma (",").
        archive (ZipArchive): ZipArchive for this issue.
        toc_data (dict): Table of contents (ToC) data for this issue.
        content_elements (list[dict[str, Any]]): All content elements detected.
        content_items (list[dict[str, Any]]): Issue's recomposed content items.
        clusters (dict[str, list[str]]): Inverted index of legacy ids; values
            are clusters of articles, each indexed by one member.
    """

    def __init__(self, issue_dir: IssueDir, image_dirs: str, temp_dir: str):
        super().__init__(issue_dir)
        self.image_dirs = image_dirs
        logger.info("Starting to parse %s", self.id)

        # First parse the archive and return it
        self.archive = self._parse_archive(temp_dir)

        # Parse ToC
        self.toc_data = self._parse_toc()

        # Parse image xml files with olive_image_parser
        images = self._parse_image_xml_files()

        # Parse and recompose the ToC
        articles, self.content_elements = self._parse_articles()
        self.content_items = recompose_ToC(self.toc_data, articles, images)

        self.clusters = get_clusters(articles)

        # Work around to avoid non-pickle-able objects
        self.content_elements = json.dumps(self.content_elements)
        self._find_pages()

        styles = self._parse_styles_gallery()  # Then parse the styles

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "s": styles,
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
        }
        logger.info("Finished parsing %s", self.id)

    def _parse_archive(self, temp_dir: str, file: str = "Document.zip") -> ZipArchive:
        """Parse the archive containing the Olive OCR for this issue.

        Args:
            temp_dir (str): Temporary directory to unpack the archive.
            file (str, optional): Archive filename. Defaults to "Document.zip".

        Raises:
            ValueError: The `file` archive could not be opened
            ValueError: The `file` archive was not found.

        Returns:
            ZipArchive: :obj:`ZipArchive` of the `file` with its contents.
        """
        archive_path = os.path.join(self.path, file)
        if os.path.isfile(archive_path):
            archive_tmp_path = os.path.join(temp_dir, canonical_path(self.issuedir, as_dir=True))

            try:
                archive = ZipFile(archive_path)
                logger.debug("Contents of archive for %s: %s", self.id, archive.namelist())
                return ZipArchive(archive, archive_tmp_path)
            except Exception as e:
                msg = f"Bad Zipfile for {self.id}, failed with error : {e}"
                raise ValueError(msg) from e
        else:
            msg = f"Could not find archive {file} for {self.id}"
            raise ValueError(msg)

    def _get_page_xml_files(self) -> dict[int, str]:
        """Get all page XML files in `self.archive`, indexed by page number.

        Returns:
            dict[int, str]: Mapping from page number to XML file for each page.
        """
        page_xml = None
        if self.archive is not None:
            page_xml = {
                int(item.split("/")[0]): item
                for item in self.archive.namelist()
                if ".xml" in item and not item.startswith("._") and "/Pg" in item
            }

        return page_xml

    def _parse_toc(self, file: str = "TOC.xml") -> dict[int, dict[str, dict]]:
        """Parse the XML file containing the issue's ToC.

        For each page, the resulting parsed ToC contains a dict mapping legacy
        content item IDs to their metadata.

        Args:
            file (str, optional): Name of the ToC file. Defaults to "TOC.xml".

        Raises:
            FileNotFoundError: The ToC file was not found.
            e: The ToC file could not be read.

        Returns:
            dict[int, dict[str, dict]]: Parsed ToC dict
        """
        toc_path = os.path.join(self.path, file)
        try:
            toc_data = olive_toc_parser(toc_path, self.issuedir)
            logger.debug(toc_data)
        except FileNotFoundError as e:
            msg = f"Missing ToC.xml for {self.id}"
            raise FileNotFoundError(msg) from e
        except Exception as e:
            logger.error("Corrupted ToC.xml for %s", self.id)
            raise e
        return toc_data

    def _parse_image_xml_files(self) -> list[dict[str, str]]:
        """Find image XML files and extract the image metadata.

        Returns:
            list[dict[str, str]]: Metadata about all images in issue.
        """
        image_xml_files = [
            item
            for item in self.archive.namelist()
            if ".xml" in item and not item.startswith("._") and "/Pc" in item
        ]

        images = []
        for image_file in image_xml_files:
            try:
                image_data = olive_image_parser(self.archive.read(image_file))
                # because of course there are empty files!
                if image_data is not None:
                    images.append(image_data)
            except Exception as e:
                # there are e.g. image file with empty coordinate attributes
                msg = f"Parsing img file {image_file} in {self.id} failed"
                logger.error(msg)
                logger.error(e)
        return images

    def _parse_styles_gallery(self, file: str = "styleGallery.txt") -> list[dict[str, Any]]:
        """Parse the style file (plain text).

        Args:
            file (str, optional): Filename. Defaults to 'styleGallery.txt'.

        Returns:
            list[dict[str, Any]]: list of styles according to canonical format.
        """
        styles = []
        if file in self.archive.namelist():
            try:
                styles = parse_styles(self.archive.read(file).decode())
            except Exception as e:
                msg = f"Parsing styles file {file} for {self.id}, failed with error: {e}"
                logger.warning(msg)
        else:
            logger.warning("Could not find styles %s for %s", file, self.id)
        return styles

    def _parse_articles(self) -> tuple[list[dict], list[dict]]:
        """Parse the article and ad XML files for this issue.

        Content elements are article parts, which are then grouped and
        combined to create each article.

        Returns:
            tuple[list[dict], list[dict]]: Parsed articles & content elements.
        """
        articles = []
        content_elements = []
        counter = 0
        # recompose each article by following the continuation links
        article_parts = []
        items = sorted(
            [
                item
                for item in self.archive.namelist()
                if ".xml" in item and not item.startswith("._") and ("/Ar" in item or "/Ad" in item)
            ]
        )

        while len(items) > 0:
            counter += 1

            internal_deque = deque([items[0]])
            items = items[1:]

            while len(internal_deque) > 0:
                item = internal_deque.popleft()
                try:
                    xml_data = self.archive.read(item).decode("windows-1252")
                    new_data = olive_parser(xml_data)
                except Exception as e:
                    logger.error("Parsing of %s failed for %s", item, self.id)
                    raise e

                # check if it needs to be parsed later on
                if new_data["legacy"]["continuation_from"] is not None:
                    target = new_data["legacy"]["continuation_from"]
                    target = [x for x in items if target in x]
                    if len(target) > 0:
                        items.append(item)
                        continue

                article_parts.append(new_data)

                if new_data["legacy"]["continuation_to"] is not None:
                    next_id = new_data["legacy"]["continuation_to"]
                    next_id = [x for x in items if next_id in x][0]
                    internal_deque.append(next_id)
                    items.remove(next_id)

            try:
                content_elements += article_parts
                combined_article = combine_article_parts(article_parts)

                if combined_article is not None:
                    articles.append(combined_article)

                article_parts = []
            except Exception as e:
                raise e
        return articles, content_elements

    def _get_image_info(self) -> dict[str, Any]:
        """Read `image-info.json` file for a given issue.

        Go though all given image directories and only load the
        contents of the file corresponding to this issue.

        Raises:
            e: `image-info.json` file could not be decoded.
            ValueError: No `image-info.json` file was found

        Returns:
            dict[str, Any]: Contents of this issue's `image-info.json` file.
        """
        json_data = []
        for im_dir in self.image_dirs.split(","):
            issue_dir = os.path.join(
                im_dir, self.alias, str(self.date).replace("-", "/"), self.edition
            )

            issue_w_images = IssueDir(
                alias=self.alias,
                date=self.date,
                edition=self.edition,
                path=issue_dir,
            )

            image_info_name = canonical_path(issue_w_images, suffix="image-info", extension=".json")

            image_info_path = os.path.join(issue_w_images.path, image_info_name)

            if os.path.exists(image_info_path):
                with open(image_info_path, "r", encoding="utf-8") as inp_file:
                    try:
                        json_data = json.load(inp_file)
                        if len(json_data) == 0:
                            msg = f"Empty image info for {self.id} at {image_info_path}"
                            logger.debug(msg)
                        else:
                            return json_data
                    except Exception as e:
                        logger.error("Decoding file %s failed with '%s'", image_info_path, e)
                        raise e
        if len(json_data) == 0:
            raise ValueError(f"Could not find image info for {self.id}")

    def _find_pages(self) -> None:
        """Find page XML files and initialize page objects.

        Raises:
            ValueError: No page XML file was found.
        """
        if self.toc_data is not None:
            image_info = self._get_image_info()
            pages_xml = self._get_page_xml_files()
            for page_n, data in self.toc_data.items():
                can_id = f"{self.id}-p{str(page_n).zfill(4)}"
                image_info_records = [p for p in image_info if int(p["pg"]) == page_n]

                if len(image_info_records) == 0:
                    image_info_record = None
                else:
                    image_info_record = image_info_records[0]

                try:
                    page_xml = pages_xml[page_n]
                except Exception as e:
                    raise ValueError(f"Could not find page xml for {can_id}") from e

                self._convert_images(image_info_record, page_n, page_xml)

                self.pages.append(
                    OliveNewspaperPage(can_id, page_n, data, image_info_record, page_xml)
                )

    def _convert_images(
        self, image_info_record: dict[str, Any], page_n: int, page_xml: dict[int, str]
    ) -> None:
        """Convert a page's image information to canonical format.

        The coordinates are also converted to a iiif-compliant format.

        Args:
            image_info_record (dict[str, Any]): Page's images information.
            page_n (int): Corresponding page number.
            page_xml (dict[int, str]): Parsed contents of the page's XML.
        """
        if image_info_record is not None:
            box_strategy = image_info_record["strat"]
            image_name = image_info_record["s"]
            images_in_page = [
                content_item
                for content_item in self.content_items
                if content_item["m"]["tp"] == "picture" and page_n in content_item["m"]["pp"]
            ]

            for image in images_in_page:
                image = convert_image_coordinates(
                    image,
                    self.archive.read(page_xml),
                    image_name,
                    self.archive,
                    box_strategy,
                    self.issuedir,
                )
                image["m"]["tp"] = "image"
