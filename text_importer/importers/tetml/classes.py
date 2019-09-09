
"""Classes to handle the TETML OCR format."""

import logging
import os
from time import strftime

from impresso_commons.path import IssueDir
from impresso_commons.path.path_fs import canonical_path

from text_importer.importers.classes import NewspaperIssue, NewspaperPage

from text_importer.importers.tetml.parsers import tetml_parser

from text_importer.utils import get_issue_schema, get_page_schema

logger = logging.getLogger(__name__)
IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

IMPRESSO_IIIF_BASEURI = "https://impresso-project.ch/api/proxy/iiif/"


class TetmlNewspaperPage(NewspaperPage):
    """Class representing a page in Tetml format.

    :param str _id: Canonical page ID.
    :param int n: Page number.
    :param dict toc_data: Metadata about content items in the newspaper issue.
    :param dict image_info: Metadata about the page image.
    :param str page_xml: Path to the Olive XML file of the page.

    """

    def __init__(self, _id: str, n: int, article_data: dict, page_xml):
        super().__init__(_id, n)
        self.article_data = article_data
        self.page_data = None
        # self.image_info = image_info
        self.page_xml = page_xml
        self.archive = None

    def parse(self):
        if self.issue is None:
            raise ValueError(f"No NewspaperIssue for {self.id}")

        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "cc": True,
            "iiif": os.path.join(IMPRESSO_IIIF_BASEURI, self.id),

            "r": self.article_data["r"]

        }


        if not self.page_data["r"]:
            logger.warning(f"Page {self.id} has not OCR text")

        # content item to which page region belongs
        self.page_data["r"]["pOf"]: self.article_data['m']['id']

        # self._convert_page_coords() # # TODO: conversion can be parallelized when trigggered here

    def add_issue(self, issue: NewspaperIssue):
        self.issue = issue
        self.archive = issue.archive


class TetmlNewspaperIssue(NewspaperIssue):
    """Class representing a newspaper issue in TETML format.

    Upon object initialization the following things happen:

    - the Zip archive containing the issue is uncompressed
    - the ToC file is parsed to determine the logical structure of the issue
    - page objects (instances of ``TetmlNewspaperPage``) are initialised.

    :param IssueDir issue_dir: Description of parameter `issue_dir`.
    :param str image_dirs: Path to the directory with the page images. Multiple
        paths should be separated by comma (",").
    :param str temp_dir: Description of parameter `temp_dir`.

    """

    def __init__(self, issue_dir: IssueDir, image_dirs: str, temp_dir: str):
        super().__init__(issue_dir)
        """
        self.id = canonical_path(issue_dir, path_type='dir').replace('/', '-')
        self.edition = issue_dir.edition
        self.journal = issue_dir.journal
        self.path = issue_dir.path
        self.date = issue_dir.date
        self.issue_data = {}
        self._notes = []
        self.pages = []
        self.rights = issue_dir.rights
        """
        # self.image_dirs = image_dirs
        logger.info(f"Starting to parse {self.id}")

        """
        # Parse ToC
        self.toc_data = self._parse_toc()

        # Parse and recompose the ToC
        articles, self.content_elements = self._parse_articles()
        self.content_items = recompose_ToC(self.toc_data, articles, images)

        # Work around to avoid non-pickle-able objects
        self.content_elements = json.dumps(self.content_elements)



        i -> content item
        m -> meta data

        i: [
            "m":{
                "id":"GDL-1900-01-02-a-i0002",
                "l":"fr",
                "pp":[
                   1,2,3,4
                ],
                "t":"LES JUSTES",
                "tp":"article"
        ]
        """

        # get all tetml files of this issue
        self.files = self.index_issue_files()

        # parse the indexed files
        self.article_data = self.parse_articles()

        self.content_items = [art['m'] for art in self.article_data]

        # instantiate the individual pages
        self.get_pages()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "s": None,  # TODO: ignore style for the time being
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "ar": None,  # TODO: ignore access rights for the time being
        }
        logger.info(f"Finished parsing {self.id}")

    def index_issue_files(self, suffix=".tetml"):
        """
        Index all files with a tetml suffix of this issue
        """
        issue_files = []
        dir_path, dirs, files = next(os.walk(self.path))

        for file in files:
            if file.endswith(suffix):
                file_path = os.path.join(self.path, file)
                issue_files.append(file_path)

        return issue_files

    def parse_articles(self):
        """
        Parse all articles of the issue
        """

        articles = []
        current_issue_page = 1 # start page of next article
        for i, fname in enumerate(self.files):
            try:
                with open(fname, mode="r") as article:
                    data = tetml_parser(article)
                    # canonical identifier
                    data['m']['id'] = canonical_path(self.path, name=f"i{str(i).zfill(4)}", extension="")
                    data['m']['tp']= 'article' # type attribute

                    # attribute indicating the range of pages an article covers
                    page_end = current_issue_page + data['meta']['npages']
                    data['m']['pp'] = list(range(current_issue_page, page_end + 1))
                    current_issue_page = page_end + 2

                    articles.append(data)

            except Exception as e:
                logger.error(f"Parsing of {fname} failed for {self.id}")
                raise e

        return articles


    def get_pages(self):
        """
        Initialize all page objects per issue,
        assuming that no identical pages are scanned/included across files
        """

        for art in self.article_data:
            can_id = "{}-p{}".format(self.id, str(art['npages']).zfill(4))

            self.pages.append(
                TetmlNewspaperPage(
                    can_id, art['npages'], art, art['tetml_path']
                )
            )

    def _parse_toc(self, file="metadata.tsv"):
        """
            Parse table of content (toc) get title and logical segmentation

            change metadata:
                logical_id
                logical_page_first
                logical_page_end
            """
