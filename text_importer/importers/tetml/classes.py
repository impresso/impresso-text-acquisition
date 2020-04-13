"""Classes to handle the TETML OCR format."""

import logging
import os
from pathlib import Path
from time import strftime


import pandas as pd

from impresso_commons.path import IssueDir
from impresso_commons.path.path_fs import canonical_path

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.tetml.parsers import tetml_parser

logger = logging.getLogger(__name__)


class TetmlNewspaperPage(NewspaperPage):
    """Generic class representing a page in Tetml format.

    :param str _id: Canonical page ID.
    :param int n: Page number.
    :param dict page_content: nested article content of a single page
    :param str page_xml: Path to the Tetml file of the page.
    """

    def __init__(self, _id: str, n: int, page_content: dict, page_xml):
        super().__init__(_id, n)
        self.page_content = page_content
        self.page_data = None
        self.page_xml = page_xml
        self.archive = None

    def parse(self):

        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "cc": True,
            "r": self.page_content["r"],
        }

        if not self.page_data["r"]:
            logger.warning(f"Page {self.id} has no OCR text")

    def add_issue(self, issue: NewspaperIssue):
        if issue is None:
            raise ValueError(f"No NewspaperIssue for {self.id}")

        self.issue = issue


class TetmlNewspaperIssue(NewspaperIssue):
    """Class representing a newspaper issue in TETML format.

    Upon object initialization the following things happen:

    - the Zip archive containing the issue is uncompressed
    - the metadata file is parsed to determine the logical structure of the issue
    - page objects (instances of ``TetmlNewspaperPage``) are initialised.

    :param IssueDir issue_dir: Description of parameter `issue_dir`.

    """

    def __init__(self, issue_dir: IssueDir):
        super().__init__(issue_dir)

        logger.info(f"Starting to parse {self.id}")

        # get all tetml files of this issue
        self.files = self.index_issue_files()

        # parse metadata that contains additional article information
        self.df = self._parse_metadata()

        # parse the indexed files
        self.article_data = self.parse_articles()

        # using canonical ('m') and non-canonical ('meta') metadata
        self.content_items = [{"m": art["m"], "meta": art["meta"]} for art in self.article_data]

        # instantiate the individual pages
        self._find_pages()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "s": None,  # TODO: ignore style for the time being
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
            "ar": self.rights,
        }

        logger.info(f"Finished parsing {self.id}")

    def index_issue_files(self, suffix=".tetml"):
        """
        Index all files with a tetml suffix in the current issue directory
        """

        return sorted([str(path) for path in Path(self.path).rglob("*" + suffix)])

    def parse_articles(self):
        """
        Parse all articles of this issue
        """

        articles = []
        current_issue_page = 1  # start page of next article
        for i, fname in enumerate(self.files):
            try:
                data = tetml_parser(fname)

                # canonical identifier
                data["m"]["id"] = canonical_path(self.issuedir, name=f"i{i+1:04}", extension="")

                # reference to content item per region
                for page in data["pages"]:
                    for reg in page["r"]:
                        reg["pOf"] = data["m"]["id"]

                data["m"]["tp"] = "article"  # type attribute

                # attribute indicating the range of pages an article covers
                page_end = current_issue_page + data["meta"]["npages"]
                data["m"]["pp"] = list(range(current_issue_page, page_end))
                current_issue_page = page_end

                data = self.redefine_from_metadata(data)

                articles.append(data)

            except Exception as e:
                logger.error(f"Parsing of {fname} failed for {self.id}")
                raise e

        return articles

    def _find_pages(self):
        """
        Initialize all page objects per issue assuming that a particular page
        is only scanned once and not included in multpiple files.
        """

        for art in self.article_data:
            can_pages = art["m"]["pp"]
            try:
                if art["meta"]["pruned"]:
                    # omit the last page of pruned articles as it parsed with the subsequent one
                    can_pages = can_pages[:-1]
            except KeyError:
                pass

            for can_page, page_content in zip(can_pages, art["pages"]):
                can_id = f"{self.id}-p{can_page:04}"
                self.pages.append(
                    TetmlNewspaperPage(can_id, can_page, page_content, art["meta"]["tetml_path"])
                )

    def _parse_metadata(self, fname="metadata.tsv"):
        """
        Parse file with additional metadata
        """

        level_journal = self.path.split("/").index(self.journal)
        basedir = "/".join(self.path.split("/")[0: level_journal + 1])
        fpath = os.path.join(basedir, fname)

        try:
            df = pd.read_csv(
                fpath,
                sep="\t",
                parse_dates=["issue_date"],
                dtype={"article_docid": str},
                index_col="article_docid",
            )
            # discard rows from other issues as they are irrelevant
            date = pd.Timestamp(self.date)
            df = df[df["issue_date"] == date]

        except FileNotFoundError:
            raise FileNotFoundError(
                f"File with additional metadata needs to be placed in \
            the top newspaper directory and named {fname}"
            )

        return df

    def redefine_from_metadata(self, data):
        """
        Use additional metadata from file to set attributes
        """
        docid = data["meta"]["id"]
        data["m"]["t"] = self._lookup_article_title(docid)
        data["m"]["l"] = self._lookup_article_language(docid)
        data["m"]["pp"] = self._lookup_article_pages(docid)

        return data

    def _lookup_article_title(self, docid):
        """Use document identifier to lookup the actual title of an article"""

        return self.df.loc[docid, "article_title"]

    def _lookup_article_language(self, docid):
        """Use document identifier to lookup the language of an article"""

        return self.df.loc[docid, "volume_language"]

    def _lookup_article_pages(self, docid):
        """
        Use document identifier to lookup the page span of an article.
        The span reflects only full pages as part of an article,
        while the remainder gets assigned to the subsequent article.
        """

        page_first = self.df.loc[docid, "canonical_page_first"]
        page_last = self.df.loc[docid, "canonical_page_last"]

        return list(range(page_first, page_last + 1))
