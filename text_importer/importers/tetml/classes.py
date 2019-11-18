"""Classes to handle the TETML OCR format."""

import logging
import os
from pathlib import Path
from time import strftime
from collections import namedtuple


import pandas as pd
import regex

from impresso_commons.path import IssueDir
from impresso_commons.path.path_fs import canonical_path

from text_importer.importers.classes import NewspaperIssue, NewspaperPage

from text_importer.importers.tetml.parsers import tetml_parser


from text_importer.importers.tetml.helpers import compute_bb

logger = logging.getLogger(__name__)

IMPRESSO_IIIF_BASEURI = "https://impresso-project.ch/api/proxy/iiif/"

TokPosition = namedtuple("TokPosition", "art page reg para line tok")


class TetmlNewspaperPage(NewspaperPage):
    """
    Class representing a page in Tetml format.

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
        if self.issue is None:
            raise ValueError(f"No NewspaperIssue for {self.id}")

        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "cc": True,
            "iiif": os.path.join(IMPRESSO_IIIF_BASEURI, self.id),
            "r": [reg for reg in self.page_content["r"]],
        }

        if not self.page_data["r"]:
            logger.warning(f"Page {self.id} has no OCR text")

    def add_issue(self, issue: NewspaperIssue):
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

        # recompose articles according to their logical boundaries
        overlapping = self._lookup_overlapping()
        self._find_logical_article_boundary(overlapping)

        # using canonical ('m') and non-canonical ('meta') metadata
        self.content_items = [
            {"m": art["m"], "meta": art["meta"]} for art in self.article_data
        ]

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
                data["m"]["id"] = canonical_path(
                    self.issuedir, name=f"i{i+1:04}", extension=""
                )

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
                    TetmlNewspaperPage(
                        can_id, can_page, page_content, art["meta"]["tetml_path"]
                    )
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

    def _lookup_overlapping(self):
        return self.df[self.df.pruned == True].index.values

    def _find_logical_article_boundary(self, overlapping: list = None):
        """
        Find and set the actual article boundary with a fuzzy match search using
        the title of the next article.

        Without this recomposition articles may be pruned to their last full page
        due to a in-page segmentation of some articles.
        The remainder on the last page is assigned to the next article.

        :param list overlapping_articles: Overlapping articles for which the boundary needs to be redefined.
        :return: None.
        """

        to_do_new_boundaries = []

        if overlapping is None:
            # consider all articles as potentially overlapping
            overlapping = self.df.index.values

        for i_art, art in enumerate(self.article_data[:-1]):
            # only proceed if last article overlaps with the current
            if self.article_data[i_art - 1]["meta"]["id"] in overlapping:
                self.article_data[i_art - 1]["meta"]["pruned"] = True
            else:
                self.article_data[i_art - 1]["meta"]["pruned"] = False
                continue

            tok_counter = 0
            tok_pos = {}  # store exact position of token
            tokens = []

            for i_page, page in enumerate(art["pages"]):
                for i_region, region in enumerate(page["r"]):
                    for i_para, para in enumerate(region["p"]):
                        for i_line, line in enumerate(para["l"]):
                            for i_tok, tok in enumerate(line["t"]):
                                tokens.append(tok["tx"])
                                tok_pos[tok_counter] = TokPosition(
                                    i_art, i_page, i_region, i_para, i_line, i_tok
                                )
                                tok_counter += 1
                # restrict search space of finding the actual boundary to first page
                continue

            text = " ".join(tokens).lower()

            # lowercase title and shorten long titles to 30 characters at most
            title = str(art["m"]["t"]).lower()[:30]

            # mask potential parenthesis
            title = title.replace("(", r"\(").replace(")", r"\)")

            # max corrections as a function of length
            max_cost_total = max(2, int(0.2 * len(title)))
            max_insert = int(0.3 * len(title))
            # scaled by 3 to make insertions very cheap to account for bad OCR
            fuzzy_cost = (
                "{i<="
                + str(max_insert)
                + ",1i+3d+3s<="
                + str(max_cost_total * 3)
                + r"}"
            )
            # fuzzy match article headline to locate (bestmatch flag)
            pattern = r"(?b)(" + title + r")" + fuzzy_cost

            try:
                match = regex.search(pattern, text)
                match_pos = match.start(1)
                # remap the match position to the position in the initial parsing
                match_tok_pos = text[:match_pos].count(" ")

                error_msg = (f'Positive fuzzy match (sanity check):\n' +
                       f'\ttitle: {art["m"]["t"]}\n' +
                       f'\tpattern: {pattern}\n' +
                       f'\tmatch: {match}')

                logger.info(error_msg)

                # accumalate the boundaries
                to_do_new_boundaries.append(tok_pos[match_tok_pos])

            except AttributeError:
                error_msg = (f"Error while searching for the logical boundary.\n" +
                       f"The fuzzy match failed to match anything in the following article:\n" +
                       f"{art['meta']}\n" +
                       f"Pattern:\n{pattern}")
                logger.error(error_msg)

                # page needs to be removed in any case regardless of matching,
                # otherwise the relation to the corresponding tif file is broken
                del self.article_data[i_art - 1]["pages"][-1]

        # do the actual reassigning
        for bound in to_do_new_boundaries:
            try:
                self._set_new_article_boundary(bound)
            except Exception as e:
                error_msg = (f"Error while setting new article boundary at position {bound} of the issue {self.id}.\n" +
                f"Error: {e}")
                logger.error(error_msg)


    def _set_new_article_boundary(self, bndry: TokPosition):
        """
        Set new article boundary and reassign the remainder from the subsequent
        to the previous article.

        :param TokPosition boundary: Specifies the actual boundary between subsequent articles
        :return: None.
        """

        # short notation for convenience
        art_page_reg = self.article_data[bndry.art]["pages"][bndry.page]["r"][bndry.reg]
        art_page = self.article_data[bndry.art]["pages"][bndry.page]

        # extract paragraphs, lines and tokens before in-page article boundary
        prev_para = art_page_reg["p"][: bndry.para]
        """
        prev_lines = art_page_reg["p"][bndry.para]["l"][: bndry.line]
        prev_toks = art_page_reg["p"][bndry.para]["l"][bndry.line]["t"][: bndry.tok]
        """
        # assemble extracted parts in a single paragraph
        subtree = {"p": []}
        if prev_para:
            subtree["p"].extend(prev_para)

            # TODO: we currently only reassign full paragraphs
            """
            if prev_lines:
                # subtree["p"].append({"l": prev_lines})
            if prev_toks:
                # create new paragraph and line for tokens on the same line as headline
                # subtree["p"].append({"l": [{"t": prev_toks}]})
            """
            # remove the remainings (i.e. subtree) from the subsequent article
            # del art_page_reg["p"][bndry.para]["l"][bndry.line]["t"][: bndry.tok]
            # del art_page_reg["p"][bndry.para]["l"][: bndry.line]
            del art_page_reg["p"][: bndry.para]

            # insert subtree as a separate region at the beginning of the page where
            # the subsequent article starts
            art_page["r"].insert(0, subtree)

            # replace the reference of the reassigned region to the new content item
            art_page["r"][0]["pOf"] = self.article_data[bndry.art - 1]["m"]["id"]

            # redefine coordinates of region belonging to previous article
            paras_coord_prev = [para["c"] for para in art_page["r"][0]["p"]]
            art_page["r"][0]["c"] = compute_bb(paras_coord_prev)

            # redefine coordinates of region belonging to subsequent article
            paras_coord_sub = [para["c"] for para in art_page["r"][1]["p"]]
            art_page["r"][1]["c"] = compute_bb(paras_coord_sub)

        # remove last page of previous article that is identical
        # with the first page of the subsequent one
        del self.article_data[bndry.art - 1]["pages"][-1]
