"""Classes to handle the TETML OCR format."""

import logging
import os

from time import strftime
from collections import namedtuple
import pandas as pd

import regex

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp
from impresso_essentials.io.fs_utils import canonical_path

from text_preparation.importers.classes import CanonicalIssue
from text_preparation.importers.tetml import TetmlNewspaperIssue, TetmlNewspaperPage
from text_preparation.importers.tetml.parsers import tetml_parser
from text_preparation.importers.tetml.helpers import compute_bb

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"


class TokPosition(namedtuple("TokPosition", "art page reg para line tok")):
    """Create a an identifier to store the position of the fuzzy match.

    :param int art: Article number.
    :param int page: Page number.
    :param int reg: Region number.
    :param int para: Paragraph number.
    :param int line: Line number.
    :param int tok: Token number.
    """


class FedgazNewspaperPage(TetmlNewspaperPage):
    """
    Class representing a page in FedGaz TETML format.

    :param int n: Page number.
    :param dict page_content: Nested article content of a single page
    :param str page_xml: Path to the Tetml file of the page.
    """

    def parse(self):
        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            "cc": True,
            "iiif_img_base_uri": os.path.join(IIIF_ENDPOINT_URI, self.id),
            "r": self.page_content["r"],
        }

        if not self.page_data["r"]:
            logger.warning("Page %s has no OCR text", self.id)

        # TODO add img width & height


class FedgazNewspaperIssue(TetmlNewspaperIssue):
    """Class representing a issue in FedGaz TETML format.

    All functions defined in this child class are used to parse additional
    information specific for FedGaz and extend the generic TETML importer.

    Upon object initialization the following things happen:

    - index all the tetml documents of an issue
    - parse the metadata file to determine the logical structure of the issue
    - parse the tetml file that contains the actual content and some metadata
    - perform a heuristic article segmentation
    - redefine metadata and initialize page objects (instances of ``TetmlNewspaperPage``).

    :param IssueDir issue_dir: Newspaper issue with relevant information.

    """

    def __init__(self, issue_dir: IssueDir):
        CanonicalIssue.__init__(self, issue_dir)

        logger.info("Starting to parse %s", self.id)

        # get all tetml files of this issue
        self.files = self._index_issue_files()

        # parse metadata that contains additional article information
        self.df = self._parse_metadata()

        # parse the indexed files
        self.article_data = self.parse_articles()

        # recompose articles according to their logical boundaries
        self._heuristic_article_segmentation(candidates_only=True)

        # using canonical ('m') and additional non-canonical ('meta') metadata
        self.content_items = [{"m": art["m"], "meta": art["meta"]} for art in self.article_data]

        # instantiate the individual pages
        self._find_pages()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.NP.value,
            "sm": SourceMedium.PT.value,
            # "s": None,  # TODO: ignore style for the time being
            "i": self.content_items,
            "pp": [p.id for p in self.pages],
        }

        logger.info("Finished parsing %s", self.id)

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
                data["m"]["id"] = canonical_path(self.issuedir, suffix=f"i{i+1:04}")

                # reference to content item per region
                for page in data["pages"]:
                    for reg in page["r"]:
                        reg["pOf"] = data["m"]["id"]

                data["m"]["tp"] = "article"  # type attribute

                # attribute indicating the range of pages an article covers
                page_end = current_issue_page + data["meta"]["npages"]
                data["m"]["pp"] = list(range(current_issue_page, page_end))
                current_issue_page = page_end

                data = self._redefine_from_metadata(data)

                articles.append(data)

            except Exception as e:
                logger.error("Parsing of %s failed for %s", fname, self.id)
                raise e

        return articles

    def _find_pages(self):
        """
        Initialize all page objects per issue assuming that a particular page
        is only scanned once and not included in multpiple files.
        """

        for art in self.article_data:
            can_pages = art["m"]["pp"]
            # omit the last page of pruned articles as it parsed with the subsequent one
            try:
                if art["meta"]["pruned"]:
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

        level_journal = self.path.split("/").index(self.alias)
        basedir = "/".join(self.path.split("/")[0 : level_journal + 1])
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

        except FileNotFoundError as e:
            msg = (
                "File with additional metadata needs to be placed in "
                f"the top newspaper directory and named {fname}"
            )
            raise FileNotFoundError(msg) from e

        return df

    def _redefine_from_metadata(self, data):
        """
        Use additional metadata from file to set attributes
        """

        docid = data["meta"]["id"]
        data["m"]["t"] = self._lookup_article_title(docid)
        data["m"]["lg"] = self._lookup_article_language(docid)
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

    def _heuristic_article_segmentation(self, candidates_only: bool = True) -> None:
        """
        Find and set the actual article boundary with a fuzzy match search using
        the title of the next article.

        Without this recomposition articles may be pruned to their last full page
        due to a in-page segmentation of some articles.
        The remainder on the last page is assigned to the next article.

        :param bool candidates_only: Perform article segmentation only for predefined candidates.
        :return: None.
        """

        to_do_new_boundaries = []

        if candidates_only:
            overlapping = self._lookup_overlapping()
        else:
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
            fuzzy_cost = "{i<=" + str(max_insert) + ",1i+3d+3s<=" + str(max_cost_total * 3) + r"}"
            # fuzzy match article headline to locate (bestmatch flag)
            pattern = r"(?b)(" + title + r")" + fuzzy_cost

            try:
                match = regex.search(pattern, text)
                match_pos = match.start(1)
                # remap the match position to the position in the initial parsing
                match_tok_pos = text[:match_pos].count(" ")

                error_msg = (
                    f"Positive fuzzy match (sanity check):\n"
                    f'\ttitle: {art["m"]["t"]}\n'
                    f"\tpattern: {pattern}\n"
                    f"\tmatch: {match}"
                )

                logger.info(error_msg)

                # accumalate the boundaries
                to_do_new_boundaries.append(tok_pos[match_tok_pos])

            except AttributeError:
                error_msg = (
                    f"Error while searching for the logical boundary.\n"
                    f"The fuzzy match failed to match anything in the following article:\n"
                    f"{art['meta']}\n"
                    f"Pattern:\n{pattern}"
                )
                logger.error(error_msg)

                # page needs to be removed in any case regardless of matching,
                # otherwise the relation to the corresponding tif file is broken
                del self.article_data[i_art - 1]["pages"][-1]

        # do the actual reassigning
        for bound in to_do_new_boundaries:
            try:
                self._set_new_article_boundary(bound)
            except Exception as e:
                error_msg = (
                    f"Error while drawing a new article boundary at position {bound} "
                    f"in the issue {self.id}.\n Error: {e}"
                )
                logger.error(error_msg)

    def _set_new_article_boundary(self, bndry: TokPosition):
        """
        Set new article boundary and reassign the remainder from the subsequent
        to the previous article.

        :param TokPosition bndry: Specifies the actual boundary between subsequent articles
        :return: None.
        """

        # short notation for convenience
        art_page_reg = self.article_data[bndry.art]["pages"][bndry.page]["r"][bndry.reg]
        art_page = self.article_data[bndry.art]["pages"][bndry.page]

        # extract paragraphs, lines and tokens before in-page article boundary
        prev_para = art_page_reg["p"][: bndry.para]

        # TODO: we currently only reassign full paragraphs
        # prev_lines = art_page_reg["p"][bndry.para]["l"][: bndry.line]
        # prev_toks = art_page_reg["p"][bndry.para]["l"][bndry.line]["t"][: bndry.tok]

        # assemble extracted parts in a single paragraph
        subtree = {"p": []}
        if prev_para:
            subtree["p"].extend(prev_para)

            # TODO: we currently only reassign full paragraphs
            # if prev_lines:
            # subtree["p"].append({"l": prev_lines})
            # if prev_toks:
            # create new paragraph and line for tokens on the same line as headline
            # subtree["p"].append({"l": [{"t": prev_toks}]})

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
