import sys
import traceback
import datetime
import json
import pathlib
import logging
import os
import shutil
import signal
from typing import Any, Optional
import git

import dask.bag as db
import jsonlines
from dask.distributed import Client, progress
from docopt import docopt
from smart_open import smart_open

from impresso_essentials.io.fs_utils import parse_canonical_filename
from impresso_essentials.io.s3 import read_s3_issues, get_s3_resource
from impresso_essentials.utils import Timer, timestamp

from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.versioning.aggregators import compute_stats_in_rebuilt_bag

from text_preparation.rebuilders.helpers import (
    read_issue_supports,
    rejoin_articles,
    reconstruct_iiif_link,
    insert_whitespace,
    _article_has_problem,
    _article_without_problem,
    rebuild_for_passim,
    TYPE_MAPPINGS,
)

logger = logging.getLogger(__name__)


def rebuild_paper_text(
    page: list[dict], language: Optional[str], string: Optional[str] = None
) -> tuple[str, dict[list], dict[list]]:
    """Rebuild the text of an article for Solr ingestion.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, dict[list], dict[list]]: [0] Article fulltext, [1] offsets and
            [2] coordinates of token regions.
    """

    coordinates = {"regions": [], "tokens": []}

    offsets = {"line": [], "para": [], "region": []}

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)
    for region in page:

        if len(string) > 0:
            offsets["region"].append(len(string))

        coordinates["regions"].append(region["c"])

        for para in region["p"]:

            if len(string) > 0:
                offsets["para"].append(len(string))

            for line in para["l"]:

                for n, token in enumerate(line["t"]):
                    region = {}
                    if "c" not in token:
                        print(f"'c' was not present in token: {token}, line: {line}")
                        continue
                    region["c"] = token["c"]
                    region["s"] = len(string)

                    token_text = None
                    if "hy" in token:
                        region["l"] = len(token["tx"][:-1]) - 1
                        region["hy1"] = True
                    elif "nf" in token:
                        region["l"] = len(token["nf"])
                        region["hy2"] = True

                        token_text = token["nf"] if token["nf"] is not None else ""
                    else:
                        if token["tx"]:
                            region["l"] = len(token["tx"])
                        else:
                            region["l"] = 0

                        token_text = token["tx"] if token["tx"] is not None else ""

                    # don't add the tokens corresponding to the first part of a hyphenated word
                    if "hy" not in token:
                        next_token = (
                            line["t"][n + 1]["tx"] if n != len(line["t"]) - 1 else None
                        )
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=next_token,
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        string += f"{token_text} " if ws else f"{token_text}"

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        if "hy" in token:
                            offsets["line"].append(region["s"])
                        else:
                            token_length = len(token["tx"]) if token["tx"] else 0
                            offsets["line"].append(region["s"] + token_length)

                    coordinates["tokens"].append(region)

    return (string, coordinates, offsets)


def rebuild_paper_text_passim(
    page: list[dict], language: Optional[str], string: Optional[str] = None
) -> tuple[str, list[dict]]:
    """The text rebuilding function from pages for passim.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, list[dict]]: [0] article fulltext, [1] coordinates of token regions.
    """

    regions = []

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)

    for region in page:

        for para in region["p"]:

            for line in para["l"]:

                for n, token in enumerate(line["t"]):

                    region_string = ""

                    if "c" not in token:
                        # if the coordniates are missing, they should be skipped
                        logger.debug("Missing 'c' in token %s", token)
                        print(f"Missing 'c' in token {token}")
                        continue

                    # each page region is a token
                    output_region = {
                        "start": None,
                        "length": None,
                        "coords": {
                            "x": token["c"][0],
                            "y": token["c"][1],
                            "w": token["c"][2],
                            "h": token["c"][3],
                        },
                    }

                    if len(string) == 0:
                        output_region["start"] = 0
                    else:
                        output_region["start"] = len(string)

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        tmp = f"{token['tx']}\n"
                        region_string += tmp
                    else:
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=line["t"][n + 1]["tx"],
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        region_string += f"{token['tx']} " if ws else f"{token['tx']}"

                    string += region_string
                    output_region["length"] = len(region_string)
                    regions.append(output_region)

    return (string, regions)


def rebuild_paper_for_solr(content_item: dict[str, Any]) -> dict[str, Any]:
    """Rebuilds the text of an article content-item given its metadata as input.

    Note:
        This rebuild function is thought especially for ingesting the newspaper
        data into our Solr index.

    Args:
        content_item (dict[str, Any]): The content-item to rebuilt using its metadata.

    Returns:
        dict[str, Any]: The rebuilt content-item following the Impresso JSON Schema.
    """
    t = Timer()

    article_id = content_item["m"]["id"]
    logger.debug("Started rebuilding article %s", article_id)

    issue_id = "-".join(article_id.split("-")[:-1])

    page_file_names = {
        p: f"{issue_id}-p{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]
    }

    year, month, day, _, ci_num = article_id.split("-")[1:]
    d = datetime.date(int(year), int(month), int(day))

    if content_item["m"]["tp"] in TYPE_MAPPINGS:
        mapped_type = TYPE_MAPPINGS[content_item["m"]["tp"]]
    else:
        mapped_type = content_item["m"]["tp"]

    fulltext = ""
    linebreaks = []
    parabreaks = []
    regionbreaks = []

    article_lang = content_item["m"]["l"] if "l" in content_item["m"] else None

    # if the reading order is not defined, use the number associated to each CI
    reading_order = content_item["m"]["ro"] if "ro" in content_item["m"] else int(ci_num[1:])

    article = {
        "id": article_id,
        "pp": content_item["m"]["pp"],
        "d": d.isoformat(),
        "olr": False if mapped_type is None else True,
        "ts": timestamp(),
        "lg": article_lang,
        "tp": mapped_type,
        "ro": reading_order,
        "ppreb": [],
        "lb": [],
        "cc": content_item["m"]["cc"],
    }

    if mapped_type == "img":
        article["iiif_link"] = reconstruct_iiif_link(content_item)

    if "t" in content_item["m"]:
        article["t"] = content_item["m"]["t"]

    if mapped_type != "img":
        for n, page_no in enumerate(article["pp"]):

            page = content_item["pprr"][n]

            if fulltext == "":
                fulltext, coords, offsets = rebuild_paper_text(page, article_lang)
            else:
                fulltext, coords, offsets = rebuild_paper_text(page, article_lang, fulltext)

            linebreaks += offsets["line"]
            parabreaks += offsets["para"]
            regionbreaks += offsets["region"]

            page_doc = {
                "id": page_file_names[page_no].replace(".json", ""),
                "n": page_no,
                "t": coords["tokens"],
                # todo add paragraphs?
                "r": coords["regions"],
            }
            article["ppreb"].append(page_doc)
        article["lb"] = linebreaks
        article["pb"] = parabreaks
        article["rb"] = regionbreaks
        logger.debug("Done rebuilding article %s (Took %s)", article_id, t.stop())
        article["ft"] = fulltext

    return article


def recompose_paper_fulltext(content_item, passim_document, page_file_names):
    fulltext = ""
    for n, page_no in enumerate(content_item["m"]["pp"]):

        page = content_item["pprr"][n]

        if fulltext == "":
            fulltext, regions = rebuild_paper_text_passim(page, passim_document["lg"])
        else:
            fulltext, regions = rebuild_paper_text_passim(
                page, passim_document["lg"], fulltext
            )

        page_doc = {
            "id": page_file_names[page_no].replace(".json", ""),
            "seq": page_no,
            "regions": regions,
        }
        passim_document["pages"].append(page_doc)

    passim_document["text"] = fulltext

    return passim_document


def rebuild_paper_for_passim(content_item: dict[str, Any]) -> dict[str, Any]:
    """Rebuilds the text of an article content-item to be used with passim.

    Args:
        content_item (dict[str, Any]): The content-item to rebuilt using its metadata.

    Returns:
        dict[str, Any]: The rebuilt content-item built for passim.
    """
    alias, date, _, _, _, _ = parse_canonical_filename(content_item["m"]["id"])

    article_id = content_item["m"]["id"]
    logger.debug("Started rebuilding article %s", article_id)
    issue_id = "-".join(article_id.split("-")[:-1])

    page_file_names = {
        p: f"{issue_id}-p{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]
    }

    article_lang = content_item["m"]["l"] if "l" in content_item["m"] else None

    if content_item["m"]["tp"] in TYPE_MAPPINGS:
        mapped_type = TYPE_MAPPINGS[content_item["m"]["tp"]]
    else:
        mapped_type = content_item["m"]["tp"]

    passim_document = {
        "series": alias,
        "date": f"{date[0]}-{date[1]}-{date[2]}",
        "id": content_item["m"]["id"],
        "cc": content_item["m"]["cc"],
        "tp": mapped_type,
        "lg": article_lang,
        "pages": [],
    }

    if "t" in content_item["m"]:
        passim_document["title"] = content_item["m"]["t"]

    fulltext = ""
    for n, page_no in enumerate(content_item["m"]["pp"]):

        page = content_item["pprr"][n]

        if fulltext == "":
            fulltext, regions = rebuild_paper_text_passim(page, article_lang)
        else:
            fulltext, regions = rebuild_paper_text_passim(page, article_lang, fulltext)

        page_doc = {
            "id": page_file_names[page_no].replace(".json", ""),
            "seq": page_no,
            "regions": regions,
        }
        passim_document["pages"].append(page_doc)

    passim_document["text"] = fulltext

    return passim_document


def filter_and_process_paper_cis(issues_bag, input_bucket, issue_type, issue_medium, _format):
    # Process the issues into rebuilt CIs for "paper" issues
    # ie. data for which the source medium is "print" or "typescript"

    # issues_bag contains pairs of issueDir objects and their corresponding json canonical issue

    # determine which rebuild function to apply
    if _format == "solr":
        rebuild_function = rebuild_paper_for_solr
    elif _format == "passim":
        rebuild_function = rebuild_for_passim
    else:
        raise NotImplementedError

    faulty_issues = (
        issues_bag.filter(lambda i: len(i[1]["pp"]) == 0)
        .map(lambda i: i[1])
        .pluck("id")
        .compute()
    )
    msg = f"Issues with no pages (will be skipped): {faulty_issues}"
    logger.debug(msg)
    print(msg)
    del faulty_issues
    msg = f"Number of partitions: {issues_bag.npartitions}"
    logger.debug(msg)
    print(msg)

    articles_bag = (
        issues_bag.filter(lambda i: len(i[1]["pp"]) > 0)
        .starmap(read_issue_supports, pages=True, bucket=input_bucket)
        .starmap(rejoin_articles)
        .flatten()
        .persist()
    )

    faulty_articles_n = (
        articles_bag.filter(_article_has_problem).pluck("m").pluck("id").compute()
    )
    msg = f"Skipped articles: {faulty_articles_n}"
    logger.debug(msg)
    print(msg)
    del faulty_articles_n

    articles_bag = (
        articles_bag.filter(_article_without_problem).map(rebuild_function).persist()
    )

    return articles_bag
