"""Helper functions for the text `rebuilder.py` script."""

import json
import logging
import os
from typing import Any, Optional, Union

from impresso_commons.utils.s3 import (
    IMPRESSO_STORAGEOPT,
    alternative_read_text,
    get_s3_resource,
    get_s3_versions,
)

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_BASE_2_SUFFIX = {
    # suffix for SWA data
    "https://ub-sipi.ub.unibas.ch/impresso": "max/0/default.jpg",
    # suffix for BCUL data, sometimes 300 can be replaced by larger values
    "https://scriptorium.bcu-lausanne.ch/api": "300,/0/default.jpg",
}

WHITESPACE_RULES = {
    "fr": {
        "pct_no_ws_before": [".", ",", ")", "]", "}", "°", "...", ".-", "%"],
        "pct_no_ws_after": ["(", "[", "{"],
        "pct_no_ws_before_after": ["'", "-"],
        "pct_number": [".", ","],
    },
    "de": {
        "pct_no_ws_before": [
            ".",
            ",",
            ")",
            "]",
            "}",
            "°",
            "...",
            "?",
            "!",
            ":",
            ";",
            ".-",
            "%",
        ],
        "pct_no_ws_after": ["(", "[", "{"],
        "pct_no_ws_before_after": ["'", "-"],
        "pct_number": [".", ","],
    },
    "other": {
        "pct_no_ws_before": [
            ".",
            ",",
            ")",
            "]",
            "}",
            "°",
            "...",
            "?",
            "!",
            ":",
            ";",
            ".-",
            "%",
        ],
        "pct_no_ws_after": ["(", "[", "{"],
        "pct_no_ws_before_after": ["'", "-"],
        "pct_number": [".", ","],
    },
}


def read_issue(issue, bucket_name, s3_client=None):
    """Read the data from S3 for a given newspaper issue.

    NB: It injects the s3_version into the returned object.

    :param issue: input issue
    :type issue: `IssueDir`
    :param bucket_name: bucket's name
    :type bucket_name: str
    :param s3_client: open connection to S3 storage
    :type s3_client: `boto3.resources.factory.s3.ServiceResource`
    :return: a JSON representation of the issue object
    """
    if s3_client is None:
        s3_client = get_s3_resource()

    content_object = s3_client.Object(bucket_name, issue.path)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    issue_json = json.loads(file_content)
    issue_json["s3_version"] = get_s3_versions(bucket_name, issue.path)[0][0]
    logger.info("Read JSON of %s", issue)
    return (issue, issue_json)


def read_page(page_key, bucket_name, s3_client):
    """Read the data from S3 for a given newspaper pages."""

    try:
        content_object = s3_client.Object(bucket_name, page_key)
        file_content = content_object.get()["Body"].read().decode("utf-8")
        page_json = json.loads(file_content)
        page_json["s3v"] = get_s3_versions(bucket_name, page_key)[0][0]
        logger.info("Read page %s from bucket %s", page_key, bucket_name)
        return page_json
    except Exception as e:
        logger.error("There was a problem reading %s: %s", page_key, e)
        return None


def read_issue_pages(issue, issue_json, bucket=None):
    """Read all pages of a given issue from S3 in parallel."""
    newspaper = issue.journal
    year = issue.date.year

    filename = (
        f"{bucket}/{newspaper}/pages/{newspaper}-{year}"
        f"/{issue_json['id']}-pages.jsonl.bz2"
    )

    pages = [
        json.loads(page)
        for page in alternative_read_text(filename, IMPRESSO_STORAGEOPT)
    ]

    print(filename)
    issue_json["pp"] = pages
    del pages
    return (issue, issue_json)


def rejoin_articles(issue, issue_json):
    print(f"Rejoining pages for issue {issue.path}")
    articles = []
    for article in issue_json["i"]:

        art_id = article["m"]["id"]
        article["m"]["s3v"] = issue_json["s3_version"]
        article["has_problem"] = False
        article["m"]["pp"] = sorted(list(set(article["m"]["pp"])))

        pages = []
        page_ids = [page["id"] for page in issue_json["pp"]]
        for page_no in article["m"]["pp"]:
            # given a page  number (from issue.json) and its canonical ID
            # find the position of that page in the array of pages (with text
            # regions)
            page_no_string = f"p{str(page_no).zfill(4)}"
            try:
                page_idx = [
                    n
                    for n, page in enumerate(issue_json["pp"])
                    if page_no_string in page["id"]
                ][0]
                pages.append(issue_json["pp"][page_idx])
            except IndexError:
                article["has_problem"] = True
                articles.append(article)
                logger.error(
                    "Page %s not found for item %s. Issue %s has pages %s",
                    page_no_string,
                    art_id,
                    issue_json["id"],
                    page_ids,
                )
                continue

        regions_by_page = []
        for page in pages:
            regions_by_page.append(
                [
                    region
                    for region in page["r"]
                    if "pOf" in region and region["pOf"] == art_id
                ]
            )
        article["pprr"] = regions_by_page
        try:
            convert_coords = [p["cc"] for p in pages]
            article["m"]["cc"] = sum(convert_coords) / len(convert_coords) == 1.0
        except Exception:
            # it just means there was no CC field in the pages
            article["m"]["cc"] = None

        articles.append(article)
    return articles


def pages_to_article(article, pages):
    """Return all text regions belonging to a given article."""
    try:
        art_id = article["m"]["id"]
        print("Extracting text regions for article %s", art_id)
        regions_by_page = []
        for page in pages:
            regions_by_page.append(
                [region for region in page["r"] if region["pOf"] == art_id]
            )
        convert_coords = [page["cc"] for page in pages]
        article["m"]["cc"] = sum(convert_coords) / len(convert_coords) == 1.0
        article["has_problem"] = False
        article["pprr"] = regions_by_page
        return article
    except Exception:
        article["has_problem"] = True
        return article


def text_apply_breaks(fulltext, breaks):
    """Apply breaks to the text returned by `rebuild_for_solr`.

    The purpose of this function is to debug (visually) the `rebuild_for_solr`
    function. It applies to `fulltext` the characte offsets contained in
    `breaks` (e.g. line breaks, paragraph breaks, etc.).

    :param fulltext: input text
    :type fulltext: str
    :param breaks: a list of character offsets
    :type breaks: list of int
    :return: a list of text chunks
    :rtype: list
    """

    text = []
    start = 0

    for br in breaks:
        text.append(fulltext[start:br].strip())
        start = br

    text.append(fulltext[start:])

    return text


def get_iiif_and_coords(ci: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Fetch the iiif link and image coordinates from CI metadata.

    Adapts to the various cases currently present in the canonical data, see
    https://github.com/impresso/impresso-text-acquisition/issues/117.

    Args:
        ci (dict[str, Any]): Content item to retrieve the information from.

    Returns:
        tuple[str | None, str | None]: IIIF link and coordinates as string or
            None if part of the information is missing from the content item
    """
    if "iiif_link" in ci or "iiif_link" in ci["m"]:
        iiif_link = ci["m"]["iiif_link"] if "iiif_link" in ci["m"] else ci["iiif_link"]

        if "c" in ci or "c" in ci["m"]:
            coords = ci["c"] if "c" in ci else ci["m"]["c"]

            if iiif_link and coords:
                return iiif_link, ",".join([str(c) for c in coords])
    return None, None


def reconstruct_iiif_link(content_item: dict[str, Any]) -> str:
    """Construct the iiif link to the CI's image based on its metadata.

    A iiif image API link and the image coordinates are to be fetched
    from the content item first.
    Different importers (and endpoints) have different formats, needing
    different processing.
    In addition, some inconsistencies exist in the canonical data.
    This function adapts to these variations, more details in issue:
    https://github.com/impresso/impresso-text-acquisition/issues/117

    Args:
        content_item (dict[str, Any]): Content item in canonical format.

    Returns:
        str: iiif link to the image area of the content item if present in the
            CI metadata, else None.
    """
    img_suffix = "full/0/default.jpg"

    iiif, coords = get_iiif_and_coords(content_item)
    if iiif:
        # recover the url base to which the image suffix should be appended
        uri_base, old_suffix = os.path.split(iiif)

        # SWA and BCUL data have a different image suffix than other endpoints
        for iiif_base, iiif_suffix in IIIF_ENDPOINT_BASE_2_SUFFIX.items():
            img_suffix = iiif_suffix if iiif_base in uri_base else img_suffix

        if old_suffix == "default.jpg":
            # iiif was already constructed according to needs.
            if coords in iiif and img_suffix in iiif:
                return iiif
            # uri was already of image, but not correct.
            uri_base = "/".join(uri_base.split("/")[:-3])
        elif old_suffix != "info.json":
            warn_msg = (
                f"Unexpected iiif url suffix: {old_suffix} "
                f"for CI with id: {content_item['id']}."
            )
            logger.warning(warn_msg)

        # reconstruct the final image link
        return os.path.join(uri_base, coords, img_suffix)
    return None


def insert_whitespace(
    token: str,
    next_t: Optional[str],
    prev_t: Optional[str],
    lang: Optional[str],
) -> bool:
    """Determine whether a whitespace should be inserted after a token.

    Args:
        token (str): Current token.
        next_t (str): Following token.
        prev_t (str): Previous token.
        lang (str): Language of text.

    Returns:
        bool: Whether a whitespace should be inserted after the `token`.
    """
    # if current token text is None, previous token's whitespace rule applies
    if token is None or len(token) == 0:
        return False

    wsrules = WHITESPACE_RULES[lang if lang in WHITESPACE_RULES else "other"]

    insert_ws = True

    if (
        token in wsrules["pct_no_ws_before_after"]
        or next_t in wsrules["pct_no_ws_before_after"]
    ):
        insert_ws = False

    # the first char of the next token is punctuation.
    elif next_t is not None and len(next_t) != 0:
        if (
            next_t in wsrules["pct_no_ws_before"]
            or next_t[0] in wsrules["pct_no_ws_before"]
        ):
            insert_ws = False

    # the last char of current token is punctuation.
    elif token in wsrules["pct_no_ws_after"] or token[-1] in wsrules["pct_no_ws_after"]:
        insert_ws = False

    elif token in wsrules["pct_number"] and prev_t is not None and next_t is not None:
        if prev_t.isdigit() and next_t.isdigit():
            return False
        else:
            return True

    debug_msg = (
        f"Insert whitespace: curr={token}, follow={next_t}, "
        f"prev={prev_t} ({insert_ws})"
    )
    logger.debug(debug_msg)

    return insert_ws
