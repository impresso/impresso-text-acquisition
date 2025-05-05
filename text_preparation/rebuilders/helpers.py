"""Helper functions for the text `rebuilder.py` script."""

import json
import logging
import os
from typing import Any, Optional

from impresso_essentials.io.s3 import (
    IMPRESSO_STORAGEOPT,
    alternative_read_text,
    get_s3_resource,
)
from impresso_essentials.io.fs_utils import parse_canonical_filename
from impresso_essentials.text_utils import WHITESPACE_RULES
from impresso_essentials.utils import IssueDir
from text_preparation.rebuilders.audio_rebuilders import (
    recompose_audio_fulltext,
    reconstruct_audios,
)
from text_preparation.rebuilders.paper_rebuilders import (
    recompose_paper_fulltext,
    reconstruct_pages,
)

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_BASE_2_SUFFIX = {
    # suffix for SWA data
    "https://ub-sipi.ub.unibas.ch/impresso": "max/0/default.jpg",
    # suffix for BCUL data, sometimes 300 can be replaced by larger values
    "https://scriptorium.bcu-lausanne.ch/api": "300,/0/default.jpg",
}

TYPE_MAPPINGS = {
    "article": "ar",
    "ar": "ar",
    "advertisement": "ad",
    "ad": "ad",
    "pg": None,
    "image": "img",
    "table": "tb",
    "death_notice": "ob",
    "weather": "w",
    "chronicle": "ch",
}
# TODO KB data: add familial announcement?


def ci_has_problem(ci: dict[str, Any]) -> bool:
    """Helper function to keep CIs with problems.

    Args:
        ci (dict[str, Any]): Input CI

    Returns:
        bool: Whether a problem was detected in the CI.
    """
    return ci["has_problem"]


def ci_without_problem(ci: dict[str, Any]) -> bool:
    """Helper function to keep CIs without problems, and log others.

    Args:
        ci (dict[str, Any]): Input CI

    Returns:
        bool: Whether the CI ws problem-free.
    """
    if ci["has_problem"]:
        msg = f"Content-item {ci['m']['id']} won't be rebuilt as a problem was found."
        logger.warning(msg)
        print(msg)
    return not ci["has_problem"]


def read_issue(
    issue: IssueDir, bucket_name: str, s3_client=None
) -> tuple[IssueDir, dict[str, Any]]:
    """Read the data from S3 for a given canonical issue.

    Args:
        issue (IssueDir): Input issue to fetch form S3
        bucket_name (str): S3 bucket's name
        s3_client (`boto3.resources.factory.s3.ServiceResource`, optional): open connection to S3 storage. Defaults to None.

    Returns:
        tuple[IssueDir, dict[str, Any]]: Pair of the input issue and its JSON canonical representation.
    """
    if s3_client is None:
        s3_client = get_s3_resource()

    content_object = s3_client.Object(bucket_name, issue.path)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    issue_json = json.loads(file_content)
    logger.info("Read JSON of %s", issue)
    return (issue, issue_json)


def read_page(page_key: str, bucket_name: str, s3_client) -> dict[str, Any] | None:
    """Read the data from S3 for a given canonical page

    Args:
        page_key (str): S3 key to the page
        bucket_name (str): S3 bucket's name
        s3_client (`boto3.resources.factory.s3.ServiceResource`): open connection to S3 storage.

    Returns:
        dict[str, Any] | None: The page's JSON representation or None if the page could not be read.
    """

    try:
        content_object = s3_client.Object(bucket_name, page_key)
        file_content = content_object.get()["Body"].read().decode("utf-8")
        page_json = json.loads(file_content)
        # logger.info("Read page %s from bucket %s", page_key, bucket_name)
        return page_json
    except Exception as e:  # Replace with specific exceptions
        if isinstance(e, s3_client.meta.client.exceptions.NoSuchKey):
            msg = f"Key {page_key} does not exist in bucket {bucket_name}."
        elif isinstance(e, s3_client.meta.client.exceptions.ClientError):
            msg = f"Client error occurred while accessing {page_key}: {e}"
        elif isinstance(e, json.JSONDecodeError):
            msg = f"Failed to decode JSON for {page_key}: {e}"
        else:
            msg = f"An unexpected error occurred while reading {page_key}: {e}"
        print(msg)
        logger.error(msg)
        return None


def read_issue_supports(issue, issue_json, pages: bool, bucket=None):
    """Read all pages of a given issue from S3 in parallel."""
    support = "pages" if pages else "audios"
    alias = issue.alias
    year = issue.date.year

    filename = (
        f"{bucket}/{alias}/{support}/{alias}-{year}" f"/{issue_json['id']}-{support}.jsonl.bz2"
    )

    supports = [json.loads(s) for s in alternative_read_text(filename, IMPRESSO_STORAGEOPT)]

    print(filename)
    if pages:
        issue_json["pp"] = supports
    else:
        issue_json["rr"] = supports
    del supports
    return (issue, issue_json)


def rebuild_for_passim(content_item: dict[str, Any]) -> dict[str, Any]:
    """Rebuilds the text of an article content-item to be used with passim.

    Args:
        content_item (dict[str, Any]): The content-item to rebuild using its metadata.

    Returns:
        dict[str, Any]: The rebuilt content-item built for passim.
    """
    # TODO: ensure the medium and types are in the CI!
    alias, date, _, _, _, _ = parse_canonical_filename(content_item["m"]["id"])

    ci_id = content_item["m"]["id"]
    logger.debug("Started rebuilding article %s", ci_id)
    issue_id = "-".join(ci_id.split("-")[:-1])

    support_file_names = (
        {r: f"{issue_id}-r{str(r).zfill(4)}.json" for r in content_item["m"]["rr"]}
        if content_item["sm"] == "audio"
        else {p: f"{issue_id}-r{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]}
    )

    # fetch the article language, noting the deprecated "l" field
    if "lg" in content_item["m"]:
        article_lang = content_item["m"]["lg"]
    elif "l" in content_item["m"]:
        article_lang = content_item["m"]["l"]
    else:
        article_lang = None

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
    }

    if content_item["sm"] == "audio":
        passim_document["audios"] = []
    else:
        passim_document["pages"] = []

    if "t" in content_item["m"]:
        passim_document["title"] = content_item["m"]["t"]

    if content_item["sm"] == "audio":
        return recompose_audio_fulltext(content_item, passim_document, support_file_names)
    else:
        return recompose_paper_fulltext(content_item, passim_document, support_file_names)


def rejoin_cis(issue: IssueDir, issue_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Rejoin the CIs of a given issue using its pyhsical supports (pages or audio records).

    Args:
        issue (IssueDir): Issue directory of issue to be processed.
        issue_json (dict[str, Any]): Issue canonical json wôf which to rejoin CIs.

    Returns:
        list[dict[str, Any]]: Processed content-items for the issue.
    """
    print(f"Rejoining physical supports (pages or audios) for issue {issue.path}")
    cis = []
    for ci in issue_json["i"]:

        ci["has_problem"] = False
        ci["sm"] = issue_json["sm"]
        ci["st"] = issue_json["st"]

        if issue_json["sm"] == "audio":
            # for audio recordings there is only 1 per issue
            ci["m"]["rr"] = sorted(list(set(issue_json["rr"])))
            cis = reconstruct_pages(issue_json, ci, cis)
        else:
            ci["m"]["pp"] = sorted(list(set(ci["m"]["pp"])))
            cis = reconstruct_audios(issue_json, ci, cis)

    return cis


def pages_to_article(article, pages):
    """Return all text regions belonging to a given article."""
    try:
        art_id = article["m"]["id"]
        print("Extracting text regions for article %s", art_id)
        regions_by_page = []
        for page in pages:
            regions_by_page.append([region for region in page["r"] if region["pOf"] == art_id])
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
    elif (
        next_t is not None
        and len(next_t) != 0
        and (next_t in wsrules["pct_no_ws_before"] or next_t[0] in wsrules["pct_no_ws_before"])
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
        f"Insert whitespace: curr={token}, follow={next_t}, " f"prev={prev_t} ({insert_ws})"
    )
    logger.debug(debug_msg)

    return insert_ws
