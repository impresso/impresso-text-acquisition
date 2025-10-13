"""Helper functions for the text `rebuilder.py` script."""

import json
import logging
import os
import datetime
from typing import Any, Optional

from impresso_essentials.io.s3 import (
    IMPRESSO_STORAGEOPT,
    alternative_read_text,
    get_s3_resource,
)
from impresso_essentials.io.fs_utils import parse_canonical_filename
from impresso_essentials.utils import IssueDir, Timer, timestamp
from text_preparation.rebuilders.audio_rebuilders import (
    reconstruct_audios,
    recompose_ci_from_audio_passim,
    recompose_ci_from_audio_solr,
)
from text_preparation.rebuilders.paper_rebuilders import (
    reconstruct_pages,
    recompose_ci_from_page_passim,
    recompose_ci_from_page_solr,
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
    "page": None,
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
        s3_client (`boto3.resources.factory.s3.ServiceResource`, optional): open connection
            to S3 storage. Defaults to None.

    Returns:
        tuple[IssueDir, dict[str, Any]]: Input issue and its JSON canonical representation.
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


def read_issue_supports(
    issue: IssueDir, issue_json: dict[str, Any], is_audio: bool, bucket: str | None = None
) -> tuple[IssueDir, dict[str, Any]]:
    """Read all pages/audio records of a given issue from S3 in parallel, and add them to it.

    The found and read files will then be added to the issue's canonical json representation
    in the properties `rr` or `pp` based on `is_audio`.

    Args:
        issue (IssueDir): IssueDir object for which to read the pages/audios.
        issue_json (dict[str, Any]): `issue_data` dict of the given issue.
        is_audio (bool): Whether the issue corresponds to audio data.
        bucket (str | None, optional): Bucket where to go fetch the pages/audios.
            Defaults to None.

    Returns:
        tuple[IssueDir, dict[str, Any]]: The given issue, with the pages/audios data added.
    """
    support = "audios" if is_audio else "pages"
    alias = issue.alias
    year = issue.date.year

    if "s3//" not in bucket:
        bucket = f"s3://{bucket}"

    # TODO add provider
    filename = os.path.join(
        bucket, alias, support, f"{alias}-{year}", f"{issue_json['id']}-{support}.jsonl.bz2"
    )

    supports = [json.loads(s) for s in alternative_read_text(filename, IMPRESSO_STORAGEOPT)]

    if is_audio:
        issue_json["rr"] = supports
    else:
        issue_json["pp"] = supports
    del supports
    return (issue, issue_json)


def rebuild_for_solr(content_item: dict[str, Any]) -> dict[str, Any]:
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

    ci_id = content_item["m"]["id"]
    logger.debug("Started rebuilding ci %s", ci_id)

    year, month, day, _, ci_num = ci_id.split("-")[1:]
    d = datetime.date(int(year), int(month), int(day))

    if content_item["m"]["tp"] in TYPE_MAPPINGS:
        mapped_type = TYPE_MAPPINGS[content_item["m"]["tp"]]
    else:
        mapped_type = content_item["m"]["tp"]

    if "lg" in content_item["m"]:
        ci_lang = content_item["m"]["lg"]
    elif "l" in content_item["m"]:
        ci_lang = content_item["m"]["l"]
    else:
        ci_lang = None

    # if the reading order is not defined, use the number associated to each CI
    reading_order = content_item["m"]["ro"] if "ro" in content_item["m"] else int(ci_num[1:])
    has_olr = False if mapped_type is None or content_item["st"] == "radio_broadcast" else True

    support_field = "rr" if content_item["sm"] == "audio" else "pp"

    solr_ci = {
        "id": ci_id,
        "ts": timestamp(),
        support_field: content_item["m"][support_field],
        "d": d.isoformat(),
        # for audio, cc is true by default
        "cc": True if content_item["sm"] == "audio" else content_item["m"]["cc"],
        "olr": has_olr,
        "st": content_item["st"],
        "sm": content_item["sm"],
        "lg": ci_lang,
        "tp": mapped_type,
        "ro": reading_order,
    }

    if mapped_type == "img":
        solr_ci["iiif_link"] = reconstruct_iiif_link(content_item)

    if content_item["sm"] == "audio":
        solr_ci["stt"] = content_item["stt"]
        solr_ci["dur"] = content_item["dur"]

    # add the metadata on the content item if it's available
    if "t" in content_item["m"]:
        solr_ci["title"] = content_item["m"]["t"]
    if "rc" in content_item:
        solr_ci["rc"] = content_item["rc"]
    if "rp" in content_item:
        solr_ci["rp"] = content_item["rp"]

    # special case for BL and SWISSINFO data - when there can be period-specific titles
    if "var_t" in content_item["m"]:
        solr_ci["var_t"] = content_item["m"]["var_t"]
    # special case for INA data
    if "archival_note" in content_item["m"]:
        solr_ci["archival_note"] = content_item["m"]["archival_note"]

    if mapped_type != "img":
        if content_item["sm"] == "audio":
            solr_ci = recompose_ci_from_audio_solr(solr_ci, content_item)
        else:
            solr_ci = recompose_ci_from_page_solr(solr_ci, content_item)
        logger.debug("Done rebuilding CI %s (Took %s)", ci_id, t.stop())

    return solr_ci


def rebuild_for_passim(content_item: dict[str, Any]) -> dict[str, Any]:
    """Rebuilds the text of an article content-item to be used with passim.

    TODO Check that this works with passim!

    Args:
        content_item (dict[str, Any]): The content-item to rebuild using its metadata.

    Returns:
        dict[str, Any]: The rebuilt content-item built for passim.
    """
    alias, date, _, _, _, _ = parse_canonical_filename(content_item["m"]["id"])

    ci_id = content_item["m"]["id"]
    logger.debug("Started rebuilding article %s", ci_id)

    # fetch the ci language, noting the deprecated "l" field
    if "lg" in content_item["m"]:
        ci_lang = content_item["m"]["lg"]
    elif "l" in content_item["m"]:
        ci_lang = content_item["m"]["l"]
    else:
        ci_lang = None

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
        "lg": ci_lang,
    }

    if content_item["sm"] == "audio":
        passim_document["audios"] = []
    else:
        passim_document["pages"] = []

    if "t" in content_item["m"]:
        passim_document["title"] = content_item["m"]["t"]

    if content_item["sm"] == "audio":
        return recompose_ci_from_audio_passim(content_item, passim_document)
    else:
        return recompose_ci_from_page_passim(content_item, passim_document)


def rejoin_cis(issue: IssueDir, issue_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Rejoin the CIs of a given issue using its pyhsical supports (pages or audio records).

    Args:
        issue (IssueDir): Issue directory of issue to be processed.
        issue_json (dict[str, Any]): Issue canonical json wôf which to rejoin CIs.

    Returns:
        list[dict[str, Any]]: Processed content-items for the issue.
    """
    msg = f"Rejoining physical supports (pages or audios) for issue {issue_json['id']}"
    logger.debug(msg)
    cis = []
    for ci in issue_json["i"]:

        ci["has_problem"] = False
        ci["sm"] = issue_json["sm"]
        ci["st"] = issue_json["st"]

        if issue_json["st"] == "radio_broadcast":
            # if the radio channel and program are defined, add them to the content item
            if "rc" in issue_json:
                ci["rc"] = issue_json["rc"]
            if "rp" in issue_json:
                ci["rp"] = issue_json["rp"]
            if "rr" in issue_json:
                # TODO update in the case we can have >1 record per CI or vice-versa
                if len(ci["m"]["rr"]) > 1:
                    msg = (
                        f"{ci['if']} - PROBLEM - more than one record for this CI! "
                        "Taking only the first one."
                    )
                    print(msg)
                    logger.warning(msg)
                # taking the start time and duration of the first record for this CI
                # (numbering starts at 1)
                ci["stt"] = issue_json["rr"][ci["m"]["rr"][0] - 1]["stt"]
                ci["dur"] = issue_json["rr"][ci["m"]["rr"][0] - 1]["dur"]

        if issue_json["sm"] == "audio":
            # for audio recordings there is only 1 per issue
            ci["m"]["rr"] = sorted(list(set(ci["m"]["rr"])))
            cis = reconstruct_audios(issue_json, ci, cis)
        else:
            ci["m"]["pp"] = sorted(list(set(ci["m"]["pp"])))
            cis = reconstruct_pages(issue_json, ci, cis)

    return cis


def pages_to_article(article: dict[str, Any], pages: list[dict[str, Any]]) -> dict[str, Any]:
    """Return all text regions belonging to a given article."

    Args:
        article (dict[str, Any]): Article/CI for which to fetch the regions.
        pages (list[dict[str, Any]]): Pages from which to fetch the regions.

    Returns:
        dict[str, Any]: Article completed with the regions extracted from the page.
    """
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
