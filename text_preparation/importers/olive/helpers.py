"""Helper functions used by the Olive Importer.

These functions are mainly used within (i.e. called by) the classes
``OliveNewspaperIssue`` and ``OliveNewspaperPage``.
"""

import os
import copy
import logging
import time
from operator import itemgetter
from time import strftime
from enum import Enum
from typing import Any

import numpy as np
from bs4 import BeautifulSoup
import cv2 as cv

from impresso_essentials.utils import IssueDir, SourceMedium, SourceType, timestamp
from text_preparation.importers.classes import CanonicalIssue, ZipArchive
from text_preparation.tokenization import insert_whitespace

logger = logging.getLogger(__name__)


def merge_tokens(tokens: list[dict[str, Any]], line: str) -> dict[str, Any]:
    """Merge two or more tokens for the same line into one.

    The resulting (merged) token will have new coordinates corresponding
    to the combination of coordinates of the input tokens.

    Args:
        tokens (list[dict[str, Any]]): Tokens to merge.
        line (str): The line of text to which the input tokens belong.

    Returns:
        dict[str, Any]: The new (merged) token.
    """
    merged_token = {
        "tx": "".join([token["tx"] for token in tokens]),
        "c": tokens[0]["c"][:2] + tokens[-1]["c"][2:],
        "s": tokens[0]["s"],
    }
    logger.debug(
        '(In-line pseudo tokens) Merged %s => %s in line "%s"',
        "".join([t["tx"] for t in tokens]),
        merged_token["tx"],
        line,
    )

    return merged_token


def merge_pseudo_tokens(line: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """Remove pseudo tokens from a line by merging them.

    Args:
        line (dict[str, list[Any]]): A line of OCR in JSON format.

    Returns:
        dict[str, list[Any]]: A new line object (with some merged tokens).
    """
    original_line = " ".join([t["tx"] for t in line["t"]])
    qids = {token["qid"] for token in line["t"] if "qid" in token}

    inline_qids = []

    for qid in qids:
        tokens = [
            (i, token)
            for i, token in enumerate(line["t"])
            if "qid" in token and token["qid"] == qid
        ]
        if len(tokens) > 1:
            inline_qids.append(qid)

    if len(inline_qids) == 0:
        return line

    for qid in inline_qids:
        # identify tokens to merge
        tokens = [
            (i, token)
            for i, token in enumerate(line["t"])
            if "qid" in token and token["qid"] == qid
        ]

        # remove tokens to merge from the line
        tokens_to_merge = [line["t"].pop(line["t"].index(token)) for i, token in tokens]

        if len(tokens_to_merge) >= 2:
            insertion_point = tokens[0][0]
            merged_token = merge_tokens(tokens_to_merge, original_line)
            line["t"].insert(insertion_point, merged_token)

    return line


def normalize_hyphenation(line: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """Normalize end-of-line hyphenated words.

    Args:
        line (dict[str, list[Any]]): A line of OCR.

    Returns:
        dict[str, list[Any]]: A new line element.
    """
    for i, token in enumerate(line["t"]):
        if i == (len(line["t"]) - 1):
            if token["tx"][-1] == "-":
                token["hy"] = True
            if token["tx"] == "-" and "nf" in token:
                prev_token = line["t"][i - 1]
                line["t"] = line["t"][:-2]
                merged_token = {
                    "tx": "".join([prev_token["tx"], token["tx"]]),
                    "c": prev_token["c"][:2] + token["c"][2:],
                    "s": token["s"],
                    "hy": token["hy"],
                }
                logger.debug("Merged %s and %s => %s", prev_token, token, merged_token)
                line["t"].append(merged_token)

    return line


def combine_article_parts(article_parts: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge article parts into a single element.

    Olive format splits an article into multiple components whenever it spans
    over multiple pages. Thus, it is necessary to recompose multiple parts.

    Args:
        article_parts (list[dict[str, Any]]): One or more article parts.

    Returns:
        dict[str, Any]: Dict with keys `meta`, `fulltext`, `stats`,`legacy`.
    """
    if len(article_parts) > 1:
        # if an article has >1 part, retain the metadata
        # from the first item in the list
        article_dict = {"meta": {}, "fulltext": "", "stats": {}, "legacy": {}}
        article_dict["legacy"]["id"] = [ar["legacy"]["id"] for ar in article_parts]
        article_dict["legacy"]["source"] = [ar["legacy"]["source"] for ar in article_parts]
        article_dict["meta"]["type"] = {}
        article_dict["meta"]["type"]["raw"] = article_parts[0]["meta"]["type"]["raw"]

        article_dict["meta"]["title"] = article_parts[0]["meta"]["title"]
        article_dict["meta"]["page_no"] = [
            int(n) for ar in article_parts for n in ar["meta"]["page_no"]
        ]

        # TODO: remove from production
        if len(article_dict["meta"]["page_no"]) > 1:
            # pdb.set_trace()
            pass

        article_dict["meta"]["language"] = {}
        article_dict["meta"]["language"] = article_parts[0]["meta"]["language"]
        article_dict["meta"]["issue_date"] = article_parts[0]["meta"]["issue_date"]
    elif len(article_parts) == 1:
        article_dict = next(iter(article_parts))
    else:
        article_dict = None

    return article_dict


def normalize_line(line: dict[str, list[Any]], lang: str) -> dict[str, list[Any]]:
    """Apply normalization rules to a line of OCR.

    The normalization rules that are applied depend on the language in which
    the text is written. This normalization is necessary because Olive, unlike
    e.g. Mets, does not encode explicitly the presence/absence of whitespaces.

    Args:
        line (dict[str, list[Any]]): A line of OCR text.
        lang (str): Language of the text.

    Returns:
        dict[str, list[Any]]: The new normalized line of text.
    """
    mw_tokens = [token for token in line["t"] if "qid" in token]
    # apply normalization only to those lines that contain at least one
    # multi-word token (denoted by presence of `qid` field)
    if len(mw_tokens) > 0:
        line = merge_pseudo_tokens(line)
        line = normalize_hyphenation(line)

    for i, token in enumerate(line["t"]):
        if "qid" not in token and "nf" in token:
            del token["nf"]

        if "qid" in token:
            del token["qid"]

        if i == 0 and i != len(line["t"]) - 1:
            insert_ws = insert_whitespace(token["tx"], line["t"][i + 1]["tx"], None, lang)

        elif i == 0 and i == len(line["t"]) - 1:
            insert_ws = insert_whitespace(token["tx"], None, None, lang)

        elif i == len(line["t"]) - 1:
            insert_ws = insert_whitespace(token["tx"], None, line["t"][i - 1]["tx"], lang)

        else:
            insert_ws = insert_whitespace(
                token["tx"], line["t"][i + 1]["tx"], line["t"][i - 1]["tx"], lang
            )
        if not insert_ws:
            token["gn"] = True

    return line


def keep_title(title: str) -> bool:
    """Whether an element's title should be kept.

    The title should not be kept if it is one of "untitled article",
    "untitled ad", and "untitled picture".

    Args:
        title (str): Title to verify

    Returns:
        bool: False if given title is in the black list, True otherwise.
    """
    black_list = ["untitled article", "untitled ad", "untitled picture"]
    if title.lower() in black_list:
        return False

    return True


def recompose_ToC(
    original_toc_data: dict[int, dict[str, dict]],
    articles: list[dict[str, Any]],
    images: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Recompose the ToC of a newspaper issue.

    Function used by
    :class:`~text_importer.importers.olive.classes.OliveNewspaperIssue`.

    Args:
        original_toc_data (dict[int, dict[str, dict]]): ToC data.
        articles (list[dict[str, Any]]): List of articles in the issue.
        images (list[dict[str, str]]): List of images in the issue.

    Returns:
        list[dict[str, Any]]: List of final content items in the issue.
    """
    # Added deep copy because function changes toc_data
    toc_data = copy.deepcopy(original_toc_data)
    # concate content items from all pages into a single flat list
    content_items = [toc_data[pn][elid] for pn in toc_data.keys() for elid in toc_data[pn].keys()]

    # filter out those items that are part of a multipart article
    contents = []
    sorted_content_items = sorted(content_items, key=itemgetter("seq"))
    for item in sorted_content_items:

        item["m"] = {}
        item["l"] = {}

        if item["type"] == "Article" or item["type"] == "Ad":

            # find the corresponding item in `articles`
            # by using `legacy_id` as the search key
            # if not found (raises exception) means that it's one of the
            # multipart articles, and it's ok to skip it
            legacy_id = item["legacy_id"]
            article = None
            for ar in articles:
                if isinstance(ar["legacy"]["id"], list):
                    if ar["legacy"]["id"][0] == legacy_id:
                        article = ar
                else:
                    if ar["legacy"]["id"] == legacy_id:
                        article = ar

            try:
                assert article is not None
            except Exception:
                continue

            item["m"]["id"] = item["id"]
            item["m"]["pp"] = article["meta"]["page_no"]
            item["m"]["lg"] = article["meta"]["language"]
            item["m"]["tp"] = article["meta"]["type"]["raw"].lower()

            if keep_title(article["meta"]["title"]):
                item["m"]["t"] = article["meta"]["title"]

            item["l"]["id"] = article["legacy"]["id"]
            item["l"]["source"] = article["legacy"]["source"]

        elif item["type"] == "Picture":

            # find in which page the image is
            page_no = [page_no for page_no in toc_data if item["legacy_id"] in toc_data[page_no]]

            # get the new canonical id via the legacy id
            item["m"]["id"] = item["id"]
            item["m"]["tp"] = item["type"].lower()
            item["m"]["pp"] = page_no

            try:
                image = [image for image in images if image["id"] == item["legacy_id"]][0]
            except IndexError:
                # if the image XML was faulty (e.g. because of missing
                # coords, it won't find a corresping image item
                logger.info("Image %s will be skipped", item["legacy_id"])
                continue

            if keep_title(image["name"]):
                item["m"]["t"] = image["name"]

            item["l"]["id"] = item["legacy_id"]
            item["l"]["res"] = image["resolution"]
            item["l"]["path"] = image["filepath"]

            item["c"] = image["coords"]
            toc_item = toc_data[page_no[0]][item["legacy_id"]]

            if "embedded_into" in item:
                cont_article_id = toc_item["embedded_into"]
                try:
                    containing_article = toc_data[page_no[0]][cont_article_id]

                    # content item entries exists in different shapes within
                    # the `toc_data` dict, depending on whether they have
                    # already been processed in this `for` loop or not
                    if "m" in containing_article and len(containing_article["m"].keys()) > 0:
                        item["pOf"] = containing_article["m"]["id"]
                    else:
                        item["pOf"] = containing_article["id"]
                except Exception as e:
                    logger.error(
                        "Containing article for %s not found (error = %s)",
                        item["m"]["id"],
                        e,
                    )

        # delete redundant fields
        if "embedded_into" in item:
            del item["embedded_into"]
        del item["seq"]
        del item["legacy_id"]
        del item["type"]
        del item["id"]

        contents.append(item)

    return contents


def recompose_page(
    page_id: str,
    info_from_toc: dict[str, dict],
    page_elements: dict[str, dict],
    clusters: dict[str, list[str]],
) -> dict[str, Any]:
    """Merge a list of page elements into a single one.

    Note:
        It is here that an ``n`` attribute is assigned to each
        region/paragraph/line/token.

    Args:
        page_id (str): Page canonical id.
        info_from_toc (dict[str, dict]): Dictionary with page element IDs
            (articles, ads.) as keys, and dictionaries as values.
        page_elements (dict[str, dict]): Page's articles or advertisements.
        clusters (dict[str, list[str]]): Inverted index of legacy ids; values
            are clusters of articles, each indexed by one member.

    Returns:
        dict[str, Any]: Page data according to impresso canonical format.
    """
    page = {"r": []}
    ordered_elements = sorted(list(info_from_toc.values()), key=itemgetter("seq"))

    id_mappings = {legacy_id: info_from_toc[legacy_id]["id"] for legacy_id in info_from_toc}

    # put together the regions while keeping the order in the page
    for el in ordered_elements:

        # keep only IDS of content items that are Ads or Articles
        # but escluding various other files in the archive
        if "Ar" not in el["legacy_id"] and "Ad" not in el["legacy_id"]:
            continue

        # this is to manage the situation of a multi-part article
        part_of = None
        if el["legacy_id"] in clusters:
            part_of = el["legacy_id"]
        else:
            for key in clusters:
                if el["legacy_id"] in clusters[key]:
                    part_of = key
                    break

        if el["legacy_id"] in page_elements:
            element = page_elements[el["legacy_id"]]
        else:
            logger.error("%s: %s not found in page %s", el["id"], el["legacy_id"], page_id)
            continue
        mapped_id = id_mappings[part_of] if part_of in id_mappings else None

        for region in element["r"]:
            region["pOf"] = mapped_id

        page["r"] += element["r"]

    return page


def compute_scale_factor(img_source_path: str, img_dest_path: str) -> float:
    """Computes x scale factor bewteen 2 images.

    Args:
        img_source_path (str): Full path ot the source image.
        img_dest_path (str): Full path to the destination image.

    Returns:
        float: X Scale factor between the two.
    """
    img_s = cv.imread(img_source_path, 0)
    img_d = cv.imread(img_dest_path, 0)
    x_s = img_s.shape[0]
    x_d = img_d.shape[0]
    return x_d / x_s


def compute_box(scale_factor: float, input_box: str) -> str | None:
    """Compute IIIF box coordinates of input_box relative to scale_factor.

    Args:
        scale_factor (float): Ratio between 2 images with different dimensions.
        input_box (str): String with 4 values separated by spaces.

    Returns:
        str | None: New box coordinates or None if the string had the wrong fromat.
    """
    try:
        elems = input_box.split(" ")
    except ValueError:
        logger.info("Invalid box format: %s", input_box)
        return

    x = round(int(elems[0]) * scale_factor)
    y = round(int(elems[1]) * scale_factor)
    w = round((int(elems[2]) - int(elems[0])) * scale_factor)
    h = round((int(elems[3]) - int(elems[1])) * scale_factor)
    return " ".join([str(x), str(y), str(w), str(h)])


def convert_box(coords: list[int], scale_factor: float) -> list[int]:
    """Rescale iiif box coordinates relative to given scale factor.

    Args:
        coords (list[int]): Original box coordinates.
        scale_factor (float): Scale factor based on image conversion necessary.

    Returns:
        list[int]: Rescaled box coordinates.
    """
    box = " ".join([str(coord) for coord in coords])
    converted_box = compute_box(scale_factor, box)
    new_box = [int(c) for c in converted_box.split()]
    logger.debug("Converted box coordinates: %s => %s", box, converted_box)

    return new_box


class BoxStrategy(Enum):
    tif = "tif"
    png_highest = "png_highest"
    png_uniq = "png_uniq"
    jpg_uniq = "jpg_uniq"
    jpg_highest = "jpg_highest"


def get_scale_factor(issue_dir_path, archive, page_xml, box_strategy, img_source_name):
    """
    Returns the scale factor in Olive context, given a strategy to choose the source image.

    :param issue_dir_path: the path of the issue
    :type  issue_dir_path: str
    :param archive: the zip archive
    :type  archive: zipfile.ZipFile
    :param page_xml: the xml handler of the page
    :type page_xml: bytes
    :param box_strategy: the box strategy such as found in the info.txt from jp2 folder
    :type box_strategy: str
    :param img_source_name: as found in the info.txt from jp2 folder
    :return: the hopefully correct scale factor
    :rtype: float

    Background information
    ======================
    Impresso converts library images to JP2, taking the best image available: tif > highest png > jpg.
    Olive box coordinates were computed according to an image source which we have to identify among several.
    Image format coverage is different from issue to issue, and we have to devise strategies.

    Case 1: tif
    -----------
    The tif is present and is the file from which the jp2 was converted.
    Dest: Tif dimensions can therefore be used as jp2 dimensions, no need to read the jp2 file.
    Source: Image source dimension is present in the page.xml (normally).

    Case 2: several png
    -------------------
    In this case the jp2 was acquired using the png with the highest dimension.
    Dest: It looks that in case of several png, Olive also took the highest for the OCR. It is therefore
    possible to rely on the resolution indicated in the page xml, which should be the same as our jp2.
    N.B.: the page width and heigth indicated in the xml do not correspond (usually) to the highest
    resolution png (there is therefore a discrepancy in Olive file between the tag 'images_resolution'
    on the one hand, and 'page_width|height'on the other). It seems we can ignore this and rely on the
    resolution only in the current case.
    Source: the highest png
    Here source and dest dimension are equals, the function returns 1.

    Case 3: one png only
    --------------------
    To be checked if it happens.
    In this case, there is no choice and Olive OCR and JP2 acquisition should be from the same source
    => scale factor of 1.
    Here we do an additional check to see if the page_width|height are the same as the image ones.
    The only danger is if Olive used another image file and did not provide it.

    Case 4: one jpg only
    --------------------
    Same as Case 3, scale factor of 1.
    Here we do an additional check to see if the page_width|height are the same as the image ones.
    (there is only one image and things should fit, not like in case 2)
    """
    page_soup = BeautifulSoup(page_xml, "lxml")
    page_root = page_soup.find("xmd-page")
    page_number = page_root.meta["page_no"]
    if box_strategy == BoxStrategy.tif.name:
        for f in page_root.datafiles.find_all("files"):
            if f["type"] == "PAGE_IMG" and f["present"] == "1":
                source_res = f["xresolution_dpi"]
                dest_res = page_root.meta["images_resolution"]
                break
        if source_res and dest_res:
            return int(source_res) / int(dest_res)

        else:
            logger.info(
                "Impossible to get resolution in case: tif in %s, page %s",
                issue_dir_path,
                page_number,
            )
            return None

    elif box_strategy == BoxStrategy.png_highest.name:
        if "_" not in img_source_name:
            logger.info("Not valid png filename %s", img_source_name)
            return None

        png_res = os.path.splitext(img_source_name)[0].split("_", 1)[-1]
        olive_res = page_root.meta["images_resolution"]
        if png_res == olive_res:
            return 1.0
        else:
            msg = (
                "Incompatible resolutions between highest png and olive indications "
                f"in {issue_dir_path}, page {page_number}"
            )
            logger.info(msg)
            print(msg)
            return None

    elif box_strategy == BoxStrategy.png_uniq.name:
        # TODO if needed
        logger.info(
            "Finally found a case of %s, " "which is not ready yet",
            BoxStrategy.png_uniq,
        )

    elif box_strategy == BoxStrategy.jpg_uniq.name:
        # get the x dimension of the unique jpg (from which jp2 was acquired)
        # and compare with olive's one
        img_data = archive.read(img_source_name)
        img = cv.imdecode(np.frombuffer(img_data, np.uint8), 1)
        jpg_x_dim = img.shape[1]
        olive_x_dim = page_root.meta["page_width"]
        if jpg_x_dim == int(olive_x_dim):
            return 1.0
        else:
            logger.info(
                "Incompatible resolutions between uniq jpg and olive indications"
                " in {issue_dir_path}, page {page_number}."
            )
            return None


# TODO: move to the OliveNewspaperPage class as a method?
# I cannot document using type info because of circular imports, which is a
# sign that perhaps this function should rather be a method.
def convert_page_coordinates(
    page: dict[str, Any],
    page_xml: str,
    page_image_name: str,
    zip_archive: ZipArchive,
    box_strategy: str,
    issue: CanonicalIssue,
) -> bool:
    """Convert coordinates of all elements in a page that have coordinates.

    Note:
        This conversion is necessary since the coordinates recorded in the XML
        file were computed on a different image than the one used for display
        in the impresso interface.

    Args:
        page (dict[str, Any]): Page data where coordinates should be converted.
        page_xml (str): Content of Olive page XML.
        page_image_name (str): Name of page image file.
        zip_archive (ZipArchive): Olive Zip archive.
        box_strategy (str): Conversion strategy to apply.
        issue (CanonicalIssue): Newspaper issue the page belongs to.

    Returns:
        bool: Whether the coordinate conversion was successful or not.
    """
    start_t = time.time()
    scale_factor = get_scale_factor(
        issue.path, zip_archive, page_xml, box_strategy, page_image_name
    )
    if scale_factor is not None:
        for region in page["r"]:
            region["c"] = convert_box(region["c"], scale_factor)
            for paragraph in region["p"]:
                for line in paragraph["l"]:
                    line["c"] = convert_box(line["c"], scale_factor)
                    for token in line["t"]:
                        token["c"] = convert_box(token["c"], scale_factor)
        end_t = time.time()
        t = end_t - start_t
        logger.debug("Converted coordinates %s in %s (took %ss)", page_image_name, issue.id, t)
        return True

    logger.info("Could not find scale factor for %s", page["id"])
    return False


def convert_image_coordinates(
    image: dict[str, Any],
    page_xml: str,
    page_image_name: str,
    zip_archive: ZipArchive,
    box_strategy: str,
    issue: IssueDir,
) -> dict[str, Any]:
    """Convert coordinates of an Olive image element.

    Note:
        This conversion is necessary since the coordinates recorded in the XML
        file were computed on a different image than the one used for display
        in the impresso interface.

    Args:
        image (dict[str, Any]): Image metadata.
        page_xml (str): Content of Olive page XML.
        page_image_name (str): Name of page image file.
        zip_archive (ZipArchive): Olive Zip archive.
        box_strategy (str): Conversion strategy to apply.
        issue (IssueDir): IssueDie of the newspaper issue the page belongs to.

    Returns:
        dict[str, Any]: Updated image metadata based on the conversion.
    """
    try:
        scale_factor = get_scale_factor(
            issue.path, zip_archive, page_xml, box_strategy, page_image_name
        )
        image["c"] = convert_box(image["c"], scale_factor)
        image["cc"] = True
    except Exception:
        image["cc"] = False

    return image


def normalize_language(language: str) -> str:
    """Normalize the language's string representation.

    Args:
        language (str): Language to normalize.

    Returns:
        str: Normalized language, one of "fr", "en" and "de".
    """
    mappings = {"french": "fr", "english": "en", "german": "de"}

    return mappings[language.lower()]


def get_clusters(articles: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Created inverted index of legacy ids to article clusters.

    Each cluster of articles is indexed by the legacy id of one its members.
    If a cluster contains only one element, the its id will be in the keys.

    Args:
        articles (list[dict[str, Any]]): Articles to cluster by legacy ids.

    Returns:
        dict[str, list[str]]: Article clusters dictionary.
    """
    clusters = {}
    for ar in articles:
        legacy_id = ar["legacy"]["id"]
        if isinstance(legacy_id, list):
            clusters[legacy_id[0]] = legacy_id
        else:
            clusters[legacy_id] = [legacy_id]

    return clusters
