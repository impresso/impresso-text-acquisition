"""Helper functions used by the Olive Importer.

These functions are mainly used within (i.e. called by) the classes
``OliveNewspaperIssue`` and ``OliveNewspaperPage``.
"""

import copy
import logging
import time
from operator import itemgetter
from time import strftime
from typing import List
import lxml.etree
from math import ceil, floor

from text_importer.tokenization import insert_whitespace

logger = logging.getLogger(__name__)

TETPREFIX = "{http://www.pdflib.com/XML/TET3/TET-3.0}"


FILTER_WORDS = ["#", "ST", "#ST", "ST#", "#ST#"]


def get_metadata(root: lxml.etree.Element):
    """Return dict with relevant metadata for page file

    :param root: etree.Element of tetml page file
    :return: A dictionary with keys: ``tetcdt``, ``pdfpath``, ``pdfcdt``, ``npages``.
    """

    result = {}
    creation = root.find(f".//{TETPREFIX}Creation")
    if creation is not None:
        result["tetcdt"] = creation.attrib["date"]
    pdfdoc = root.find(f".//{TETPREFIX}Document")
    if pdfdoc is not None:
        result["pdfpath"] = pdfdoc.attrib["filename"]
    pdfcreation = root.find(f".//{TETPREFIX}CreationDate")
    if pdfcreation is not None:
        result["pdfcdt"] = pdfcreation.text

    pages = root.findall(f".//{TETPREFIX}Page")
    if pages is not None:
        result["npages"] = len(pages)

    return result


def filter_word(jtoken):
    """
    Check if token needs to be filtered out as it is a non-content word.
    """
    return jtoken["tx"] in FILTER_WORDS


def word2json(
    word,
    pageheight,
    pagewidth,
    imageheight,
    imagewidth,
    placed_image_attribs,
    filename=None,
):
    """
    Return dict with all information about (hyphenated) XML word element

    {"tx": Text,
    "c": coords,
    "hy" : Bool,
     "hyt": {"nf": Text, "c":coords, "tx":coords}}

    "hyt" is {} if word is not hyphenated

    :param pageheight:
    :param pagewidth:
    :param imageheight:
    :param imagewidth:
    :param placed_image_attribs:
    :param filename:
    :param word:
    :return:
    """

    result = {}
    boxes = word.findall(f"{TETPREFIX}Box")

    # unhyphenated case
    if len(boxes) == 1:
        tokentext = word.find(f"{TETPREFIX}Text").text
        if tokentext is None:
            error_msg = f"Empty TOKEN (# boxes: {len(boxes)}) in the following file:\n{filename}\n{lxml.etree.tostring(word)}"
            logger.error(error_msg)

            return

        result["tx"] = tokentext

        box = word.find("%sBox" % TETPREFIX)
        llx = float(box.get("llx"))
        lly = float(box.get("lly"))
        ury = float(box.get("ury"))
        urx = float(box.get("urx"))
        coords = compute_box(
            llx,
            lly,
            urx,
            ury,
            pageheight,
            pagewidth,
            imageheight,
            imagewidth,
            placed_image_attribs,
        )
        result["c"] = coords

    # hyphenated case
    elif len(boxes) >= 2:
        result["hy"] = True

        # word part before hyphenation
        llx1 = float(boxes[0].get("llx"))
        lly1 = float(boxes[0].get("lly"))
        ury1 = float(boxes[0].get("ury"))
        urx1 = float(boxes[0].get("urx"))
        coords1 = compute_box(
            llx1,
            lly1,
            urx1,
            ury1,
            pageheight,
            pagewidth,
            imageheight,
            imagewidth,
            placed_image_attribs,
        )
        result["c"] = coords1

        result["tx"] = "".join(c.text for c in boxes[0].findall(f"{TETPREFIX}Glyph"))

        # word part following hyphenation
        hyphenated = {
            "tx": "".join(c.text for c in boxes[1].findall(f"{TETPREFIX}Glyph")),
            "nf": word.find(f"{TETPREFIX}Text").text,
        }
        llx2 = float(boxes[1].get("llx"))
        lly2 = float(boxes[1].get("lly"))
        ury2 = float(boxes[1].get("ury"))
        urx2 = float(boxes[1].get("urx"))
        coords2 = compute_box(
            llx2,
            lly2,
            urx2,
            ury2,
            pageheight,
            pagewidth,
            imageheight,
            imagewidth,
            placed_image_attribs,
        )
        hyphenated["c"] = coords2
        result["hyt"] = hyphenated

        if len(boxes) > 2:
            error_msg = f"Wrong number of boxes: {len(boxes)} in following file \
            {filename}\n{lxml.etree.tostring(word)}"

            logger.error(error_msg)

    return result


def compute_box(
    llx,
    lly,
    urx,
    ury,
    pageheight,
    pagewidth,
    imageheight,
    imagewidth,
    placedimage_attribs,
):
    """
    Compute IIIF box coordinates of input_box.

    :param pageheight:
    :param pagewidth:
    :param imageheight:
    :param imagewidth:
    :param llx: lower left x coordinate
    :param lly: lower left y coordinate
    :param urx: upper right x coordinate
    :param ury: upper right y coordinate
    :param placedimage_attribs: all attributes of the placed image
    :return: new box coordinates
    :rtype: list



    New box coordinates [x,y,w,h] are in IIIF coordinate system https://iiif.io/api/image/2.0/#region
        (x, y)
        *--------------
        |             |
        |             |
        |             |
        |             |
        |             |
        |             |
        --------------*
                      (x2, y2)

    w = x2 - x
    h = y2 - y
    """
    pix = placedimage_attribs["x"]
    if int(pix) != 0:
        logger.error(
            f"#ERROR: placed image x coordinate is NOT 0 {placedimage_attribs}"
        )
        exit(3)

    factorh = imageheight / (placedimage_attribs["height"])
    factorw = imagewidth / (placedimage_attribs["width"] - pix)

    x = llx * factorw
    y = (pageheight - ury) * factorh
    x2 = urx * factorw
    y2 = (pageheight - ury) * factorw + (ury - lly) * factorw
    h = y2 - y
    w = x2 - x

    return [ceil(x), floor(y), ceil(w), ceil(h)]


def compute_bb(innerbbs):
    """
    Return bounding box (x,y,w,h) from a list of inner bounding boxes
    """

    bblux = min(b[0] for b in innerbbs)
    bbluy = min(b[1] for b in innerbbs)
    bbrlx = max(b[0] + b[2] for b in innerbbs)
    bbrly = max(b[1] + b[3] for b in innerbbs)
    bbw = bbrlx - bblux
    bbh = bbrly - bbluy
    return [bblux, bbluy, bbw, bbh]


def get_placed_image(root):
    """
    Return dimensions of the placed image
    :param root: etree.Element
    :return: dict with all attributes of image xml element
     <PlacedImage image="I0" x="0.00" y="0.00" width="588.84" height="842.00" />
      => {"image":"IO", ,...}
    """

    img = root.find(f".//{TETPREFIX}PlacedImage")
    return {
        "image": img.attrib["image"],
        "x": float(img.attrib["x"]),
        "y": float(img.attrib["y"]),
        "width": float(img.attrib["width"]),
        "height": float(img.attrib["height"]),
    }


def get_tif_shape(root):
    """
    Return original tiff dimensions stored in tetml
    :param root: etree.ELement
    :return: pair of int
    """

    img = root.find(f".//{TETPREFIX}Image[@extractedAs]")
    return int(img.attrib["width"]), int(img.attrib["height"])


def add_gn_property(tokens: list, language: str):
    """
    Set property to indicate the use of whitespace following a token
    """

    # padding the sequence of tokens temporarily with Nones
    tokens.insert(0, None)
    tokens.append(None)
    for i, t in enumerate(tokens):
        if t is None:
            continue
        if not insert_whitespace(
            t["tx"],
            tokens[i + 1]["tx"] if tokens[i + 1] else None,
            tokens[i - 1]["tx"] if tokens[i - 1] else None,
            language,
        ):
            t["gn"] = True

    # unpadding the temporary None tokens
    tokens.pop(0)
    tokens.pop(-1)
