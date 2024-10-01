"""Helper functions used by the Tetml Importer.

These functions are mainly used within (i.e. called by) the classes
``TetmlNewspaperIssue`` and ``TetmlNewspaperPage``.
"""

import logging
from math import ceil, floor
import lxml.etree

from text_preparation.tokenization import insert_whitespace

logger = logging.getLogger(__name__)

TETPREFIX = "{http://www.pdflib.com/XML/TET3/TET-3.0}"
FILTER_WORDS = ["#", "ST", "#ST", "ST#", "#ST#"]


def get_metadata(root: lxml.etree.Element) -> dict:
    """Return dict with relevant metadata from page file

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


def filter_special_symbols(jtoken: dict) -> bool:
    """
    Check if token needs to be filtered out as it is a non-content word

    :param dict jtoken: Token text and coordinates.
    :return: bool to indicate stop or content word

    """
    return jtoken["tx"] in FILTER_WORDS


def remove_page_number(jtoken: dict, i_line: int, i_word: int) -> bool:
    """
    Check if page number in the header appears within the first 3 tokens
    of the first line and is not longer than 3 digits.

    :param dict jtoken: Token text and coordinates.
    :param int i_line: Line number.
    :param dict i_word: Word number in line.
    :return: bool to indicate page number.

    """

    word = jtoken["tx"]
    return (
        any(char.isdigit() for char in word)
        and len(word) < 4
        and i_line == 0
        and i_word < 3
    )


def word2json(
    word: lxml.etree.Element,
    pageheight: float,
    pagewidth: float,
    imageheight: float,
    imagewidth: float,
    placed_image_attribs: dict,
    filename: str = None,
) -> dict:
    """
    Return dict with all information about the (hyphenated) TETML word element

    ``{"tx": Text,
    "c": coords,
    "hy" : Bool,
    "hyt": {"nf": Text, "c":coords, "tx":coords}}``

    "hyt" is {} if word is not hyphenated

    :param float pageheight:
    :param float pagewidth:
    :param float imageheight:
    :param float imagewidth:
    :param dict placed_image_attribs:
    :param str filename:
    :param lxml.etree.Element word:
    :return: dictionary with token text and metadata
    """

    result = {}
    boxes = word.findall(f"{TETPREFIX}Box")

    # unhyphenated case
    if len(boxes) == 1:
        tokentext = word.find(f"{TETPREFIX}Text").text
        if tokentext is None:
            error_msg = f"Empty TOKEN in the following file (# boxes: {len(boxes)}):{filename}\n{lxml.etree.tostring(word)}"
            logger.info(error_msg)

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

    # case of a single hyphenation across two subsequent lines
    elif len(boxes) == 2:
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

    # case of multiple hyphenation of a single word that
    # occurs when squeezed in a narrow cell of a table
    elif len(boxes) > 2:
        # treat as a non-hyphenated word as it is delicate to assume
        # a proper table segmentation because of the possibility of merged cells.

        tokentext = word.find(f"{TETPREFIX}Text").text
        result["tx"] = tokentext

        coords_boxes = []
        for box in boxes:
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
            coords_boxes.append(coords)

        coords = compute_bb(coords_boxes)
        result["c"] = coords

        error_msg = f"Wrong number of boxes ({len(boxes)}) in following file: \
        {filename}\nThe reconstructed word is: {result}\n  \
        {lxml.etree.tostring(word)}"

        logger.info(error_msg)

    return result


def compute_box(
    llx: float,
    lly: float,
    urx: float,
    ury: float,
    pageheight: float,
    pagewidth: float,
    imageheight: float,
    imagewidth: float,
    placedimage_attribs: dict,
) -> list:
    """
    Compute IIIF box coordinates of input_box.


    New box coordinates [x,y,w,h] are in IIIF coordinate system https://iiif.io/api/image/2.0/#region

    .. code-block::

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

    :param float pageheight:
    :param float pagewidth:
    :param float imageheight:
    :param float imagewidth:
    :param float llx: lower left x coordinate (lower=smaller)
    :param float lly: lower left y coordinate (lower=smaller)
    :param float urx: upper right x coordinate (upper=bigger)
    :param float ury: upper right y coordinate (upper=bigger)
    :param dict placedimage_attribs: all attributes of the placed image
    :return: list with new box coordinates
    :rtype: list
    """
    pix = placedimage_attribs["x"]
    if int(pix) != 0:
        logger.error(
            f"POTENTIAL ERROR: placed image x coordinate is NOT 0 {placedimage_attribs}"
        )

    ratioh = imageheight / (placedimage_attribs["height"])
    ratiow = imagewidth / (placedimage_attribs["width"])

    x = llx * ratiow
    y = (pageheight - ury) * ratioh
    x2 = urx * ratiow
    y2 = (pageheight - ury) * ratiow + (ury - lly) * ratiow
    h = y2 - y
    w = x2 - x

    return [ceil(x), floor(y), ceil(w), ceil(h)]


def compute_bb(innerbbs: list) -> list:
    """
    Compute coordinates of the bounding box from multiple boxes.

    :param list innerbbs: List of multiple inner boxes (x,y,w,h).
    :return: List of coordinates from the bounding box (x,y,w,h).
    """

    bblux = min(b[0] for b in innerbbs)
    bbluy = min(b[1] for b in innerbbs)
    bbrlx = max(b[0] + b[2] for b in innerbbs)
    bbrly = max(b[1] + b[3] for b in innerbbs)
    bbw = bbrlx - bblux
    bbh = bbrly - bbluy

    return [bblux, bbluy, bbw, bbh]


def get_placed_image(root: lxml.etree.Element) -> dict:
    """
    Return dimensions of the placed image

    ``
    <PlacedImage image="I0" x="0.00" y="0.00" width="588.84" height="842.00" />
    => {"image":"IO", ,...}
    ``
    :param etree.Element: TETML document.
    :return: dict with all attributes of image xml element

    """

    img = root.find(f".//{TETPREFIX}PlacedImage")

    return {
        "image": img.attrib["image"],
        "x": float(img.attrib["x"]),
        "y": float(img.attrib["y"]),
        "width": float(img.attrib["width"]),
        "height": float(img.attrib["height"]),
    }


def get_tif_shape(root: lxml.etree.Element, id_image: str) -> tuple:
    """
    Return original tiff dimensions stored in tetml

    ``
    <Image id="I0" extractedAs=".tif" width="1404" height="2367" colorspace="CS0" bitsPerComponent="1"/>
    ``

    :param root: etree.ELement
    :return: width and height of tiff image.

    """

    img = root.find(f".//{TETPREFIX}Image[@id='{id_image}']")
    return int(img.attrib["width"]), int(img.attrib["height"])


def add_gn_property(tokens: [dict], language: str) -> None:
    """
    Set property to indicate the use of whitespace following a token


    :param list tokens: list of token dictionaries.
    :param str language: abbreviation of languages (de, fr, eng etc.).

    :return: None
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
