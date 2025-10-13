"""Functions to parse TETML data."""

import logging
import os
import lxml.etree

from text_preparation.importers.tetml.helpers import (
    get_metadata,
    word2json,
    TETPREFIX,
    compute_bb,
    add_gn_property,
    get_placed_image,
    get_tif_shape,
    filter_special_symbols,
    remove_page_number,
)

logger = logging.getLogger(__name__)


def tetml_parser(
    tetml: str, filtering: bool = True, ignore_page_number: bool = True, language="de"
) -> dict:
    """
    Parse a TETML file (e.g. from Swiss Federal Archive).

    The main logic implemented here was derived from
    https://github.com/impresso/nzz/. A TETML file
    corresponds loosely to one article given by the boundaries
    of the founding pdf.

    :param text tetml: path to tetml file that needs to be parsed
    :param filtering bool: call method to filter out pre-defined tokens
    :return: A dictionary with keys: ``metadata``, ``pages (content)``, ``meta (additional metadata)``.
    :rtype: dict
    """

    parsed = lxml.etree.parse(tetml)
    root = parsed.getroot()

    data = {}
    # canonical metadata
    data["m"] = {}
    # use title of tetml file as provisionary title attribute
    data["m"]["t"] = os.path.basename(tetml)
    data["m"]["l"] = language

    # other metadata
    docmeta = get_metadata(root)
    data["meta"] = docmeta
    data["meta"]["id"] = data["m"]["t"].split(".")[0]
    data["meta"]["tetml_path"] = tetml

    jpages = []
    data["pages"] = jpages

    for page in root.iter(f"{TETPREFIX}Page"):

        # a region corresponds to an entire page initially
        jregions = []
        jpage = {"r": jregions}

        try:
            # get page coordinates to calculate standardized coordinates of the boxes
            placed_image_attribs = get_placed_image(page)
        except AttributeError:
            logger.info(f"Article of the following file has no OCR text:\n{tetml}")
            return data

        imagewidth, imageheight = get_tif_shape(root, placed_image_attribs["image"])

        # TODO: in our case, this is actually identical to placed_image_attribs
        pageheight = float(page.get("height"))
        pagewidth = float(page.get("width"))

        jparas = []
        jparas_coords_per_region = []
        jregion = {"p": jparas}

        # cover special cases
        if list(page.iter(f"{TETPREFIX}Para")):
            # regular case with at least one paragraph on its page
            para_iter = page.iter(f"{TETPREFIX}Para")
        elif not list(page.iter(f"{TETPREFIX}Line")):
            # case of an empty page
            error_msg = f"Empty PAGE in the following file: {tetml}\n{lxml.etree.tostring(page)}"
            logger.info(error_msg)
            jpages.append(jpage)  # add page with empty regions tag
            continue
        else:
            # case of tables that cover the entire page without generating
            # a paragraph node
            para_iter = [page]

        for para in para_iter:
            jlines = []
            jlines_coords_per_para = []
            jpara = {"l": jlines}

            # contains the former part(s) of hyphenated tokens from the previous line(s)
            jhyphenated = None

            for i_line, line in enumerate(para.iter(f"{TETPREFIX}Line")):
                token_coords_per_line = []  # accumulator for word coordinates
                if jhyphenated is not None:
                    jtokens = [jhyphenated]
                    token_coords_per_line = [jhyphenated["c"]]
                    jhyphenated = None
                else:
                    jtokens = []
                jline = {"t": jtokens}

                for i_word, word in enumerate(line.iter(f"{TETPREFIX}Word")):
                    jworddict = word2json(
                        word,
                        pageheight,
                        pagewidth,
                        imageheight,
                        imagewidth,
                        placed_image_attribs,
                        filename=docmeta["tetml_path"],
                    )

                    if jworddict is None:
                        continue
                    if filtering and filter_special_symbols(jworddict):
                        continue
                    if ignore_page_number and remove_page_number(
                        jworddict, i_line, i_word
                    ):
                        continue

                    token_coords_per_line.append(jworddict["c"])
                    jtoken = {"tx": jworddict["tx"], "c": jworddict["c"]}

                    if "hyt" in jworddict:
                        # will be inserted at the begin of next line
                        jhyphenated = jworddict["hyt"]
                        jtoken["hy"] = True

                    jtokens.append(jtoken)

                add_gn_property(jtokens, data["m"]["l"])  # language attribute

                if token_coords_per_line:
                    linecoords = compute_bb(token_coords_per_line)
                    jline["c"] = linecoords
                    jlines_coords_per_para.append(linecoords)
                    jlines.append(jline)
                else:
                    # may be caused by the removal of some words due to filter_special_symbols()
                    error_msg = f"Empty LINE in the following file:{tetml}\n{lxml.etree.tostring(line)}"
                    logger.info(error_msg)

            if jlines_coords_per_para:
                jparacoords = compute_bb(jlines_coords_per_para)
                jpara["c"] = jparacoords
                jparas_coords_per_region.append(jparacoords)
                jparas.append(jpara)

        if jparas_coords_per_region:
            jregioncoords = compute_bb(jparas_coords_per_region)
            jregion["c"] = jregioncoords
            jregions.append(jregion)

        jpages.append(jpage)

    return data
