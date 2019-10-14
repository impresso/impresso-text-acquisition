"""Functions to parse TETML data."""

import logging
import os
import lxml.etree

from text_importer.importers.tetml.helpers import (
    get_metadata,
    word2json,
    TETPREFIX,
    compute_bb,
    add_gn_property,
    get_placed_image,
    get_tif_shape,
)

logger = logging.getLogger(__name__)


def tetml_parser(tetml: str) -> dict:
    """Parse an TETML file (e.g. from Swiss Federal Archive).

    The main logic implemented here was derived from
    <https://github.com/impresso/nzz/>. A TETML file
    corresponds loosely to one article given by the boundaries
    of the founding pdf

    :param text: content of the xml file to parse
    :type text: string
    :return: A dictionary with keys: ``meta``, ``r``, ``stats``, ``legacy``.
    :rtype: dict
    """

    parsed = lxml.etree.parse(tetml)
    root = parsed.getroot()

    docmeta = get_metadata(root)

    data = {"meta": docmeta}
    data["meta"]["tetml_path"] = tetml

    data["m"] = {}
    # use title of tetml file as provisionary title attribute
    data["m"]["t"] = os.path.basename(data["meta"]["tetml_path"])
    data["m"]["l"] = "de"  # language attribute
    data["meta"]["id"] = data["m"]["t"].split(".")[0]

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
            error_msg = f"Article with id {data['meta']['id']} has no OCR text."
            logger.error(error_msg)
            return data

        imagewidth, imageheight = get_tif_shape(root)
        pageheight = float(page.get("height"))
        pagewidth = float(page.get("width"))

        jparas = []
        jparas_coords_per_region = []
        jregion = {"p": jparas}

        # catch special cases
        if list(page.iter(f"{TETPREFIX}Para")):
            para_iter = page.iter(f"{TETPREFIX}Para")
        elif not list(page.iter(f"{TETPREFIX}Line")):
            # if the page is empty the paragraph attribute is empty as well
            error_msg = f"Empty PAGE in the following file:\n{tetml}\n{lxml.etree.tostring(page)}"
            logger.error(error_msg)
            continue
        else:
            # If there is only a table tetml doesn't produce paragraph nodes
            # In these cases, continue with page element
            para_iter = [page]

        for para in para_iter:
            jlines = []
            jlines_coords_per_para = []
            jpara = {"l": jlines}
            jparas.append(jpara)
            jhyphenated = (
                None
            )  # contains rest of hyphenated tokens if there was one on earlier line

            for line in para.iter(f"{TETPREFIX}Line"):
                token_coords_per_line = []  # accumulator for word coordinates
                if jhyphenated is not None:
                    jtokens = [jhyphenated]
                    token_coords_per_line = [jhyphenated["c"]]
                    jhyphenated = None
                else:
                    jtokens = []
                jline = {"t": jtokens}

                for word in line.iter(f"{TETPREFIX}Word"):
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
                    error_msg = f"Empty LINE in the following file:\n{tetml}\n{lxml.etree.tostring(line)}"
                    logger.error(error_msg)

            if jlines_coords_per_para:
                jparacoords = compute_bb(jlines_coords_per_para)
                jpara["c"] = jparacoords
                jparas_coords_per_region.append(jparacoords)

        if jparas_coords_per_region:
            jregioncoords = compute_bb(jparas_coords_per_region)
            jregion["c"] = jregioncoords
            jregions.append(jregion)

        jpages.append(jpage)

    return data
