#!/usr/bin/python3

__author__ = "Phillip Ströbel/Simon Clematide"
__email__ = "pstroebel@cl.uzh.ch"
__organisation__ = "Institute of Computational Linguistics, University of Zurich"
__copyright__ = "UZH, 2018"
__status__ = "development"

import argparse
import json
import logging
import os
import re
import sys
import zipfile
from collections import defaultdict
from math import ceil, floor
from time import strftime

import jsonschema
import lxml.etree

logging.basicConfig(format="%(asctime)s %(message)s")

"""
tetxml contains information about the extracted tiff file. No need to access the physical file in this script
<Images>
 <Image id="I0" extractedAs=".tif" width="2101" height="2362" colorspace="CS0" bitsPerComponent="4"/>
 </Images>

$ identify  NZZ-TIFS/1781/01/13/a/NZZ-1781-01-13-a-p0002.tif
NZZ-TIFS/1781/01/13/a/NZZ-1781-01-13-a-p0002.tif TIFF 2101x2362 2101x2362+0+0 4-bit Grayscale Gray 994KB 0.000u 0:00.000


Hyphenation in tetml
  <Word>
   <Text>Verdienst</Text>
   <Box llx="314.07" lly="578.82" urx="334.68" ury="589.73">
    <Glyph font="F0" size="10.91" x="314.07" y="578.82" width="7.28">V</Glyph>
    <Glyph font="F0" size="10.91" x="321.35" y="578.82" width="6.07">e</Glyph>
    <Glyph font="F0" size="10.91" x="327.41" y="578.82" width="3.63" dehyphenation="pre">r</Glyph>
    <Glyph font="F0" size="10.91" x="331.05" y="578.82" width="3.63" dehyphenation="artifact">-</Glyph>
   </Box>
   <Box llx="104.78" lly="563.54" urx="129.03" ury="572.63">
    <Glyph font="F0" size="9.09" x="104.78" y="563.54" width="5.05" dehyphenation="post">d</Glyph>
    <Glyph font="F0" size="9.09" x="109.83" y="563.54" width="2.02">i</Glyph>
    <Glyph font="F0" size="9.09" x="111.85" y="563.54" width="5.05">e</Glyph>
    <Glyph font="F0" size="9.09" x="116.91" y="563.54" width="5.05">n</Glyph>
    <Glyph font="F0" size="9.09" x="121.96" y="563.54" width="4.55">s</Glyph>
    <Glyph font="F0" size="9.09" x="126.51" y="563.54" width="2.53">t</Glyph>
   </Box>
  </Word>


                        {
                           "c": [
                              847,
                              832,
                              157,
                              35
                           ],
                           "tx": "nouveaux",
                           "s": 5
                        },
                        {
                           "hy": true,
                           "c": [
                              1022,
                              832,
                              53,
                              35
                           ],
                           "tx": "dé-",
                           "s": 5
                        }
                     ]
                  },
                  {
                     "c": [
                        62,
                        872,
                        552,
                        35
                     ],
                     "t": [
                        {
                           "nf": "détails",
                           "c": [
                              62,
                              872,
                              60,
                              35
                           ],
                           "tx": "tails",
                           "s": 5
                        },
                        {
                           "c": [
                              132,
                              872,
                              47,
                              35
                           ],
                           "tx": "sur",
                           "s": 5
                        },


"""

TETPREFIX = "{http://www.pdflib.com/XML/TET3/TET-3.0}"


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


def get_metadata(root):
    """Return dict with relevant metadata for page file"""

    result = {}
    creation = root.find(f".//{TETPREFIX}Creation")
    if creation is not None:
        result["tetcdt"] = creation.attrib["date"]
    pdfdoc = root.find(f".//{TETPREFIX}Document")
    if pdfdoc is not None:
        result["pdf"] = pdfdoc.attrib["filename"]
    pdfcreation = root.find(r".//{TETPREFIX}CreationDate")
    if pdfcreation is not None:
        result["pdfcdt"] = pdfcreation.text
    return result


def tetfile2infos(tetfile, arguments):
    """Analyse tetfile path into a dict of corresponding informations

    :param tetfile: filename as str
    :param arguments: argparser options
    :return: dict with

    "NZZ/1814/12/30/a/NZZ-1814-12-30-a-p0003.tetml" => {'year': '1814', 'month':'12', 'day':'30', 'issue':'a', 'page':'0003'}
    """
    properties = ["collection", "year", "month", "day", "issue", "page"]

    if arguments.no_collection_dir:
        m = re.search(
            r"^(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/(?P<issue>\w)/(?P<collection>\w+?)-(?P=year)-(?P=month)-(?P=day)-(?P=issue)-p(?P<page>\d{4})\.tetml$",
            tetfile,
        )
        if m is None:
            print(
                "ERROR: Malformed tetfile (no collection dir) %s" % (tetfile,),
                file=sys.stderr,
            )
            exit(1)
    else:
        m = re.search(
            r"^(?P<collection>\w+?)/(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/(?P<issue>\w)/"
            + r"(?P=collection)-(?P=year)-(?P=month)-(?P=day)-(?P=issue)-p(?P<page>\d{4})\.tetml$",
            tetfile,
        )
        if m is None:
            print(
                "ERROR: Malformed tetfile (collection dir) %s" % (tetfile,),
                file=sys.stderr,
            )
            exit(1)
    return {k: m.group(k) for k in properties}


# noinspection PySimplifyBooleanCheck,PySimplifyBooleanCheck
def next_segment(ilist):
    """
    Yield sublists of ilist with monotonically increasing values

    :param ilist:
    :return:


    """
    highest = -1
    result = []
    for i, e in enumerate(ilist):
        if e > highest:
            highest = e
            result.append(e)
            continue
        else:
            if result != []:
                yield result
                highest = -1
                result = []
    else:
        if result != []:
            yield result


def split_list_by_indicator(longlist, indicator):
    """
    Return list of sublist of all elements segmented by binary indicator function

    If indicator function that compares two adjacent elements returns True at position i, all elements before position i are grouped into a sublist.
    :return:

    edge cases are treated as follows: [] -> [[]], [1] -> [[1]]

    lambda x,y: x > y
    [1,2,3,2,4,1,2,3] => [[1,2,3],[2,4],[1,2,3]]
    """
    if len(longlist) < 2:
        return [longlist]
    ll = []
    sublist = []
    for i, e in enumerate(longlist[:-1]):
        if indicator(e, longlist[i + 1]):
            sublist.append(e)
            ll.append(sublist)
            sublist = []
        else:
            sublist.append(e)
    sublist.append(longlist[-1])

    ll.append(sublist)
    return ll


def create_para_lines(oldparagraph):
    """Return new representation of oldparagraph with explicit lines


    Old paragraph is a list of token dicts {"l": <TOKENLIST>} which is an list element in the "p" attribute of its parent region.

    A new paragraph  is a dict {"c": <PARACOORD>, "l": [<LINEDICT1>,..., <LINEDICTN>] which is an list element in the "p" attribute of its parent region


        (ulx, uly)
        *--------------
        |             |
        |             |
        |             |
        |             |
        |             |
        |             |
        --------------*
                      (x2, y2)
    w=x2-x1
    h=y2-y1
    x2 = x1 + w
    y2 = y1 + h

    """

    npd = {}  # does not have paragraph coordinates for now
    # originally, each paragraph contains a list of token dictionaries td
    # we need to organize this list into sublists of tokens belonging to the
    # same line
    tds = oldparagraph["t"]  # list of token dicts in old format
    # conversion into line lists:
    # tdlines contains lists of token dicts ; each list represents a line
    tdlines = split_list_by_indicator(tds, lambda t1, t2: t2["c"][0] < t1["c"][0])

    # the new paragraph will be lists of line dicts
    # each line dict ld will have its coordinates "c" which need to be computed from the token coordinates
    # the ld["t"] will containt the original token dicts
    lds = []
    pulx = None
    puly = None
    pwidth = None
    pheight = None
    for tdline in tdlines:
        # current line dict ld contains all tokens
        ld = {"t": tdline}
        # compute the coordinates of the line
        lulx = tdline[0]["c"][0]
        luly = min(t["c"][1] for t in tdline)
        llrx = tdline[-1]["c"][0] + tdline[-1]["c"][2]
        llry = max(t["c"][1] + t["c"][3] for t in tdline)
        lwidth = llrx - lulx
        lheight = llry - luly
        if pulx is None:
            pulx = lulx
            puly = luly
            pwidth = lwidth
            pheight = lheight
        else:
            if lulx < pulx:
                pulx = lulx
            if luly < puly:
                puly = luly
            if lwidth > pwidth:
                pwidth = lwidth
            if lheight > pheight:
                pheight = lheight
        ld["c"] = [lulx, luly, lwidth, lheight]
        lds.append(ld)
    npd["l"] = lds
    npd["c"] = [pulx, puly, pwidth, pheight]

    return npd


def insert_lines(canonical):
    """
    Insert lines lists in canonical format according to ascending llx

    token coordinates "c": x, y, width, height
    :param canonical:
    :return:

    Each "p" attribute contains a list of line dicts.
    The dicts of a level x of text are named xd in the following.

    {
   "cc": true,
   "r": [
      {
         "c": [
            115,
            132,
            3093,
            448
         ],
         "p": [
            {
               "l": [
                  {
                     "c": [
                        180,
                        213,
                        2893,
                        202
                     ],
                     "t": [
                        {
                           "c": [
                              180,
                              218,
                              1103,
                              197
                           ],
                           "tx": "GAZETTE",
                           "s": 4
                        },
                        {
                           "c": [
                              1567,
                              213,
                              142,
                              198
                           ],
                           "tx": "E",
                           "s": 4
                        },
                        {
                           "c": [
                              1843,
                              213,
                              1230,
                              198
                           ],
                           "tx": "LAUSANNE",
                           "s": 4
                        }
                     ]
                  },
                  {
                     "c": [
                        772,
                        437,
                        1707,
                        138
                     ],
                     "t": [

    """
    for rd in canonical["r"]:

        nparas = []
        rd["op"] = rd["p"]
        rd["p"] = nparas
        # for each paragraph dict in one region
        for pd in rd["op"]:
            np = create_para_lines(pd)
            nparas.append(np)
            # print(np,file=sys.stderr)
    # fix paragraph coords
    for rd in canonical["r"]:
        for pd in rd["p"]:
            lbbs = []
            for ld in pd["l"]:
                lbbs.append(ld["c"])

            pd["c"] = compute_bb(lbbs)
            print("#INFOPBB", lbbs, pd["c"], file=sys.stderr)


def compute_bb(innerbbs):
    """Return bounding box (x,y,w,h) from a list of inner bounding boxes



    """

    bblux = min(b[0] for b in innerbbs)
    bbluy = min(b[1] for b in innerbbs)
    bbrlx = max(b[0] + b[2] for b in innerbbs)
    bbrly = max(b[1] + b[3] for b in innerbbs)
    bbw = bbrlx - bblux
    bbh = bbrly - bbluy
    return [bblux, bbluy, bbw, bbh]


def insert_linestart(canonical):
    """
    Returns modified JSON representation with lines inferred from x coordinates

    Idea: Treat everything as being on one line within a given paragraph a long as x coordinates are ascending.

    :param canonical: json dict representing an article
    :return: json dict representing an enriched article
    """
    paragraphs = canonical["r"]

    for pi in paragraphs:
        for p in pi["p"]:
            last_llx = None
            tokeninfos = p["t"]
            for i, t in enumerate(tokeninfos):

                if last_llx is None or t["oc"][0] < last_llx:
                    t["ls"] = True
                    last_llx = t["oc"][0]
                else:
                    t["ls"] = False


def wrap_lines_in_paras(root):
    """
    Wrap each line element with a para element
    :param root:
    :return:
    """
    lines = root.findall(".//%sLine" % TETPREFIX)
    for line in lines:
        para = lxml.etree.Element("%sPara" % TETPREFIX)
        line.addprevious(para)
        para.insert(0, line)  # this moves the table


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
    boxes = word.findall("%sBox" % TETPREFIX)
    if len(boxes) == 1:
        tokentext = word.find("%sText" % TETPREFIX).text
        if tokentext is None:
            print(
                "%s,Empty TOKEN: %d" % (filename, len(boxes)),
                lxml.etree.tostring(word),
                file=sys.stderr,
            )
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
    elif len(boxes) >= 2:
        # print("Hyphenated found", file=sys.stderr)
        result["hy"] = True
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
            print(
                f"{filename},Wrong number of boxes: {len(boxes):d}",
                lxml.etree.tostring(word),
                file=sys.stderr,
            )
    else:
        print(
            f"{filename},Wrong number of boxes: {len(boxes):d}",
            lxml.etree.tostring(word),
            file=sys.stderr,
        )
        # @TODO treat the rare case where we have 3 boxes
    # NZZ/1884/02/21/b/NZZ-1884-02-21-b-p0002.tetml
    #         '<Word xmlns="http://www.pdflib.com/XML/TET3/TET-3.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    #    <Text>rechtschaffeneund</Text>
    #    <Box llx="564.27" lly="167.66" urx="575.26" ury="171.96">
    #     <Glyph font="F0" size="4.3" x="564.27" y="167.66" width="1.43">r</Glyph>
    #     <Glyph font="F0" size="4.3" x="565.70" y="167.66" width="2.39">e</Glyph>
    #     <Glyph font="F0" size="4.3" x="568.09" y="167.66" width="2.15">c</Glyph>
    #     <Glyph font="F0" size="4.3" x="570.24" y="167.66" width="2.39">h</Glyph>
    #     <Glyph font="F0" size="4.3" x="572.63" y="167.66" width="1.20" dehyphenation="pre">t</Glyph>
    #     <Glyph font="F0" size="4.3" x="573.83" y="167.66" width="1.43" dehyphenation="artifact">-</Glyph>
    #    </Box>
    #    <Box llx="444.28" lly="161.60" urx="464.29" ury="165.69">
    #     <Glyph font="F0" size="4.09" x="444.28" y="161.60" width="2.05" dehyphenation="post">s</Glyph>
    #     <Glyph font="F0" size="4.09" x="446.32" y="161.60" width="2.05">c</Glyph>
    #     <Glyph font="F0" size="4.09" x="448.37" y="161.60" width="2.27">h</Glyph>
    #     <Glyph font="F0" size="4.09" x="450.64" y="161.60" width="2.27">a</Glyph>
    #     <Glyph font="F0" size="4.09" x="452.92" y="161.60" width="1.14">f</Glyph>
    #     <Glyph font="F0" size="4.09" x="454.06" y="161.60" width="1.14">f</Glyph>
    #     <Glyph font="F0" size="4.09" x="455.19" y="161.60" width="2.27">e</Glyph>
    #     <Glyph font="F0" size="4.09" x="457.47" y="161.60" width="2.27">n</Glyph>
    #     <Glyph font="F0" size="4.09" x="459.74" y="161.60" width="2.27">e</Glyph>
    #     <Glyph font="F0" size="4.09" x="462.01" y="161.60" width="2.27">n</Glyph>
    #    </Box>
    #    <Box llx="489.18" lly="162.48" urx="497.39" ury="167.40">
    #     <Glyph font="F0" size="4.92" x="489.18" y="162.48" width="2.74" dehyphenation="post">u</Glyph>
    #     <Glyph font="F0" size="4.92" x="491.92" y="162.48" width="2.74">n</Glyph>
    #     <Glyph font="F0" size="4.92" x="494.65" y="162.48" width="2.74">d</Glyph>
    #    </Box>
    #   </Word>

    return result


def tet2json(arguments):
    """
    Takes TET output and creates two types of json files: one json per issue, and one json per page.
    :param arguments:
    :return:
    """
    page_schema = None
    issue_schema = None
    if arguments.schemas is not None:
        with open(os.path.join(arguments.schemas, "page.schema"), "r") as psf:
            page_schema = json.loads(psf.read())

        with open(os.path.join(arguments.schemas, "issue.schema"), "r") as isf:
            issue_schema = json.loads(isf.read())

    with zipfile.ZipFile(arguments.inputZip, "r") as tetml_zip:
        output = arguments.outputZip
        outdir = os.path.dirname(output)
        tetfiles = sorted(
            f
            for f in tetml_zip.namelist()
            if f.endswith(".tetml") and not f.startswith("__")
        )
        issue_dict = build_issue_dict(tetfiles, arguments)

        with zipfile.ZipFile(
            os.path.join(output),
            "w",
            compression=(
                zipfile.ZIP_LZMA if arguments.compression else zipfile.ZIP_STORED
            ),
        ) as json_zip:
            os.makedirs(os.path.join(outdir, arguments.collection), exist_ok=True)
            # json_zip.write(os.path.join(outdir, '%s/' %(arguments.collection, )), arcname=arguments.collection)
            for tetfile in tetfiles:

                logging.warning("DEBUG tetfile" + tetfile)
                tetinfodict = tetfile2infos(tetfile, arguments)
                issue_path = "{collection}/{year}/{month}/{day}/{issue}".format(
                    **tetinfodict
                )
                # os.makedirs(os.path.join(outdir, issue_path), exist_ok=True)
                # json_zip.write(os.path.join(outdir, issue_path), arcname=issue_path, exist_ok=True)
                tetml = tetml_zip.open(tetfile)
                parsed = lxml.etree.parse(tetml)
                root = parsed.getroot()
                if root.find(f".//{TETPREFIX}Para") is None:
                    wrap_lines_in_paras(root)
                docmeta = get_metadata(root)
                page_json = {
                    "cc": True,
                    "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                    "id": "{collection}-{year}-{month}-{day}-{issue}-p{page}".format(
                        **tetinfodict
                    ),
                    "src": docmeta,
                }  # if True, "c" coordinates follow the IIIF format ulx,uly,width, height with origin 0,0 at the left corner of the page

                jregions = []

                page_json["r"] = jregions
                for page in root.iter(f"{TETPREFIX}Page"):

                    placed_image_attribs = get_placed_image(page)
                    imagewidth, imageheight = get_tif_shape(root)
                    pageheight = float(page.get("height"))
                    pagewidth = float(page.get("width"))

                    jparas = []
                    jparas_coords_per_region = []
                    jregion = {
                        "p": jparas,
                        "pOf": "{collection}-{year}-{month}-{day}-{issue}-i{page}".format(
                            **tetinfodict
                        ),
                    }

                    for para_index, para in enumerate(page.iter(f"{TETPREFIX}Para")):
                        jlines = []
                        jlines_coords_per_para = []
                        jpara = {"l": jlines}
                        jparas.append(jpara)
                        jhyphenated = (
                            None
                        )  # contains rest of hyphenated tokens if there was one on earlier line
                        for line in para.iter(f"{TETPREFIX}Line"):
                            token_coords_per_line = (
                                []
                            )  # accumulator for word coordinates
                            if jhyphenated is not None:
                                jtokens = [jhyphenated]
                                # print("Hyphenated found",jtokens, file=sys.stderr)
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
                                    filename=tetfile,
                                )
                                if jworddict is None:
                                    continue
                                token_coords_per_line.append(jworddict["c"])
                                jtoken = {"tx": jworddict["tx"], "c": jworddict["c"]}
                                if "hyt" in jworddict:
                                    # will be inserted at the start of next
                                    # line
                                    jhyphenated = jworddict["hyt"]
                                    jtoken["hy"] = True
                                jtokens.append(jtoken)

                            add_gn_property(jtokens, "de")

                            if len(token_coords_per_line) > 0:
                                linecoords = compute_bb(token_coords_per_line)
                                jline["c"] = linecoords
                                jlines_coords_per_para.append(linecoords)
                                jlines.append(jline)
                            else:
                                print("#EMPTY LINE", jline, file=sys.stderr)
                        jparacoords = compute_bb(jlines_coords_per_para)
                        jparas_coords_per_region.append(jparacoords)
                        jpara["c"] = jparacoords
                    if len(jparas_coords_per_region) > 0:
                        regioncoords = compute_bb(jparas_coords_per_region)
                        jregion["c"] = regioncoords
                        jregions.append(jregion)
                    else:
                        print("#EMPTY REGION", jregion, file=sys.stderr)

                page_json_obj = json.dumps(
                    page_json,
                    indent=None if not arguments.verbose else True,
                    separators=(",", ":") if not arguments.verbose else (", ", ": "),
                    ensure_ascii=False,
                )

                if arguments.schemas is not None:
                    jsonschema.validate(page_json, page_schema)
                page_file_string = "{collection}-{year}-{month}-{day}-{issue}-p{page}.json".format(
                    **tetinfodict
                )
                json_zip.writestr(
                    "{collection}/{year}/{month}/{day}/{issue}/{page_file_string}".format(
                        **tetinfodict, page_file_string=page_file_string
                    ),
                    page_json_obj,
                )

            for yearmonth, day_issue in issue_dict.items():
                y, m = yearmonth.split("/")

                for d, issue_pages in day_issue.items():

                    for i, pages in issue_pages.items():
                        issue_page_ids = []
                        issue_json = {
                            "id": f"{arguments.collection}-{y}-{m}-{d}-{i}",
                            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                            "i": [],
                            "pp": issue_page_ids,
                        }
                        for page in pages:
                            issue_page_ids.append(
                                f"{arguments.collection}-{y}-{m}-{d}-{i}-p{page}"
                            )
                            p_dict = {
                                "m": {
                                    "id": f"{arguments.collection}-{y}-{m}-{d}-{i}-i{page}",
                                    "pp": [int(page)],
                                    "tp": "pg",
                                    "l": "de",
                                }
                            }
                            issue_json["i"].append(p_dict)
                        #   issue_json["pp"].append("%s-%s-%s-%s-%s-p%s" % (arguments.collection, y, m, d, i, page))
                        # issue_json['pp'] = issue_page_ids
                        issue_json_obj = json.dumps(
                            issue_json,
                            indent=None if not arguments.verbose else True,
                            separators=(",", ":")
                            if not arguments.verbose
                            else (", ", ": "),
                            ensure_ascii=False,
                        )
                        if arguments.schemas is not None:
                            jsonschema.validate(issue_json, issue_schema)
                        issue_file_string = (
                            f"{arguments.collection}-{y}-{m}-{d}-{i}-issue.json"
                        )
                        json_zip.writestr(
                            f"{arguments.collection}/{y}/{m}/{d}/{i}/{issue_file_string}",
                            issue_json_obj,
                        )

        # shutil.rmtree(os.path.join(os.path.dirname(arguments.outputZip), arguments.collection, os.path.basename(arguments.inputZip[:-4])))


def build_issue_dict(filenames, arguments):
    """
    builds issue dict from filename list

    :param arguments:
    :param filenames:
    :return:
    """
    issue_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for fn in filenames:
        print(f"INFO filename {fn}", file=sys.stderr)
        i_dict = tetfile2infos(fn, arguments)

        issue_dict[i_dict["year"] + "/" + i_dict["month"]][i_dict["day"]][
            i_dict["issue"]
        ] += [i_dict["page"]]

    return issue_dict


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

  w = x2 -x
  h = y2-y
    """
    pix = placedimage_attribs["x"]
    if int(pix) != 0:
        print(
            "#ERROR: placed image x coordinate is NOT 0",
            placedimage_attribs,
            file=sys.stderr,
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


# Copied over from https://raw.githubusercontent.com/impresso/impresso-text-acquisition/master/text_importer/tokenization.py?token=AC5nkACQK1X58kMetyhmPAZY3HAnxCY-ks5cBDqMwA%3D%3D
WHITESPACE_RULES = {
    "fr": {
        "punctuation_nows_before": [".", ",", ")", "]", "}", "°", "..."],
        "punctuation_nows_after": ["(", "[", "{"],
        "punctuation_nows_beforeafter": ["'", "-"],
        "punctuation_ciffre": [".", ","],
    },
    "de": {
        "punctuation_nows_before": set(
            [".", ";", ":", ",", ")", "]", "}", "°", "...", "»"]
        ),
        "punctuation_nows_after": set(["(", "[", "{", "«"]),
        "punctuation_nows_beforeafter": set(["-"]),
        "punctuation_ciffre": set([".", ","]),
    },
}


def add_gn_property(tokens, language):

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


def insert_whitespace(token, following_token, previous_token, language):
    """Determine whether a whitespace should be inserted after a token."""
    try:
        wsrules = WHITESPACE_RULES[language]
    except Exception:
        pass
        return

    insert_ws = True

    if (
        token in wsrules["punctuation_nows_beforeafter"]
        or following_token in wsrules["punctuation_nows_beforeafter"]
    ):
        insert_ws = False

    elif following_token in wsrules["punctuation_nows_before"]:
        insert_ws = False

    elif token in wsrules["punctuation_nows_after"]:
        insert_ws = False

    elif (
        token in wsrules["punctuation_ciffre"]
        and previous_token is not None
        and following_token is not None
    ):
        if previous_token.isdigit() and following_token.isdigit():
            return False
        else:
            return True
    # print(f"Insert whitespace: curr={token}, follow={following_token}, \
    #   prev={previous_token} ({insert_ws})", file=sys.stderr)
    return insert_ws


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "-i",
        "--inputZip",
        help="zipped input file where TETML are stored (per year) from which json needs to be produced",
    )
    argparser.add_argument(
        "-o",
        "--outputZip",
        default="out.d",
        help="output folder incl. filename where json-files generated should be stored (in ZIP)",
    )
    argparser.add_argument(
        "-s", "--schemas", default=None, help="location of json schema files"
    )
    argparser.add_argument(
        "-c",
        "--collection",
        help="Specify collection name if not included in input TETML archive",
    )
    argparser.add_argument(
        "-C",
        "--compression",
        default=False,
        action="store_true",
        help="compress zip archive ",
    )
    argparser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help="Output more verbose JSON and diagnostics for easier inspection",
    )
    argparser.add_argument(
        "-n",
        "--no_collection_dir",
        default=False,
        action="store_true",
        help=(
            "TETML input archives have no top directory named by collection acronym,",
            "only the year directory. Specify the collection name.",
        ),
    )

    args = argparser.parse_args()

    tet2json(args)


if __name__ == "__main__":
    main()
