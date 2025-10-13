#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script to check the segmentation documents.

Show visually the segmentation into regions, paragraphs, lines and tokens
on the scanned documents given canonical json files from the Impresso project
"""

import logging
import argparse
import os
import sys
import codecs
import random
import json
import pathlib
import pandas as pd
from PIL import Image, ImageDraw


sys.stderr = codecs.getwriter("UTF-8")(sys.stderr.buffer)
sys.stdout = codecs.getwriter("UTF-8")(sys.stdout.buffer)
sys.stdin = codecs.getreader("UTF-8")(sys.stdin.buffer)


def parse_arguments():
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTIONS] IMAGEFILE JSONFILE",
        description="Calculate something",
        epilog="Contact simon.clematide@uzh.ch",
    )
    parser.add_argument("--version", action="version", version="0.99")
    parser.add_argument(
        "-l", "--logfile", dest="logfile", help="write log to FILE", metavar="FILE"
    )

    parser.add_argument(
        "-j",
        "--jsondir",
        action="store",
        dest="jsondir",
        default=None,
        type=str,
        help="basedir where the json files are saved",
    )

    parser.add_argument(
        "-i",
        "--imgfile",
        action="store",
        dest="imgfile",
        default=None,
        type=str,
        help="filename of image to check against",
    )
    parser.add_argument(
        "--imgdir",
        action="store",
        dest="imgdir",
        default="",
        type=str,
        help="directory where the image files are saved for this issue",
    )

    parser.add_argument(
        "--metafile",
        action="store",
        dest="metafile",
        default=None,
        type=str,
        help="file with metadata needed to evaluate article segmentation",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        dest="quiet",
        default=False,
        help="do not print status messages to stderr",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        default=False,
        help="print debug information",
    )
    parser.add_argument(
        "-R",
        "--region_color",
        action="store",
        dest="debug",
        default="",
        help="print debug information",
    )
    parser.add_argument(
        "-p",
        "--probabilities",
        action="store",
        dest="probabilities",
        default="1,1,1,1",
        type=lambda s: [float(n) for n in s.split(",")],
        help="comma separated list of sampling probabilities region,para,line,word (0 samples nothing, 1 everything)",
    )
    parser.add_argument(
        "--input_suffix",
        action="store",
        dest="input_suffix",
        default=".tif",
        type=str,
        help="suffix of input files for batchwise processing (default: .tif)",
    )
    parser.add_argument(
        "-S",
        "--output_suffix",
        action="store",
        dest="output_suffix",
        default=".jpg",
        type=str,
        help="suffix appended  to the original image filename (default: .tif)",
    )

    parser.add_argument(
        "-D",
        "--output_dir",
        action="store",
        dest="output_dir",
        default="",
        type=str,
        help="output director for decorated image; use the input directory if empty",
    )

    parser.add_argument(
        "-e",
        "--eval",
        action="store",
        dest="eval",
        default=None,
        help="evaluation type ('batch', 'article segment')",
    )

    parser.add_argument(
        "--page_prob",
        action="store",
        dest="page_prob",
        default=0.01,
        type=float,
        help="propability to create an output of a page in batch mode",
    )

    return parser.parse_args()


def read_meta(fname):
    try:
        df = pd.read_csv(
            fname,
            sep="\t",
            parse_dates=["issue_date"],
            dtype={"article_docid": str},
            index_col="article_docid",
        )

    except FileNotFoundError as e:
        msg = (
            "File with additional metadata needs to be placed in "
            f"the top newspaper directory and named {fname}"
        )
        raise FileNotFoundError(msg) from e

    return df


def article_segmentation_eval(dir_json, options, limit=100):
    """
    Draw the pages
    """
    print("Start evaluation process...")
    df = read_meta(options.metafile)

    rows = df.shift(-1).loc[df.pruned]
    rows = rows.sample(limit)
    for _, row in rows.iterrows():
        ftif = pathlib.Path(row["canonical_path_tif"])
        index = ftif.parts.index("data_tif")
        fjson = (
            pathlib.Path(dir_json)
            .joinpath(*ftif.parts[index + 1 :])
            .with_suffix(".json")
        )
        draw_boxes(fjson, options)


def read_json(file):
    f = open(file, "r", encoding="utf-8")
    return json.load(f)


def batch_eval(basedir, options):
    # dirpath, dirnames, filenames
    for dirpath, _, filenames in os.walk(basedir, followlinks=True):
        for file in filenames:
            if random.random() < options.page_prob:
                fjson = pathlib.Path(dirpath, file)
                draw_boxes(fjson, options)


def draw_boxes(jsonfile, options, imgfile=None):
    """

        (x1, y1)
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

    region_prob, para_prob, line_prob, word_prob = options.probabilities
    # region_color, para_color, line_color, word_color = options.colors

    j = read_json(jsonfile)

    if not imgfile:
        imgfile = j["id"] + options.input_suffix
        pathtif = "/".join(j["id"].split("-")[:-1])
        img_path = os.path.join(options.imgdir, pathtif, imgfile)
    else:
        img_path = imgfile

    im = Image.open(img_path).convert("RGB")

    tokens = []

    for r in j["r"]:
        [x, y, w, h] = r["c"]
        dr = ImageDraw.Draw(im, "RGBA")
        if random.random() < region_prob:
            dr.rectangle(
                ((x, y), (x + w, y + h)), outline="black", fill=(244, 3, 3, 30)
            )

        for p in r["p"]:
            if "c" in p:
                [x, y, w, h] = p["c"]
                if random.random() < para_prob:
                    dr.rectangle(
                        ((x, y), (x + w, y + h)), outline="red", fill=(244, 3, 3, 50)
                    )
            for l in p["l"]:
                [x, y, w, h] = l["c"]
                if random.random() < line_prob:
                    dr.rectangle(
                        ((x, y), (x + w, y + h)), outline="green", fill=(3, 244, 3, 50)
                    )

                for t in l["t"]:
                    tokens.append(t["tx"])
                    [x, y, w, h] = t["c"]
                    if random.random() < word_prob:
                        dr.rectangle(((x, y), (x + w, y + h)), outline="black")
                        # print(t["tx"])

    # print(" ".join(tokens))

    imgfile = imgfile.split()[0] + options.output_suffix
    if options.output_dir == "":
        im.save(imgfile + options.output_suffix)
        print(f"# boxed image saved to {imgfile}")
    else:
        pathlib.Path(options.output_dir).mkdir(parents=True, exist_ok=True)
        im.save(str(pathlib.Path(options.output_dir + "/" + imgfile)))
    print("Creating evaluation examples finished.")


def process(options):
    """
    Do the processing
    """

    if options.eval == "batch":
        batch_eval(options.jsondir, options)

    elif options.eval == "art_segment":
        article_segmentation_eval(options.jsondir, options)

    elif options.eval is None:
        draw_boxes(options.jsondir, options, options.imgfile)


def main():
    """
    Invoke this module as a script
    """

    options = parse_arguments()

    if options.logfile:
        logging.basicConfig(filename=options.logfile)
    if options.debug:
        logging.basicConfig(level=logging.DEBUG)

    process(options)


if __name__ == "__main__":
    main()
