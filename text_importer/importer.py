"""
Functions and CLI script to convert Olive OCR data into Impresso's format.

Usage:
    import.py --input-dir=<id> --output-dir==<od> [--log-file=<f> --temp-dir==<td> --verbose --parallelize --filter=<ft>]

Options:
    --input-dir=<id>    Base directory containing one sub-directory for each journal.
    --output-dir==<od>  Base directory where to write the output files.
    --log-file=<f>      Log file; when missing print log to stdout
    --verbose           Verbose log messages (good for debugging).
    --parallelize       Parallelize the import.
    --filter=<ft>       Criteria to filter issues before import ("journal=GDL; date=1900/01/01-1950/12/31;")
"""


import json
import logging
import os
import shutil
from datetime import date

import dask
# import ipdb as pdb  # remove from production version
import python_jsonschema_objects as pjs
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get as mp_get
from docopt import docopt
from impresso_commons.path import detect_issues

from text_importer.importers.olive import olive_import_issue

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"
__version__ = "0.3.0"

logger = logging.getLogger()

punctuation_nows_before = [".", ",", ")", "]", "}", "°", "..."]
punctuation_nows_after = ["(", "[", "{"]
punctuation_nows_beforeafter = ["'", "-"]
punctuation_ciffre = [".", ","]

html_escape_table = {
    "&amp;": "&",
    "&quot;": '"',
    "&apos;": "'",
    "&gt;": ">",
    "&lt;": "<",
}


def schemas_to_classes(schema_folder="./text_importer/schemas/"):
    """Generate a list of python classes starting from JSON schemas.

    :param schema_folder: path to the schema folder (default="./schemas/")
    :type schema_folder: string
    :rtype: `python_jsonschema_objects.util.Namespace`
    """
    with open(os.path.join(schema_folder, "article.schema"), 'r') as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    return builder.build_classes()


def print_article(article):
    """Only for debug, remove later."""
    print(article["legacy"]["id"])
    for r in article["regions"]:
        for p in r["paragraphs"]:
            print("------------")
            assert p["lines"] is not None
            for line in p["lines"]:
                print(" ".join(t['text'] for t in line["tokens"]))
            print("------------")
        print("#############")


def _parse_filter(filter_string):
    filters = {
        f.split("=")[0].strip(): f.split("=")[1].strip()
        for f in filter_string.split(";")
    }

    return filters


def _apply_filters(filter_dict, issues):

    filtered_issues = []

    if "journal" in filter_dict:
        filtered_issues = [
            i for i in issues if i.journal == filter_dict["journal"]
        ]
    else:
        filtered_issues = issues

    if "date" in filter_dict:

        # date filter is a range
        if "-" in filter_dict["date"]:
            start, end = filter_dict["date"].split("-")
            start = date(*[int(x) for x in start.split("/")])
            end = date(*[int(x) for x in end.split("/")])
            print(start, end)
            filtered_issues = [
                i
                for i in filtered_issues
                if i.date >= start and i.date <= end
            ]

        # date filter is not a range
        else:
            filter_date = date(*[
                int(x) for x in filter_dict["date"].split("/")
            ])

            filtered_issues += [
                i
                for i in issues
                if i.date == filter_date
            ]

    return filtered_issues


def main(args):
    """Execute the main with CLI parameters."""
    # store CLI parameters
    inp_dir = args["--input-dir"]
    outp_dir = args["--output-dir"]
    temp_dir = args["--temp-dir"]
    log_file = args["--log-file"]
    parallel_execution = args["--parallelize"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO

    # Initialise the logger
    global logger
    logger.setLevel(log_level)

    if(log_file is not None):
        handler = logging.FileHandler(filename=log_file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Logger successfully initialised")

    logger.debug("CLI arguments received: {}".format(args))

    # clean output directory if existing
    if os.path.exists(outp_dir):
        shutil.rmtree(outp_dir)

    # clean temp directory if existing
    if temp_dir is not None and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    # detect issues to be imported
    issues = detect_issues(inp_dir)
    logger.info(
        "Found {} newspaper issues to import".format(
            len(issues)
        )
    )

    # filter issues before importing
    if arguments["--filter"] is not None:
        f = _parse_filter(arguments["--filter"])
        issues = _apply_filters(f, issues)
        logger.info(
            "{} newspaper remained after applying filter {}".format(
                len(issues),
                f
            )
        )

    if len(issues) == 0:
        print("No issues to import (filtered too much perhaps?)")
        return

    logger.debug("Following issues will be imported:{}".format(issues))

    result = [
        olive_import_issue(i, outp_dir, temp_dir)
        for i in issues
    ]
    """
    # prepare the execution of the import function
    tasks = [
        delayed(olive_import_issue)(i, outp_dir, temp_dir)
        for i in issues
    ]

    print(
        "\nImporting {} newspaper issues...(parallelized={})".format(
            len(issues),
            parallel_execution
        )
    )
    with ProgressBar():
        if parallel_execution:
            result = compute(*tasks, get=mp_get)
        else:
            result = compute(*tasks, get=dask.get)
    print("Done.\n")
    """

    logger.debug(result)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
