"""
Functions and CLI script to convert BNL's Mets/Alto OCR data into Impresso's format.

Usage:
    luximporter.py --input-dir=<id> (--clear | --incremental) [--output-dir==<od> --s3-bucket=<b> --config-file=<cf> --log-file=<f> --verbose --scheduler=<sch>]
    luximporter.py --version

Options:
    --input-dir=<id>    Base directory containing one sub-directory for each journal
    --output-dir=<od>   Base directory where to write the output files
    --config-file=<cf>  configuration file for selective import
    --s3-bucket=<b>     If provided, writes output to an S3 drive, in the specified bucket
    --scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
    --log-file=<f>      Log file; when missing print log to stdout
    --verbose           Verbose log messages (good for debugging)
    --clear             Removes the output folder (if already existing)
    --version
"""  # noqa: E501

import json
import logging
import os
import shutil

from dask.distributed import Client
from docopt import docopt
from impresso_commons.path.path_fs import (KNOWN_JOURNALS,
                                           detect_canonical_issues)

from text_importer import __version__
from text_importer.importers.lux.core import import_issues as lux_import_issues
from text_importer.importers.lux.detect import \
    detect_issues as lux_detect_issues
from text_importer.importers.lux.detect import \
    select_issues as lux_select_issues

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"

logger = logging.getLogger()


def main():
    """Execute the main with CLI parameters."""

    # store CLI parameters
    args = docopt(__doc__)
    inp_dir = args["--input-dir"]
    outp_dir = args["--output-dir"]
    out_bucket = args["--s3-bucket"]
    log_file = args["--log-file"]
    scheduler = args["--scheduler"]
    clear_output = args["--clear"]
    incremental_output = args["--incremental"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO
    print_version = args["--version"]
    config_file = args["--config-file"]

    if print_version:
        print(f'impresso-txt-importer v{__version__}')
        return

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

    # start the dask local cluster
    if scheduler is None:
        client = Client(processes=False, n_workers=8, threads_per_worker=2)
    else:
        client = Client(scheduler)
    logger.info(f"Dask cluster: {client}")

    # clean output directory if existing
    if outp_dir is not None and os.path.exists(outp_dir):
        if clear_output is not None and clear_output:
            shutil.rmtree(outp_dir)

    # detect/select issues
    if config_file:
        logger.info(f"Found config file: {os.path.realpath(config_file)}")
        issues = lux_select_issues(config_file, inp_dir)

        logger.info(
            "{} newspaper remained after applying filter: {}".format(
                len(issues),
                issues
            )
        )
    else:
        logger.info("No config file found.")
        issues = lux_detect_issues(inp_dir)
        print(f'{len(issues)} newspaper issues detected')

    if os.path.exists(outp_dir) and incremental_output:
        issues_to_skip = [
            (issue.journal, issue.date, issue.edition)
            for issue in detect_canonical_issues(outp_dir, KNOWN_JOURNALS)
        ]
        logger.debug(f"Issues to skip: {issues_to_skip}")
        logger.info(f"{len(issues_to_skip)} issues to skip")
        issues = list(
            filter(
                lambda x: (x.journal, x.date, x.edition) not in issues_to_skip,
                issues
            )
        )
        logger.debug(f"Remaining issues: {issues}")
        logger.info(f"{len(issues)} remaining issues")

    logger.debug("Following issues will be imported:{}".format(issues))

    assert outp_dir is not None or out_bucket is not None

    result = lux_import_issues(issues, outp_dir, out_bucket)


if __name__ == '__main__':
    main()
