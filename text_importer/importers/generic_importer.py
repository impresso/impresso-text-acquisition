"""
Functions and CLI script to convert any OCR data into Impresso's format.

Usage:
    <importer-name>importer.py --input-dir=<id> (--clear | --incremental) [--output-dir=<od> --image-dirs=<imd> --temp-dir=<td> --s3-bucket=<b> --config-file=<cf> --log-file=<f> --verbose --scheduler=<sch> --access-rights=<ar>]
    <importer-name>importer.py --version

Options:
    --input-dir=<id>    Base directory containing one sub-directory for each journal
    --image-dirs=<imd>  Directory containing (canonical) images and their metadata (use `,` to separate multiple dirs)
    --output-dir=<od>   Base directory where to write the output files
    --temp-dir=<td>     Temporary directory to extract .zip archives
    --config-file=<cf>  configuration file for selective import
    --s3-bucket=<b>     If provided, writes output to an S3 drive, in the specified bucket
    --scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
    --log-file=<f>      Log file; when missing print log to stdout
    --access-rights=<ar>  Access right file if relevant (only for ``olive`` and ``rero`` importers)
    --verbose   Verbose log messages (good for debugging)
    --clear    Removes the output folder (if already existing)
    --version    Prints version and exits.

"""  # noqa: E501

import json
import logging
import os
import shutil
import time
from typing import Type

from dask.distributed import Client
from docopt import docopt
from impresso_commons.path.path_fs import (KNOWN_JOURNALS,
                                           detect_canonical_issues)

from text_importer import __version__
from text_importer.importers.classes import NewspaperIssue
from text_importer.importers.core import import_issues
from text_importer.utils import init_logger

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"

logger = logging.getLogger()


def clear_output_dir(out_dir, clear_output):
    if out_dir is not None:
        if os.path.exists(out_dir):
            if clear_output is not None and clear_output:
                shutil.rmtree(out_dir)  # Deletes the whole dir
                time.sleep(3)
                os.makedirs(out_dir)
        else:
            os.makedirs(out_dir)


def get_dask_client(scheduler, log_file, log_level):
    if scheduler is None:
        client = Client(processes=False, n_workers=8, threads_per_worker=2)
    else:
        client = Client(scheduler)
        client.run(init_logger, _logger=logger, log_level=log_level, log_file=log_file)
    return client


def main(issue_class: Type[NewspaperIssue], detect_func, select_func):
    """Execute the main with CLI parameters."""
    
    # store CLI parameters
    args = docopt(__doc__)
    inp_dir = args["--input-dir"]
    outp_dir = args["--output-dir"]
    temp_dir = args['--temp-dir']
    image_dirs = args["--image-dirs"]
    out_bucket = args["--s3-bucket"]
    log_file = args["--log-file"]
    access_rights_file = args['--access-rights']
    scheduler = args["--scheduler"]
    clear_output = args["--clear"]
    incremental_output = args["--incremental"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO
    print_version = args["--version"]
    config_file = args["--config-file"]
    
    if print_version:
        print(f'impresso-txt-importer v{__version__}')
        return
    
    init_logger(logger, log_level, log_file)
    logger.debug("CLI arguments received: {}".format(args))
    
    # start the dask local cluster
    client = get_dask_client(scheduler, log_file, log_level)
    
    logger.info(f"Dask cluster: {client}")
    
    # clean output directory if existing
    clear_output_dir(outp_dir, clear_output)  # Checks if out dir exists (Creates it if not) and if should empty it
    
    # detect/select issues
    if config_file and os.path.isfile(config_file):
        logger.info(f"Found config file: {os.path.realpath(config_file)}")
        with open(config_file, 'r') as f:
            config = json.load(f)
        issues = select_func(inp_dir, config, access_rights=access_rights_file)
        logger.info(
                "{} newspaper remained after applying filter: {}".format(
                        len(issues),
                        issues
                        )
                )
    else:
        logger.info("No config file found.")
        issues = detect_func(inp_dir, access_rights=access_rights_file)
        logger.info(f'{len(issues)} newspaper issues detected')
    
    if outp_dir is not None and os.path.exists(outp_dir) and incremental_output:
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
    
    result = import_issues(issues, outp_dir, out_bucket, issue_class=issue_class, image_dirs=image_dirs, temp_dir=temp_dir)
