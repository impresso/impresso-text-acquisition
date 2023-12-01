"""
Functions and CLI script to convert any OCR data into Impresso's format.

Usage:
    <importer-name>importer.py --input-dir=<id> (--clear | --incremental) [--output-dir=<od> --image-dirs=<imd> --temp-dir=<td> --chunk-size=<cs> --s3-bucket=<b> --config-file=<cf> --log-file=<f> --verbose --scheduler=<sch> --access-rights=<ar>]
    <importer-name>importer.py --version

Options:
    --input-dir=<id>    Base directory containing one sub-directory for each journal
    --image-dirs=<imd>  Directory containing (canonical) images and their metadata (use `,` to separate multiple dirs)
    --output-dir=<od>   Base directory where to write the output files
    --temp-dir=<td>     Temporary directory to extract .zip archives
    --config-file=<cf>  Configuration file for selective import
    --s3-bucket=<b>     If provided, writes output to an S3 drive, in the specified bucket
    --scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
    --log-file=<f>      Log file; when missing print log to stdout
    --access-rights=<ar>  Access right file if relevant (only for `olive` and `rero` importers)
    --chunk-size=<cs>   Chunk size in years used to group issues when importing
    --verbose   Verbose log messages (good for debugging)
    --clear    Removes the output folder (if already existing)
    --incremental   Skips issues already present in output directory 
    --version    Prints version and exits.

"""  # noqa: E501

import json
import logging
import os
import shutil
import time
from typing import Any, Type, Callable

from dask.distributed import Client, LocalCluster
from docopt import docopt
from impresso_commons.path.path_fs import (KNOWN_JOURNALS,
                                           detect_canonical_issues,
                                           IssueDir)

from text_importer import __version__
from text_importer.importers.classes import NewspaperIssue
from text_importer.importers.bl.classes import BlNewspaperIssue
from text_importer.importers.core import import_issues
from text_importer.utils import init_logger

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"

logger = logging.getLogger()


def clear_output_dir(out_dir: str | None, clear_output: bool | None) -> None:
    """Ensure `out_dir` exists, create it if not and empty it if necessary.

    Nothing is done if `out_dir` is `None`.

    Args:
        out_dir (str | None): Output directory to create if it doesn't exist.
        clear_output (bool | None)): Whether to empty the contents of `out_dir`
            if it exists. Values `False` and `None` have the same effect.
    """
    if out_dir is not None:
        if os.path.exists(out_dir):
            if clear_output is not None and clear_output:
                shutil.rmtree(out_dir)  # Deletes the whole dir
                time.sleep(3)
                os.makedirs(out_dir)
        else:
            os.makedirs(out_dir)


def get_dask_client(
    scheduler: str | None, log_file: str | None, log_level: int
) -> Client:
    """Instantiate Dask client with given scheduler address or default values.

    If a scheduler is given, a Dask scheduler and workers should have been 
    previously launched in terminals separate to the one used for the importer.
    If no scheduler is given, the created Dask distributed cluster will have 8
    workers, each with 2 threads.

    Args:
        scheduler (str | None): TCP address of the Dask scheduler to use.
        log_file (str | None): File to use for logging.
        log_level (int): Log level used, either INFO or DEBUG in verbose mode.

    Returns:
        Client: A client connected to and allowing to manage the Dask cluster.
    """
    if scheduler is None:
        cluster = LocalCluster()
        client = Client(cluster)
        cluster.adapt(minimum=2, maximum=32)
        #client = Client(processes=False, n_workers=8, threads_per_worker=2)
    else:
        client = Client(scheduler)
        client.run(
            init_logger, _logger=logger, log_level=log_level, log_file=log_file
        )
    return client


def apply_detect_func(
    issue_class: Type[NewspaperIssue], 
    input_dir: str, 
    access_rights: str, 
    detect_func: Callable[[str, str], list[IssueDir]], 
    tmp_dir: str
) -> list[IssueDir]:
    """Apply the given `detect_func` function of the importer in use.

    Args:
        issue_class (Type[NewspaperIssue]): Type of issue importer in use.
        input_dir (str): Directory containing this importer's newspaper data.
        access_rights (str): Path to the access rights file for this importer.
        detect_func (Callable[[str, str], list[IssueDir]]): Function detecting
            the issues present in `input_dir` to import.
        tmp_dir (str): Temporary directory used to unpack zip archives.

    Returns:
        list[IssueDir]: List of detected issues for this importer.
    """
    if issue_class is BlNewspaperIssue:
        return detect_func(
            input_dir, access_rights=access_rights, tmp_dir=tmp_dir
        )
    else:
        return detect_func(input_dir, access_rights=access_rights)


def apply_select_func(
    issue_class: Type[NewspaperIssue], 
    config: dict[str, Any], 
    input_dir: str, 
    access_rights: str, 
    select_func: Callable[[str, str, str], list[IssueDir]],
    tmp_dir: str
) -> list[IssueDir]:
    """Apply the given `select_func` function of the importer in use.

    Args:
        issue_class (Type[NewspaperIssue]): Type of issue importer in use.
        config (dict[str, Any]): Configuration to filter issues to import.
        input_dir (str): Directory containing this importer's newspaper data.
        access_rights (str): Path to the access rights file for this importer.
        select_func (Callable[[str, str, str], list[IssueDir]]): Function 
            detecting and selecting the issues to import using `config`.
        tmp_dir (str): Temporary directory used to unpack zip archives.

    Returns:
        list[IssueDir]: List of selected issues for this importer and config.
    """
    if issue_class is BlNewspaperIssue:
        return select_func(
            input_dir, config=config, access_rights=access_rights, tmp_dir=tmp_dir
        )
    else:
        return select_func(input_dir, config=config, access_rights=access_rights)


def main(
    issue_class: NewspaperIssue,
    detect_func: Callable[[str, str], list[IssueDir]], 
    select_func: Callable[[str, str, str], list[IssueDir]]
) -> None:
    """Import and convert newspapers to Impresso's format using CLI parameters.

    All imported newspapers have the same OCR format. For each newspaper issue
    imported, corresponding Issue and Page objects will be created before being
    serialized to json and uploaded to an S3 bucket, grouped by title and year.

    Args:
        issue_class (NewspaperIssue): Importer to use for the conversion
        detect_func (Callable[[str, str], list[IssueDir]]): `detect` function
            of the used importer.
        select_func (Callable[[str, str, str], list[IssueDir]]): `select` 
            function of the used importer.
    """
    
    # store CLI parameters
    args = docopt(__doc__)
    inp_dir = args["--input-dir"]
    outp_dir = args["--output-dir"]
    temp_dir = args['--temp-dir']
    image_dirs = args["--image-dirs"]
    out_bucket = args["--s3-bucket"]
    log_file = args["--log-file"]
    access_rights_file = args['--access-rights']
    chunk_size = args['--chunk-size']
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
    logger.info("CLI arguments received: {}".format(args))
    
    # start the dask local cluster
    client = get_dask_client(scheduler, log_file, log_level)
    
    logger.info(f"Dask cluster: {client}")
    
    # Checks if out dir exists (Creates it if not) and if should empty it
    clear_output_dir(outp_dir, clear_output)  
    
    # detect/select issues
    if config_file and os.path.isfile(config_file):
        logger.info(f"Found config file: {os.path.realpath(config_file)}")
        with open(config_file, 'r') as f:
            config = json.load(f)
        issues = select_func(inp_dir, config, access_rights=access_rights_file)
        logger.info(
            f"{len(issues)} newspaper remained after applying filter: {issues}"
        )
    else:
        logger.info("No config file found.")
        issues = apply_detect_func(issue_class, inp_dir, access_rights_file, 
                                   detect_func=detect_func, tmp_dir=temp_dir)
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
    
    import_issues(issues, outp_dir, out_bucket, issue_class, 
                  image_dirs, temp_dir, chunk_size, client)
