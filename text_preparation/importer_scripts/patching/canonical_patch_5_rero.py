"""Command-line script to perform the patch #5 (adding the reading order property) on the RERO 2 & 3 canonical data.

Usage:
    canonical_patch_5_rero.py --input-bucket=<ib> --output-bucket=<ob> --canonical-repo-path=<crp> --temp-dir=<td> --log-file=<lf> --error-log=<el> --patch-outputs-filename=<pof>
    
Options:

--input-bucket=<ib>  S3 input bucket.
--output-bucket=<ob>  S3 output bucket.
--canonical-repo-path=<crp>  Path to the local impresso-text-acquisition git repository.
--temp-dir=<td>  Temporary directory to write files in.
--log-file=<lf>  Path to log file.
--error-log=<el>  Path to error log file.
--patch-outputs-filename=<pof>  Filename of the .txt file containing the output of the patches.
"""

import os
import logging
from typing import Any
from docopt import docopt

from impresso_essentials.io.s3 import get_storage_options, fetch_files
from impresso_essentials.versioning.compute_manifest import create_manifest
from impresso_essentials.utils import init_logger
from text_preparation.utils import get_reading_order
from text_preparation.importer_scripts.patching.canonical_patch_1_uzh import (
    title_year_pair_to_issues,
    write_upload_issues,
    to_issue_id_pages_dict,
    nzz_write_upload_pages,
)

IMPRESSO_STORAGEOPT = get_storage_options()
logger = logging.getLogger()


def add_ro_to_items(issue: dict[str, Any]) -> list[dict[str, Any]]:
    reading_order_dict = get_reading_order(issue["i"])
    for ci in issue["i"]:
        ci["m"]["ro"] = reading_order_dict[ci["m"]["id"]]
    return issue


def main():
    arguments = docopt(__doc__)
    s3_input_bucket = arguments["--input-bucket"]
    s3_output_bucket = arguments["--output-bucket"]
    canonical_repo_path = arguments["--canonical-repo-path"]
    temp_dir = arguments["--temp-dir"]
    log_file = arguments["--log-file"]
    error_log = arguments["--error-log"]
    patch_outputs = arguments["--patch-outputs-filename"]

    init_logger(logger, logging.INFO, log_file)

    RERO_2_3_TITLES = [
        "BLB",
        "BNN",
        "DFS",
        "DVF",
        "EZR",
        "FZG",
        "HRV",
        "LAB",
        "LLE",
        "MGS",
        "NTS",
        "NZG",
        "SGZ",
        "SRT",
        "WHD",
        "ZBT",
        "CON",
        "DTT",
        "FCT",
        "GAV",
        "GAZ",
        "LLS",
        "OIZ",
        "SAX",
        "SDT",
        "SMZ",
        "VDR",
        "VHT",
    ]
    PROP_NAME = "ro"
    final_patches_output_path = os.path.join(os.path.dirname(log_file), patch_outputs)

    logger.info(
        "Patching titles %s: adding %s property at page level",
        RERO_2_3_TITLES,
        PROP_NAME,
    )
    logger.info("Input arguments: %s", arguments)

    # empty_folder(temp_dir)

    logger.info("Fetching the page and issues files from S3...")
    # download the issues of interest for this patch
    rero_issues, rero_pages = fetch_files(
        "canonical-data", False, "both", RERO_2_3_TITLES
    )

    logger.info("Updating the issues files and uploading them to s3...")
    rero_patched_issues = (
        rero_issues.map_partitions(
            lambda yearly_issue: [add_ro_to_items(issue) for issue in yearly_issue]
        )
        .map_partitions(title_year_pair_to_issues)
        .map_partitions(
            lambda issues: write_upload_issues(
                issues[0],
                issues[1],
                output_dir=temp_dir,
                bucket_name=s3_output_bucket,
                failed_log=error_log,
            )
        )
    ).compute()

    # free the memory allocated
    del rero_issues

    logger.info("Uploading the page files to the new bucket")
    rero_page_files = (
        rero_pages.map_partitions(lambda pages: [p for p in pages])
        .map_partitions(to_issue_id_pages_dict)
        .map_partitions(
            lambda issue_to_pages: nzz_write_upload_pages(
                issue_to_pages,
                output_dir=temp_dir,
                bucket_name=s3_output_bucket,
                failed_log=error_log,
            )
        )
        .flatten()
    ).compute()

    # create the config for the manifest computation
    manifest_config = {
        "data_stage": "canonical",
        "output_bucket": s3_output_bucket,
        "input_bucket": s3_input_bucket,
        "git_repository": canonical_repo_path,
        "newspapers": RERO_2_3_TITLES,
        "temp_directory": temp_dir,
        "previous_mft_s3_path": None,
        "is_staging": True,
        "is_patch": True,
        "patched_fields": [PROP_NAME],
        "push_to_git": True,
        "file_extensions": "issues.jsonl.bz2",
        "log_file": log_file,
        "notes": "Patching RERO 2 & 3 data to add the reading order.",
    }
    # create and upload the manifest
    create_manifest(manifest_config)


if __name__ == "__main__":
    main()
