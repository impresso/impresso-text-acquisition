import json
import logging
import os

import pkg_resources
from dask import bag as db

from text_importer.importers.core import import_issues
from text_importer.importers.lux.classes import LuxNewspaperIssue
from text_importer.importers.lux.detect import detect_issues as lux_detect_issues
from text_importer.importers.lux.detect import select_issues as lux_select_issues

logger = logging.getLogger(__name__)


# TODO: adapt code after refactoring
def test_import_issues():
    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/Luxembourg/'
            )
    out_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
    output_bucket = None  # this disables the s3 upload
    
    issues = lux_detect_issues(inp_dir)
    assert issues is not None
    import_issues(issues, out_dir, s3_bucket=output_bucket, issue_class=LuxNewspaperIssue, image_dirs=None, temp_dir=None)


def test_selective_import():
    """Tests selective ingestion of BNL data.

    What to ingest is specified in a JSON configuration file.

    ..todo::

        - add support filtering/selection based on dates and date-ranges;
        - add support for exclusion of newspapers
    """
    cfg_file = pkg_resources.resource_filename(
            'text_importer',
            'config/import_BNL.json'
            )
    with open(cfg_file, 'r') as f:
        config = json.load(f)
    
    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/Luxembourg/'
            )
    out_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
    """
    inp_dir = "/mnt/project_impresso/original/BNL/"
    out_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/out/debug/'
    )
    """
    issues = lux_select_issues(base_dir=inp_dir, config=config, access_rights="")
    assert issues is not None and len(issues) > 0
    
    logger.info(f'There are {len(issues)} to ingest')
    import_issues(issues, out_dir, s3_bucket=None, issue_class=LuxNewspaperIssue, image_dirs=None, temp_dir=None)

# # TODO: adapt it to Lux data
# def test_verify_imported_issues():
#     """Verify that imported data do not change from run to run.
#
#     We need to verify that:
#     1. canonical IDs remain stable
#     2. a given content item ID should correspond always to the same piece of
#     data.
#     """
#
#     inp_dir = pkg_resources.resource_filename(
#         'text_importer',
#         'data/out/'
#     )
#
#     expected_data_dir = pkg_resources.resource_filename(
#         'text_importer',
#         'data/expected/Olive'
#     )
#
#     # consider only newspapers in Olive format
#     newspapers = ["GDL", "JDG", "IMP"]
#
#     # look for bz2 archives in the output directory
#     issue_archive_files = [
#         os.path.join(inp_dir, file)
#         for file in os.listdir(inp_dir)
#         if any([np in file for np in newspapers]) and
#         os.path.isfile(os.path.join(inp_dir, file))
#     ]
#     logger.info(f'Found canonical files: {issue_archive_files}')
#
#     # read issue JSON data from bz2 archives
#     ingested_issues = db.read_text(issue_archive_files)\
#         .map(json.loads)\
#         .compute()
#     logger.info(f"Issues to verify: {[i['id'] for i in ingested_issues]}")
#
#     for actual_issue_json in ingested_issues:
#
#         expected_output_path = os.path.join(
#             expected_data_dir,
#             f"{actual_issue_json['id']}-issue.json"
#         )
#
#         if not os.path.exists(expected_output_path):
#             print(expected_output_path)
#             continue
#
#         with open(expected_output_path, 'r') as infile:
#             expected_issue_json = json.load(infile)
#
#         verify_imported_issues(actual_issue_json, expected_issue_json)
