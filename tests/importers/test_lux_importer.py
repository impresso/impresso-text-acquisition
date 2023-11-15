import bz2
import json
import logging
import os
from glob import glob
import logging

from contextlib import ExitStack

from text_importer.utils import get_pkg_resource
from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers.core import import_issues
from text_importer.importers.lux.classes import LuxNewspaperIssue, IIIF_ENDPOINT_URL
from text_importer.importers.lux.detect import detect_issues as lux_detect_issues
from text_importer.importers.lux.detect import select_issues as lux_select_issues

logger = logging.getLogger(__name__)


# TODO: adapt code after refactoring
def test_import_issues():
    """Test the Luxembourg XML importer with sample data."""

    logger.info("Starting test_import_issues in test_lux_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/Luxembourg/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')
    
    output_bucket = None  # this disables the s3 upload

    issues = lux_detect_issues(inp_dir)
    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues, out_dir,
        s3_bucket=output_bucket,
        issue_class=LuxNewspaperIssue,
        image_dirs=None,
        temp_dir=None,
        chunk_size=None
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()

 
def test_selective_import():
    """Tests selective ingestion of BNL data.

    What to ingest is specified in a JSON configuration file.

    TODO: 
        - add support filtering/selection based on dates and date-ranges;
        - add support for exclusion of newspapers
    """
    logger.info("Starting test_selective_import in test_lux_importer.py.")

    f_mng = ExitStack()
    cfg_file = get_pkg_resource(f_mng, 'config/import_BNL.json')
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/Luxembourg/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')

    with open(cfg_file, 'r') as f:
        config = json.load(f)

    issues = lux_select_issues(
        base_dir=inp_dir, config=config, access_rights=""
    )

    assert issues is not None and len(issues) > 0
    assert all([i.journal in config['newspapers'] for i in issues])

    logger.info(f'There are {len(issues)} to ingest')
    import_issues(
        issues, out_dir, 
        s3_bucket=None, 
        issue_class=LuxNewspaperIssue,
        image_dirs=None, 
        temp_dir=None, 
        chunk_size=None
    )

    logger.info("Finished test_selective_import, closing file manager.")
    f_mng.close()

def check_link(link: str):
    return IIIF_ENDPOINT_URL in link and "ark:" in link and "ark:/" not in link

def check_iiif_links(issue_data):
    items = issue_data['i']
    imgs = [i for i in items if i['m']['tp'] == CONTENTITEM_TYPE_IMAGE]
    return (len(imgs) == 0 or 
            all(check_link(data['m']['iiif_link']) for data in imgs))


def test_image_iiif_links():

    logger.info("Starting test_image_iiif_links in test_lux_importer.py")
    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/Luxembourg/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')
    
    issues = lux_detect_issues(base_dir=inp_dir,)
    
    assert issues is not None
    assert len(issues) > 0
    
    journals = set([x.journal for x in issues])
    blobs = [f"{j}*.jsonl.bz2" for j in journals]
    issue_files = [f for b in blobs for f in glob(os.path.join(out_dir, b))]
    logger.info(issue_files)
    
    for filename in issue_files:
        with bz2.open(filename, "rt") as bzinput:
            for line in bzinput:
                issue = json.loads(line)
                assert check_iiif_links(issue), (
                    "Issue as wrong iiif_links."
                )

    logger.info("Finished test_image_iiif_links, closing file manager.")
    f_mng.close()

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
