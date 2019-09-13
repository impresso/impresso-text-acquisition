import os
import json
import pkg_resources

from dask import bag as db

from text_importer.utils import verify_imported_issues
from text_importer.importers.core import import_issues
from text_importer.importers.olive.detect import olive_detect_issues
from text_importer.importers.olive.classes import OliveNewspaperIssue

import logging

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""

    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Olive/'
    )

    access_rights_file = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Olive/access_rights.json'
    )

    issues = olive_detect_issues(
        base_dir=inp_dir,
        access_rights=access_rights_file
    )
    assert issues is not None
    assert len(issues) > 0

    result = import_issues(
        issues,
        out_dir=pkg_resources.resource_filename('text_importer', 'data/out/'),
        s3_bucket=None,
        issue_class=OliveNewspaperIssue,
        image_dirs="/mnt/project_impresso/images/",
        temp_dir=pkg_resources.resource_filename('text_importer', 'data/temp/'),
        chunk_size=None
    )
    print(result)


def test_verify_imported_issues():
    """Verify that imported data do not change from run to run.

    We need to verify that:
    1. canonical IDs remain stable
    2. a given content item ID should correspond always to the same piece of
    data.
    """

    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/out/'
    )

    expected_data_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/expected/Olive'
    )

    # consider only newspapers in Olive format
    newspapers = ["GDL", "JDG", "IMP"]

    # look for bz2 archives in the output directory
    issue_archive_files = [
        os.path.join(inp_dir, file)
        for file in os.listdir(inp_dir)
        if any([np in file for np in newspapers]) and
        os.path.isfile(os.path.join(inp_dir, file))
    ]
    logger.info(f'Found canonical files: {issue_archive_files}')

    # read issue JSON data from bz2 archives
    ingested_issues = db.read_text(issue_archive_files)\
        .map(json.loads)\
        .compute()
    logger.info(f"Issues to verify: {[i['id'] for i in ingested_issues]}")

    for actual_issue_json in ingested_issues:

        expected_output_path = os.path.join(
            expected_data_dir,
            f"{actual_issue_json['id']}-issue.json"
        )

        if not os.path.exists(expected_output_path):
            print(expected_output_path)
            continue

        with open(expected_output_path, 'r') as infile:
            expected_issue_json = json.load(infile)

        verify_imported_issues(actual_issue_json, expected_issue_json)
