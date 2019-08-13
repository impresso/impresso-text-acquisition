import os
import json
import pkg_resources

from text_importer.importers.core import import_issues
from impresso_commons.path.path_fs import detect_canonical_issues
from text_importer.importers.olive.detect import olive_detect_issues
from text_importer.importers.olive.classes import OliveNewspaperIssue

import logging

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer."""

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
        temp_dir=pkg_resources.resource_filename('text_importer', 'data/temp/')
    )
    print(result)


def test_imported_data():
    """Verify that canonical IDs stay the same."""

    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/out/'
    )

    expected_data_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/expected/Olive'
    )

    ingested_issues = detect_canonical_issues(inp_dir, ["GDL", "JDG", "IMP"])

    for issue in ingested_issues:
        issue_json_file = [
            file
            for file in os.listdir(issue.path)
            if "issue" in file and "json" in file
        ]

        if len(issue_json_file) == 0:
            continue

        issue_json_file = issue_json_file[0]
        expected_output_path = os.path.join(expected_data_dir, issue_json_file)
        actual_output_path = os.path.join(issue.path, issue_json_file)

        if not os.path.exists(expected_output_path):
            continue

        with open(expected_output_path, 'r') as infile:
            expected_json = json.load(infile)

        with open(actual_output_path, 'r') as infile:
            actual_json = json.load(infile)

        actual_ids = set([i['m']['id'] for i in actual_json['i']])
        expected_ids = set([i['m']['id'] for i in expected_json['i']])
        assert expected_ids.difference(actual_ids) == set()
