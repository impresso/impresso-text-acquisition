import os
import json
import pkg_resources
from impresso_commons.path.path_fs import detect_issues
from text_importer.importer import import_issues
from text_importer.importers.olive import olive_import_issue
from impresso_commons.path.path_fs import (KNOWN_JOURNALS,
                                           detect_canonical_issues)


def test_olive_import_issues():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/'
    )
    """
    issues = [
        issue
        for issue in detect_issues(inp_dir)
        if issue.journal == 'GDL' and issue.date.year == 1807
    ]
    """
    issues = detect_issues(inp_dir)
    assert issues is not None
    assert len(issues) > 0

    result = import_issues(
        issues,
        "/mnt/project_impresso/images/",
        None,
        pkg_resources.resource_filename('text_importer', 'data/out/'),
        pkg_resources.resource_filename('text_importer', 'data/temp/'),
        "olive",
        True  # whether to parallelize or not
    )
    print(result)


def test_olive_import_images():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/'
    )
    issues = detect_issues(inp_dir)
    olive_import_issue(
        issues[1],
        image_dir='/mnt/project_impresso/images/',
        out_dir=pkg_resources.resource_filename('text_importer', 'data/out/')
    )


def test_imported_data():
    """Verify that canonical IDs stay the same."""
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/out/'
    )

    expected_data_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/expected/'
    )

    ingested_issues = detect_canonical_issues(inp_dir, KNOWN_JOURNALS)
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
