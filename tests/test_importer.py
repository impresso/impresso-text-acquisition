import os
import json
import pkg_resources
from impresso_commons.path.path_fs import detect_issues

from text_importer.importer import import_issues
from text_importer.importers.olive_old import olive_import_issue
from impresso_commons.path.path_fs import (KNOWN_JOURNALS,
                                           detect_canonical_issues)
from text_importer.importers.lux.core import import_issues as lux_import_issues
from text_importer.importers.lux.detect import \
    detect_issues as lux_detect_issues, select_issues as lux_select_issues

import logging

logger = logging.getLogger(__name__)


def test_olive_import_issues():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Olive/'
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


def test_lux_importer():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Luxembourg/'
    )
    out_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
    output_bucket = None  # this disables the s3 upload

    issues = lux_detect_issues(inp_dir)
    assert issues is not None
    lux_import_issues(issues, out_dir, s3_bucket=output_bucket)

    # TODO verify that issues processed are actually in the output folder
    # try to validate the JSON documents (pages and issues)


def test_lux_select():
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
    issues = lux_select_issues(cfg_file, inp_dir)
    assert issues
    logger.info(f'There are {len(issues)} to ingest')
    lux_import_issues(issues, out_dir, s3_bucket=None)
