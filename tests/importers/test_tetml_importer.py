import os
import json
import logging
import shutil
from time import strftime
import pkg_resources

from pathlib import Path

from dask import bag as db

from text_importer.utils import verify_imported_issues
from text_importer.importers.core import import_issues, compress_issues, dirs2issues, issue2pages, process_pages, serialize_pages, compress_pages
from text_importer.importers.tetml.detect import tetml_detect_issues
from text_importer.importers.tetml.classes import TetmlNewspaperIssue
from impresso_commons.path.path_fs import canonical_path



logger = logging.getLogger(__name__)


def test_import_issues_no_dask():
    inp_dir = pkg_resources.resource_filename(
        "text_importer", "data/sample_data/Tetml/"
    )

    access_rights_file = pkg_resources.resource_filename(
        "text_importer", "data/sample_data/Tetml/access_rights.json"
    )

    out_dir=pkg_resources.resource_filename("text_importer", "data/out/")
    image_dirs="/mnt/project_impresso/images/"
    temp_dir=pkg_resources.resource_filename("text_importer", "data/temp/")

    failed_log_path = os.path.join(
            out_dir,
            f'failed-{strftime("%Y-%m-%d-%H-%M-%S")}.log'
            )


    dir_issues = tetml_detect_issues(base_dir=inp_dir, access_rights=access_rights_file)

    assert dir_issues is not None
    assert len(dir_issues) > 0

    issues = dirs2issues(dir_issues, issue_class=TetmlNewspaperIssue, failed_log=failed_log_path, temp_dir=temp_dir)

    for issue in issues:
        compress_issues((issue.journal, issue.date.year), [issue], output_dir=out_dir)

        pages = issue2pages(issue)
        parsed_pages = process_pages(pages, failed_log=failed_log_path)
        serialized_pages = serialize_pages(parsed_pages, output_dir=out_dir)

        pages_out_dir = os.path.join(out_dir, 'pages')
        Path(pages_out_dir).mkdir(exist_ok=True)




        key = canonical_path(issue, path_type='dir').replace('/', '-')
        compress_pages(key, serialized_pages, prefix='pages', output_dir=pages_out_dir)



    if temp_dir is not None and os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    logger.info("---------- Done ----------")



def test_import_issues():
    """Test the Tetml importer with sample data."""

    inp_dir = pkg_resources.resource_filename(
        "text_importer", "data/sample_data/Tetml/"
    )

    access_rights_file = pkg_resources.resource_filename(
        "text_importer", "data/sample_data/Tetml/access_rights.json"
    )

    issues = tetml_detect_issues(base_dir=inp_dir, access_rights=access_rights_file)
    assert issues is not None
    assert len(issues) > 0

    result = import_issues(
        issues,
        out_dir=pkg_resources.resource_filename("text_importer", "data/out/"),
        s3_bucket=None,
        issue_class=TetmlNewspaperIssue,
        image_dirs="/mnt/project_impresso/images/",
        temp_dir=pkg_resources.resource_filename("text_importer", "data/temp/"),
        chunk_size=None,
    )
    print(result)


def test_verify_imported_issues():
    """Verify that imported data do not change from run to run.

    We need to verify that:
    1. canonical IDs remain stable
    2. a given content item ID should correspond always to the same piece of
    data.
    """

    inp_dir = pkg_resources.resource_filename("text_importer", "data/out/")

    expected_data_dir = pkg_resources.resource_filename(
        "text_importer", "data/expected/Tetml"
    )

    # consider only newspapers in Tetml format
    newspapers = ["FedGazDe", "FedGazFr", "FedGazIt"]

    # look for bz2 archives in the output directory
    issue_archive_files = [
        os.path.join(inp_dir, file)
        for file in os.listdir(inp_dir)
        if any([np in file for np in newspapers])
        and os.path.isfile(os.path.join(inp_dir, file))
    ]
    logger.info(f"Found canonical files: {issue_archive_files}")

    # read issue JSON data from bz2 archives
    ingested_issues = db.read_text(issue_archive_files).map(json.loads).compute()
    logger.info(f"Issues to verify: {[i['id'] for i in ingested_issues]}")

    for actual_issue_json in ingested_issues:

        expected_output_path = os.path.join(
            expected_data_dir, f"{actual_issue_json['id']}-issue.json"
        )

        if not os.path.exists(expected_output_path):
            print(expected_output_path)
            continue

        with open(expected_output_path, "r") as infile:
            expected_issue_json = json.load(infile)

        verify_imported_issues(actual_issue_json, expected_issue_json)
