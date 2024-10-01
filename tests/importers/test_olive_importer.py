import os
import json
import logging

from contextlib import ExitStack

from dask import bag as db

from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.utils import get_pkg_resource

from text_preparation.utils import verify_imported_issues
from text_preparation.importers.core import import_issues
from text_preparation.importers.olive.detect import olive_detect_issues
from text_preparation.importers.olive.classes import OliveNewspaperIssue

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""

    logger.info("Starting test_import_issues in test_olive_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/Olive/", "text_preparation")
    ar_file = get_pkg_resource(
        f_mng, "data/sample_data/Olive/access_rights.json", "text_preparation"
    )
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
    tmp_dir = get_pkg_resource(f_mng, "data/temp/", "text_preparation")

    test_manifest = DataManifest(
        data_stage="canonical",
        s3_output_bucket="10-canonical-sandbox",
        s3_input_bucket=None,
        git_repo="../../",
        temp_dir=tmp_dir,
        staging=True,
        is_patch=False,
        patched_fields=None,
        previous_mft_path=None,
        only_counting=False,
        push_to_git=False,
        notes="Manifest from Olive test_import_issues().",
    )

    issues = olive_detect_issues(base_dir=inp_dir, access_rights=ar_file)
    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=OliveNewspaperIssue,
        image_dirs="/mnt/project_impresso/images/",
        temp_dir=tmp_dir,
        chunk_size=None,
        manifest=test_manifest,
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()


def test_verify_imported_issues():
    """Verify that imported data do not change from run to run.

    We need to verify that:
    1. canonical IDs remain stable
    2. a given content item ID should correspond always to the same piece of
    data.
    """
    logger.info("Start test_verify_imported_issues in test_olive_importer.py")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
    expected_data_dir = get_pkg_resource(
        f_mng, "data/canonical_out/expected/Olive", "text_preparation"
    )

    # consider only newspapers in Olive format
    newspapers = ["GDL", "JDG", "IMP"]

    # look for bz2 archives in the output directory
    issue_archive_files = [
        os.path.join(inp_dir, file)
        for file in os.listdir(inp_dir)
        if any([np in file for np in newspapers])
        and os.path.isfile(os.path.join(inp_dir, file))
    ]
    logger.info("Found canonical files: %s", issue_archive_files)

    # read issue JSON data from bz2 archives
    ingested_issues = db.read_text(issue_archive_files).map(json.loads).compute()
    logger.info("Issues to verify: %s", [i["id"] for i in ingested_issues])

    for actual_issue_json in ingested_issues:

        expected_output_path = os.path.join(
            expected_data_dir, f"{actual_issue_json['id']}-issue.json"
        )

        if not os.path.exists(expected_output_path):
            print(expected_output_path)
            continue

        with open(expected_output_path, "r", encoding="utf-8") as infile:
            expected_issue_json = json.load(infile)

        verify_imported_issues(actual_issue_json, expected_issue_json)

        logger.info("test_verify_imported_issues done, closing file manager.")
        f_mng.close()
