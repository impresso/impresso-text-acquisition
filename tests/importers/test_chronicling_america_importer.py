import os
import logging
import pytest
from contextlib import ExitStack
from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.utils import get_pkg_resource

from text_preparation.importers.core import import_issues
from text_preparation.importers.chronicling_america.classes import ChroniclingAmericaNewspaperIssue
from text_preparation.importers.chronicling_america.detect import detect_issues as ca_detect_issues

logger = logging.getLogger(__name__)

def test_import_chronicling_america_issues():
    """Test the Chronicling America XML importer with the downloaded Evening Star sample issue."""
    logger.info("Starting test_import_chronicling_america_issues.")

    f_mng = ExitStack()
    # Path to sample data downloaded locally
    inp_dir = get_pkg_resource(
        f_mng, "data/sample_data/", "text_preparation"
    )
    # Target output folder for test canonical JSON files
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
    tmp_dir = get_pkg_resource(f_mng, "data/temp/", "text_preparation")

    # Clear output dir if it exists
    if os.path.exists(out_dir):
        import shutil
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)

    test_manifest = DataManifest(
        data_stage="canonical",
        s3_output_bucket="10-canonical-sandbox",
        s3_input_bucket=None,
        git_repo=".",
        temp_dir=tmp_dir,
        is_patch=False,
        patched_fields=None,
        previous_mft_path=None,
        only_counting=False,
        push_to_git=False,
        notes="Manifest from Chronicling America test.",
    )

    # Detect issues
    issues = ca_detect_issues(inp_dir)
    assert issues is not None, "Issues list should not be None"
    
    # Filter to only the eveningstar issues
    es_issues = [i for i in issues if i.alias == "eveningstar"]
    assert len(es_issues) > 0, "Should detect at least one eveningstar sample issue"

    # Run the import loop
    from unittest.mock import patch
    with patch("impresso_essentials.versioning.data_manifest.read_manifest_from_s3", return_value=(None, None)):
        import_issues(
            es_issues,
            out_dir,
            s3_bucket=None,
            issue_class=ChroniclingAmericaNewspaperIssue,
            image_dirs=None,
            temp_dir=None,
            chunk_size=None,
            manifest=test_manifest,
            provider="LOC",
        )

    # Verify that the output files exist
    # Output structure should follow: out_dir / [provider] / [alias] / [year] / [month] / [day] / [edition] /
    expected_out_dir = os.path.join(out_dir, "LOC", "eveningstar", "1932", "06", "20", "a")
    assert os.path.exists(expected_out_dir), f"Output directory {expected_out_dir} should exist"

    # We expect the issue JSON and the page JSONs to be created and validate
    files = os.listdir(expected_out_dir)
    assert any("p0001.json" in f for f in files), "Should have created page JSON files"
    
    logger.info("Finished test_import_chronicling_america_issues successfully.")
    f_mng.close()
