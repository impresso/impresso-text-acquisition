import logging

from contextlib import ExitStack

from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.utils import get_pkg_resource

from text_preparation.importers.bl.classes import BlNewspaperIssue
from text_preparation.importers.bl.detect import detect_issues
from text_preparation.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the BL XML importer with sample data."""

    logger.info("Starting test_import_issues in test_bl_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/BL/", "text_preparation")
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/", "text_preparation")
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
        notes="Manifest from BL test_import_issues().",
    )

    issues = detect_issues(base_dir=inp_dir, access_rights=None, tmp_dir=tmp_dir)
    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=BlNewspaperIssue,
        image_dirs=None,
        temp_dir=tmp_dir,
        chunk_size=None,
        manifest=test_manifest,
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()
