import logging
import json

from contextlib import ExitStack

from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.utils import get_pkg_resource

from text_preparation.importers.bcul.classes import BculNewspaperIssue
from text_preparation.importers.bcul.detect import detect_issues, select_issues
from text_preparation.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""

    logger.info("Starting test_import_issues in test_bcul_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/BCUL/", "text_preparation")
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
    tmp_dir = get_pkg_resource(f_mng, "data/temp/", "text_preparation")
    ar_file = get_pkg_resource(
        f_mng, "data/sample_data/BCUL/access_rights_and_aliases.json", "text_preparation"
    )

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
        notes="Manifest from BCUL test_import_issues().",
    )

    issues = detect_issues(base_dir=inp_dir, access_rights=ar_file)

    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=BculNewspaperIssue,
        image_dirs=None,
        temp_dir=tmp_dir,
        chunk_size=None,
        manifest=test_manifest,
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()


def test_selective_import():
    """Tests selective ingestion of BCUL data.

    What to ingest is specified in a JSON configuration file.

    TODO:
        - add support filtering/selection based on dates and date-ranges;
        - add support for exclusion of newspapers
    """
    logger.info("Starting test_selective_import in test_bcul_importer.py.")

    f_mng = ExitStack()
    cfg_file = get_pkg_resource(f_mng, "config/importer_config/import_BCUL.json", "text_preparation")
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/BCUL/", "text_preparation")
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
    ar_file = get_pkg_resource(
        f_mng, "data/sample_data/BCUL/access_rights_and_aliases.json", "text_preparation"
    )
    tmp_dir = get_pkg_resource(f_mng, "data/temp/", "text_preparation")

    with open(cfg_file, "r", encoding="utf-8") as f:
        config = json.load(f)

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
        notes="Manifest from BCUL test_selective_import().",
    )

    issues = select_issues(base_dir=inp_dir, config=config, access_rights=ar_file)

    assert issues is not None and len(issues) > 0
    assert all([i.journal in config["newspapers"] for i in issues])

    logger.info("There are %s to ingest", len(issues))
    import_issues(
        issues,
        out_dir,
        s3_bucket=None,
        issue_class=BculNewspaperIssue,
        image_dirs=None,
        temp_dir=None,
        chunk_size=None,
        manifest=test_manifest,
    )

    logger.info("Finished test_selective_import, closing file manager.")
    f_mng.close()
