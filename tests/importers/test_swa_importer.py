import logging

from contextlib import ExitStack

from text_preparation.utils import get_pkg_resource
from text_preparation.importers.core import import_issues
from text_preparation.importers.swa.classes import SWANewspaperIssue
from text_preparation.importers.swa.detect import detect_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the SWA XML importer with sample data."""

    logger.info("Starting test_import_issues in test_swa_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/SWA/")
    ar_file = get_pkg_resource(f_mng, "data/sample_data/SWA/access_rights.json")
    out_dir = get_pkg_resource(f_mng, "data/out/")
    tmp_dir = get_pkg_resource(f_mng, "data/temp/")

    issues = detect_issues(base_dir=inp_dir, access_rights=ar_file)
    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=SWANewspaperIssue,
        image_dirs="",
        temp_dir=tmp_dir,
        chunk_size=None,
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()
