import logging

from contextlib import ExitStack

from text_importer.utils import get_pkg_resource
from text_importer.importers.bl.classes import BlNewspaperIssue
from text_importer.importers.bl.detect import detect_issues
from text_importer.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the BL XML importer with sample data."""
    
    logger.info("Starting test_import_issues in test_bl_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/BL/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')
    tmp_dir = get_pkg_resource(f_mng, 'data/temp/')

    issues = detect_issues(
            base_dir=inp_dir,
            access_rights=None,
            tmp_dir=tmp_dir
            )
    assert issues is not None
    assert len(issues) > 0
    
    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=BlNewspaperIssue,
        image_dirs=None,
        temp_dir=tmp_dir,
        chunk_size=None
    )
    
    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()

