import logging
import json

from contextlib import ExitStack

from text_importer.utils import get_pkg_resource
from text_importer.importers.bcul.classes import BCULNewspaperIssue
from text_importer.importers.bcul.detect import detect_issues, select_issues
from text_importer.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""
    
    logger.info("Starting test_import_issues in test_bcul_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/BCUL/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')
    tmp_dir = get_pkg_resource(f_mng, 'data/temp/')
    
    issues = detect_issues(
            base_dir=inp_dir,
            access_rights=None
            )
    assert issues is not None
    assert len(issues) > 0
    
    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=BCULNewspaperIssue,
        image_dirs=None,
        temp_dir=tmp_dir,
        chunk_size=None
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
    cfg_file = get_pkg_resource(f_mng, 'config/import_BCUL.json')
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/BCUL/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')

    with open(cfg_file, 'r') as f:
        config = json.load(f)

    issues = select_issues(
        base_dir=inp_dir, config=config, access_rights=""
    )

    assert issues is not None and len(issues) > 0
    assert all([i.journal in config['newspapers'] for i in issues])

    logger.info(f'There are {len(issues)} to ingest')
    import_issues(
        issues, out_dir, 
        s3_bucket=None, 
        issue_class=BCULNewspaperIssue,
        image_dirs=None, 
        temp_dir=None, 
        chunk_size=None
    )

    logger.info("Finished test_selective_import, closing file manager.")
    f_mng.close()