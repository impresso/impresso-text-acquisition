import bz2
import json
import logging
import os
from glob import glob
import logging

from contextlib import ExitStack

from text_importer.utils import get_pkg_resource
from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers.core import import_issues
from text_importer.importers.kb.classes import KbNewspaperIssue
from text_importer.importers.kb.detect import detect_issues as kb_detect_issues
from text_importer.importers.kb.detect import select_issues as kb_select_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the KB XML importer with sample data."""

    logger.info("Starting test_import_issues in test_kb_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/KB/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')
    
    output_bucket = None  # this disables the s3 upload

    issues = kb_detect_issues(inp_dir)
    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues, out_dir,
        s3_bucket=output_bucket,
        issue_class=KbNewspaperIssue,
        image_dirs=None,
        temp_dir=None,
        chunk_size=None
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()

 
def test_selective_import():
    """Tests selective ingestion of KB data.

    What to ingest is specified in a JSON configuration file.

    TODO: 
        - add support filtering/selection based on dates and date-ranges;
        - add support for exclusion of newspapers
    """
    logger.info("Starting test_selective_import in test_kb_importer.py.")

    f_mng = ExitStack()
    cfg_file = get_pkg_resource(f_mng, 'config/import_KB.json')
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/KB/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')

    with open(cfg_file, 'r') as f:
        config = json.load(f)

    issues = kb_select_issues(
        base_dir=inp_dir, config=config, access_rights=""
    )

    assert issues is not None and len(issues) > 0
    assert all([i.journal in config['newspapers'] for i in issues])
    assert len([i.journal for i in issues if i.journal not in config['newspapers']])==0

    logger.info(f'There are {len(issues)} to ingest')

    import_issues(
        issues, out_dir, 
        s3_bucket=None, 
        issue_class=KbNewspaperIssue,
        image_dirs=None, 
        temp_dir=None, 
        chunk_size=None
    )

    logger.info("Finished test_selective_import, closing file manager.")
    f_mng.close()


