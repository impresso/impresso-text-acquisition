import json
import logging
import pytest
from datetime import date

from contextlib import ExitStack

from text_importer.utils import get_pkg_resource
from text_importer.importers.core import import_issues
from text_importer.importers.onb.classes import ONBNewspaperIssue
from text_importer.importers.onb.detect import detect_issues, select_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the ONB XML importer with sample data."""
    
    logger.info("Starting test_import_issues in test_onb_importer.py.")
    
    f_mng = ExitStack()
    # TODO, remove `anno_sample` once not necessary.
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/ONB/anno_sample') 
    ar_file = '' #get_pkg_resource(f_mng, 'data/sample_data/ONB/rero2_access_rights.json')
    out_dir = get_pkg_resource(f_mng, 'data/out/')

    issues = detect_issues(base_dir=inp_dir, access_rights=ar_file)

    assert issues is not None
    assert len(issues) > 0
    
    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=ONBNewspaperIssue,
        temp_dir=None,
        image_dirs=None,
        chunk_size=None
    )
    
    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()

@pytest.mark.parametrize("exclude_config", False)
@pytest.mark.parametrize("exclude_config", True)
def test_selective_import(exclude_config):
    """Tests selective ingestion of ONB data.

    What to ingest is specified in a JSON configuration file.
    """
    logger.info("Starting test_selective_import in test_onb_importer.py.")

    f_mng = ExitStack()
    cfg_file = get_pkg_resource(f_mng, 'config/import_ONB.json')
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/ONB/anno_sample')
    out_dir = get_pkg_resource(f_mng, 'data/out/')

    with open(cfg_file, 'r') as f:
        config = json.load(f)

    if exclude_config:
        logger.info("Modifying the config to exclude newspapers.")
        config['exclude_newspapers'] = config['newspapers']
        config['newspapers'] = []

    issues = select_issues(
        base_dir=inp_dir, config=config, access_rights=""
    )

    assert issues is not None and len(issues)>0

    if not config["exclude_newspapers"]:
        assert not exclude_config
        date_filters = {
            np: [date.fromisoformat(d.replace('/', '')) for d in v.split('-')]
            for np, v in config['newspapers'].items()
        }
        # all issues detected for each journal should follow the date restrictions.
        assert all([
            i.journal in date_filters and 
            date_filters[i.journal][0] <= i.date <= date_filters[i.journal][1] 
            for i in issues
        ])
    else:
        assert exclude_config
        date_filters = {
            np: [date.fromisoformat(d.replace('/', '')) for d in v.split('-')] 
            for np, v in config['exclude_newspapers'].items()
        }
        # all issues detected for each journal be outside of the excluding date restrictions 
        assert all([
            i.journal not in date_filters or 
            date_filters[i.journal][0] > i.date or i.date > date_filters[i.journal][1] 
            for i in issues
        ])

    logger.info(f'There are {len(issues)} to ingest')
    import_issues(
        issues, out_dir, 
        s3_bucket=None, 
        issue_class=ONBNewspaperIssue,
        image_dirs=None, 
        temp_dir=None, 
        chunk_size=None
    )

    logger.info("Finished test_selective_import, closing file manager.")
    f_mng.close()
