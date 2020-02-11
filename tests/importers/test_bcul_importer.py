import logging

import pkg_resources

from text_importer.importers.bcul.classes import BCULNewspaperIssue
from text_importer.importers.bcul.detect import detect_issues
from text_importer.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""
    
    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/BCUL/'
            )
    
    issues = detect_issues(
            base_dir=inp_dir,
            access_rights=None
            )
    assert issues is not None
    assert len(issues) > 0
    
    result = import_issues(
            issues,
            out_dir=pkg_resources.resource_filename('text_importer', 'data/out/'),
            s3_bucket=None,
            issue_class=BCULNewspaperIssue,
            image_dirs=None,
            temp_dir=pkg_resources.resource_filename('text_importer', 'data/temp/'),
            chunk_size=None
            )
    print(result)
