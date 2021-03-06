import logging
import os
import pkg_resources

from text_importer.importers.core import import_issues
from text_importer.importers.swa.classes import SWANewspaperIssue
from text_importer.importers.swa.detect import detect_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""
    
    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/SWA/'
            )
    
    access_rights = os.path.join(inp_dir, "access_rights.json")
    
    issues = detect_issues(
            base_dir=inp_dir,
            access_rights=access_rights
            )
    assert issues is not None
    assert len(issues) > 0
    
    result = import_issues(
            issues,
            out_dir=pkg_resources.resource_filename(
                    'text_importer',
                    'data/out'
                    ),
            s3_bucket=None,
            issue_class=SWANewspaperIssue,
            image_dirs="",
            temp_dir=pkg_resources.resource_filename(
                    'text_importer',
                    'data/temp/'
                    ),
            chunk_size=None
            )
    print(result)
