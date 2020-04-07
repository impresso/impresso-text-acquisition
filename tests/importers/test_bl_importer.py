import logging
import pathlib
import pkg_resources

from text_importer.importers.bl.classes import BlNewspaperIssue
from text_importer.importers.bl.detect import detect_issues
from text_importer.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""
    
    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/BL/'
            )
    
    tmp_dir = pkg_resources.resource_filename('text_importer', 'data/temp/')
    pathlib.Path(tmp_dir).mkdir(exist_ok=True)
    issues = detect_issues(
            base_dir=inp_dir,
            access_rights=None,
            tmp_dir=tmp_dir
            )
    assert issues is not None
    assert len(issues) > 0

    output_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
    try:
        pathlib.Path(output_dir).mkdir()
    except:
        pass
    result = import_issues(
            issues,
            out_dir=output_dir,
            s3_bucket=None,
            issue_class=BlNewspaperIssue,
            image_dirs=None,
            temp_dir=tmp_dir,
            chunk_size=None
            )
    print(result)
