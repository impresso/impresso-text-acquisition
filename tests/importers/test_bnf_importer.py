import logging

import pkg_resources

from text_importer.importers.bnf.classes import BnfNewspaperIssue
from text_importer.importers.bnf.detect import detect_issues
from text_importer.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the BNF importer with sample data."""

    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/BNF/'
    )

    temp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/tmp'
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
            issue_class=BnfNewspaperIssue,
            image_dirs=None,
            temp_dir=temp_dir,
            chunk_size=None
    )
    print(result)
