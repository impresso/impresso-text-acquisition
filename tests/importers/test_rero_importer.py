import pkg_resources


from text_importer.importers.core import import_issues
from text_importer.importers.rero.detect import detect_issues
from text_importer.importers.rero.classes import ReroNewspaperIssue

import logging

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""

    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/RERO2/'
    )

    access_rights_file = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/RERO2/rero2_access_rights.json'
    )

    issues = detect_issues(
        base_dir=inp_dir,
        access_rights=access_rights_file
    )
    assert issues is not None
    assert len(issues) > 0

    result = import_issues(
        issues,
        out_dir=pkg_resources.resource_filename('text_importer', 'data/out/'),
        s3_bucket=None,
        issue_class=ReroNewspaperIssue,
        temp_dir=None,
        image_dirs=None
    )
    print(result)
