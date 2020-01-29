import os
import json
import pkg_resources
from dask import bag as db

from text_importer.utils import verify_imported_issues
from text_importer.importers.core import import_issues
from text_importer.importers.bl.detect import detect_issues
from text_importer.importers.bl.classes import BlNewspaperIssue

import logging

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the Olive XML importer with sample data."""

    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/BL/'
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
            issue_class=BlNewspaperIssue,
            image_dirs=None,
            temp_dir=pkg_resources.resource_filename('text_importer', 'data/temp/'),
            chunk_size=None
    )
    print(result)
