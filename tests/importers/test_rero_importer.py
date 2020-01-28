import bz2
import json
import logging
import os
from glob import glob

import pkg_resources

from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers.core import import_issues
from text_importer.importers.rero.classes import ReroNewspaperIssue
from text_importer.importers.rero.detect import detect_issues

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
    
    import_issues(
            issues,
            out_dir=pkg_resources.resource_filename('text_importer', 'data/out/'),
            s3_bucket=None,
            issue_class=ReroNewspaperIssue,
            temp_dir=None,
            image_dirs=None,
            chunk_size=None
            )


def check_image_coordinates(issue_data):
    items = issue_data['i']
    images = [i for i in items if i['m']['tp'] == CONTENTITEM_TYPE_IMAGE]
    return len(images) == 0 or all('c' in data['m'] and len(data['m']['c']) == 4 for data in images)


def test_image_coordinates():
    inp_dir = pkg_resources.resource_filename(
            'text_importer',
            'data/sample_data/RERO2/'
            )
    
    out_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
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
    
    journals = set([x.journal for x in issues])
    blobs = [f"{j}*.jsonl.bz2" for j in journals]
    issue_files = [f for b in blobs for f in glob(os.path.join(out_dir, b))]
    print(issue_files)
    
    for filename in issue_files:
        with bz2.open(filename, "rt") as bzinput:
            for line in bzinput:
                issue = json.loads(line)
                assert check_image_coordinates(issue), "Images do not have coordinates"
