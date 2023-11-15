import logging
import bz2
import os
import json
from glob import glob

from contextlib import ExitStack

from text_importer.utils import get_pkg_resource
from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers.bnf_en.classes import BnfEnNewspaperIssue
from text_importer.importers.bnf_en.detect import detect_issues
from text_importer.importers.core import import_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the BNF-EN XML importer with sample data."""

    logger.info("Starting test_import_issues in test_bnf_en_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng,'data/sample_data/BNF-EN/')
    out_dir = get_pkg_resource(f_mng,'data/out/')
    tmp_dir = get_pkg_resource(f_mng,'data/tmp/')

    issues = detect_issues(base_dir=inp_dir, access_rights="")
    assert issues is not None
    assert len(issues) > 0

    import_issues(
            issues,
            out_dir=out_dir,
            s3_bucket=None,
            issue_class=BnfEnNewspaperIssue,
            image_dirs=None,
            temp_dir=tmp_dir,
            chunk_size=None
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()


def check_image_coordinates_and_iiif(issue_data):
    items = issue_data['i']
    imgs = [i for i in items if i['m']['tp'] == CONTENTITEM_TYPE_IMAGE]
    if len(imgs) == 0:
        return True
    else:
        return (all('c' in data and len(data['c']) == 4 for data in imgs) and 
                all('iiif_link' in data['m'] and "info.json" in data['m']['iiif_link'] for data in imgs))


def test_image_coordinates():

    logger.info("Starting test_image_coordinates in test_bnf_en_importer.py")
    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, 'data/sample_data/BNF-EN/')
    out_dir = get_pkg_resource(f_mng, 'data/out/')
    
    issues = detect_issues(base_dir=inp_dir, access_rights='')
    
    assert issues is not None
    assert len(issues) > 0
    
    journals = set([x.journal for x in issues])
    blobs = [f"{j}*.jsonl.bz2" for j in journals]
    issue_files = [f for b in blobs for f in glob(os.path.join(out_dir, b))]
    logger.info(issue_files)
    
    for filename in issue_files:
        with bz2.open(filename, "rt") as bzinput:
            for line in bzinput:
                issue = json.loads(line)
                assert check_image_coordinates_and_iiif(issue), (
                    "Images do not have coordinates"
                )

    logger.info("Finished test_image_coordinate, closing file manager.")
    f_mng.close()
