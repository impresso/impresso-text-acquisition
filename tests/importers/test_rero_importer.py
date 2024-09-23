import bz2
import json
import logging
import os
from glob import glob

from contextlib import ExitStack

from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.utils import get_pkg_resource

from text_preparation.importers import CONTENTITEM_TYPE_IMAGE
from text_preparation.importers.core import import_issues
from text_preparation.importers.rero.classes import ReroNewspaperIssue
from text_preparation.importers.rero.detect import detect_issues

logger = logging.getLogger(__name__)


def test_import_issues():
    """Test the RERO XML importer with sample data."""

    logger.info("Starting test_import_issues in test_rero_importer.py.")

    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/RERO2/", "text_preparation")
    ar_file = get_pkg_resource(
        f_mng, "data/sample_data/RERO2/rero2_access_rights.json", "text_preparation"
    )
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
    tmp_dir = get_pkg_resource(f_mng, "data/temp/", "text_preparation")

    test_manifest = DataManifest(
        data_stage="canonical",
        s3_output_bucket="10-canonical-sandbox",
        s3_input_bucket=None,
        git_repo="../../",
        temp_dir=tmp_dir,
        staging=True,
        is_patch=False,
        patched_fields=None,
        previous_mft_path=None,
        only_counting=False,
        push_to_git=False,
        notes="Manifest from RERO test_import_issues().",
    )

    issues = detect_issues(base_dir=inp_dir, access_rights=ar_file)

    assert issues is not None
    assert len(issues) > 0

    import_issues(
        issues,
        out_dir=out_dir,
        s3_bucket=None,
        issue_class=ReroNewspaperIssue,
        temp_dir=None,
        image_dirs=None,
        chunk_size=None,
        manifest=test_manifest,
    )

    logger.info("Finished test_import_issues, closing file manager.")
    f_mng.close()


def check_image_coordinates(issue_data):
    items = issue_data["i"]
    imgs = [i for i in items if i["m"]["tp"] == CONTENTITEM_TYPE_IMAGE]
    return len(imgs) == 0 or all(
        "c" in data["m"] and len(data["m"]["c"]) == 4 for data in imgs
    )


def check_image_coordinates_and_iiif(issue_data):
    """This function is derived from `check_image_coordinates`.

    It checks the changes made in the following commit https://github.com/impresso/impresso-text-acquisition/commit/c6009d83f3801919e3e4944362dd472a1ede20fb,
    based on the issues https://github.com/impresso/impresso-text-acquisition/issues/104 and https://github.com/impresso/impresso-text-acquisition/issues/105.
    """
    items = issue_data["i"]
    imgs = [i for i in items if i["m"]["tp"] == CONTENTITEM_TYPE_IMAGE]
    if len(imgs) == 0:
        return True
    else:
        return all("c" in data and len(data["c"]) == 4 for data in imgs) and all(
            "iiif_link" in data["m"] and "info.json" in data["m"]["iiif_link"]
            for data in imgs
        )


def test_image_coordinates():

    logger.info("Starting test_image_coordinates in test_rero_importer.py")
    f_mng = ExitStack()
    inp_dir = get_pkg_resource(f_mng, "data/sample_data/RERO2/", "text_preparation")
    ar_file = get_pkg_resource(
        f_mng, "data/sample_data/RERO2/rero2_access_rights.json", "text_preparation"
    )
    out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")

    issues = detect_issues(base_dir=inp_dir, access_rights=ar_file)

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
                assert check_image_coordinates_and_iiif(
                    issue
                ), "Images do not have coordinates"

    logger.info("Finished test_image_coordinate, closing file manager.")
    f_mng.close()
