import pkg_resources
from impresso_commons.path.path_fs import detect_issues

from text_importer.importer import import_issues
from text_importer.importers.lux.core import import_issues as lux_import_issues
from text_importer.importers.lux.detect import \
    detect_issues as lux_detect_issues, select_issues as lux_select_issues

import logging

logger = logging.getLogger(__name__)


def test_olive_import_issues():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Olive/'
    )
    issues = detect_issues(inp_dir)
    assert issues is not None
    assert len(issues) > 0

    result = import_issues(
        issues,
        "/Volumes/cdh_dhlab_2_arch/project_impresso/images/",
        None,
        pkg_resources.resource_filename('text_importer', 'data/out/'),
        pkg_resources.resource_filename('text_importer', 'data/temp/'),
        "olive",
        True  # whether to parallelize or not
    )
    print(result)


def test_lux_importer():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Luxembourg/'
    )
    out_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
    output_bucket = None  # this disables the s3 upload

    issues = lux_detect_issues(inp_dir)
    assert issues is not None
    lux_import_issues(issues, out_dir, s3_bucket=output_bucket)

    # TODO verify that issues processed are actually in the output folder
    # try to validate the JSON documents (pages and issues)


def test_lux_select():
    """Tests selective ingestion of BNL data.

    What to ingest is specified in a JSON configuration file.

    ..todo::

        - add support filtering/selection based on dates and date-ranges;
        - add support for exclusion of newspapers
    """
    cfg_file = pkg_resources.resource_filename(
        'text_importer',
        'config/import_BNL.json'
    )

    """
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/Luxembourg/'
    )
    """
    inp_dir = "/mnt/project_impresso/original/BNL/"
    out_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/out/debug/'
    )
    issues = lux_select_issues(cfg_file, inp_dir)
    assert issues
    logger.info(f'There are {len(issues)} to ingest')
    lux_import_issues(issues, out_dir, s3_bucket=None)
