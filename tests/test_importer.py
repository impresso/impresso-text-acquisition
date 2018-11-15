import pkg_resources
from impresso_commons.path.path_fs import detect_issues

from text_importer.importer import import_issues
from text_importer.importers.lux import detect_issues as lux_detect_issues
from text_importer.importers.lux import import_issues as lux_import_issues


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
    issues = lux_detect_issues(inp_dir)
    assert issues is not None
    result = lux_import_issues(issues, out_dir)
    print(result)
