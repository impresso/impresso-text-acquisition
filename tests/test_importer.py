import pkg_resources
from impresso_commons.path.path_fs import detect_issues
from text_importer.importer import import_issues


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
