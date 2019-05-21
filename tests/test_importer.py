import pkg_resources
from impresso_commons.path.path_fs import detect_issues
from text_importer.importer import import_issues
from text_importer.importers.olive import olive_import_issue


def test_olive_import_issues():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/'
    )
    issues = detect_issues(inp_dir)
    assert issues is not None
    assert len(issues) > 0

    result = import_issues(
        issues,
        "/mnt/project_impresso/images/",
        None,
        pkg_resources.resource_filename('text_importer', 'data/out/'),
        pkg_resources.resource_filename('text_importer', 'data/temp/'),
        "olive",
        True  # whether to parallelize or not
    )
    print(result)


def test_olive_import_images():
    inp_dir = pkg_resources.resource_filename(
        'text_importer',
        'data/sample_data/'
    )
    issues = detect_issues(inp_dir)
    olive_import_issue(
        issues[1],
        image_dir='/mnt/project_impresso/images/',
        out_dir=pkg_resources.resource_filename('text_importer', 'data/out/')
    )


# TODO: implement
def test_imported_data():
    """Verify that canonical IDs stay the same."""
    pass
