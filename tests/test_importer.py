import pkg_resources
from impresso_commons.path import detect_issues


def test_import_issues():
    inp_dir = pkg_resources.resource_filename('text_importer', 'data/out/')
    issues = detect_issues(inp_dir)
    assert issues is not None
    assert len(issues) > 0
