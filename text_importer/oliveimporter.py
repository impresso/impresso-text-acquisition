from text_importer.importers.olive.classes import OliveNewspaperIssue
from text_importer.importers.olive.detect import olive_select_issues, olive_detect_issues
from text_importer.importers import generic_importer

if __name__ == '__main__':
    generic_importer.main(OliveNewspaperIssue, olive_detect_issues, olive_select_issues)
