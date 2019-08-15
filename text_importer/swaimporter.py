from text_importer.importers.swa.classes import SWANewspaperIssue
from text_importer.importers.swa.detect import detect_issues as swa_detect_issues, select_issues as swa_select_issues
from text_importer.importers import generic_importer

if __name__ == '__main__':
    generic_importer.main(SWANewspaperIssue, swa_detect_issues, swa_select_issues)
