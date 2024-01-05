from text_importer.importers.bcul.classes import BculNewspaperIssue
from text_importer.importers.bcul.detect import detect_issues as bcul_detect_issues, select_issues as bcul_select_issues
from text_importer.importers import generic_importer

if __name__ == '__main__':
    generic_importer.main(BculNewspaperIssue, bcul_detect_issues, bcul_select_issues)
