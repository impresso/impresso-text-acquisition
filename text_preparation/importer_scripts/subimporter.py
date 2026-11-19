from text_preparation.importers import generic_importer
from text_preparation.importers.sub.classes import SubNewspaperIssue
from text_preparation.importers.sub.detect import detect_issues, select_issues

if __name__ == "__main__":
    generic_importer.main(SubNewspaperIssue, detect_issues, select_issues)
