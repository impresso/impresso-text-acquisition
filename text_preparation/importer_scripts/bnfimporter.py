from text_preparation.importers import generic_importer
from text_preparation.importers.bnf.classes import BnfNewspaperIssue
from text_preparation.importers.bnf.detect import detect_issues, select_issues

if __name__ == "__main__":
    generic_importer.main(BnfNewspaperIssue, detect_issues, select_issues)
