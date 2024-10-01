from text_preparation.importers import generic_importer
from text_preparation.importers.bnf_en.classes import BnfEnNewspaperIssue
from text_preparation.importers.bnf_en.detect import detect_issues, select_issues

if __name__ == "__main__":
    generic_importer.main(BnfEnNewspaperIssue, detect_issues, select_issues)
