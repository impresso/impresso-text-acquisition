from text_preparation.importers import generic_importer
from text_preparation.importers.tetml.classes import TetmlNewspaperIssue
from text_preparation.importers.tetml.detect import (
    tetml_detect_issues,
    tetml_select_issues,
)

if __name__ == "__main__":
    generic_importer.main(TetmlNewspaperIssue, tetml_detect_issues, tetml_select_issues)
