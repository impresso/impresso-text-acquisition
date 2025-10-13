from text_preparation.importers import generic_importer
from text_preparation.importers.olive.classes import OliveNewspaperIssue
from text_preparation.importers.olive.detect import (
    olive_detect_issues,
    olive_select_issues,
)

if __name__ == "__main__":
    generic_importer.main(OliveNewspaperIssue, olive_detect_issues, olive_select_issues)
