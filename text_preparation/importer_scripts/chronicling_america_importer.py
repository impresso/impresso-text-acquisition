from text_preparation.importers.chronicling_america.classes import ChroniclingAmericaNewspaperIssue
from text_preparation.importers.chronicling_america.detect import (
    detect_issues as ca_detect_issues,
    select_issues as ca_select_issues,
)
from text_preparation.importers import generic_importer

if __name__ == "__main__":
    generic_importer.main(
        ChroniclingAmericaNewspaperIssue,
        ca_detect_issues,
        ca_select_issues,
    )
