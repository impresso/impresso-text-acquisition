from text_preparation.importers.lux.classes import LuxNewspaperIssue
from text_preparation.importers.lux.detect import (
    detect_issues as lux_detect_issues,
    select_issues as lux_select_issues,
)
from text_preparation.importers import generic_importer

if __name__ == "__main__":
    generic_importer.main(LuxNewspaperIssue, lux_detect_issues, lux_select_issues)
