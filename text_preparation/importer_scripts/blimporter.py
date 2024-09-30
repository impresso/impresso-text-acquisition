from text_preparation.importers.bl.classes import BlNewspaperIssue
from text_preparation.importers.bl.detect import (
    detect_issues as bl_detect_issues,
    select_issues as bl_select_issues,
)
from text_preparation.importers import generic_importer

if __name__ == "__main__":
    generic_importer.main(BlNewspaperIssue, bl_detect_issues, bl_select_issues)
