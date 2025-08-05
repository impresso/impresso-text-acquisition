from text_preparation.importers import generic_importer
from text_preparation.importers.swissinfo.classes import SwissInfoRadioBulletinIssue
from text_preparation.importers.swissinfo.detect import (
    detect_issues,
    select_issues,
)

if __name__ == "__main__":
    print("inside swissinfo importer")
    generic_importer.main(SwissInfoRadioBulletinIssue, detect_issues, select_issues)
