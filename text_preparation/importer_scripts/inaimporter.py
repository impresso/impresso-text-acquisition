from text_preparation.importers import generic_importer
from text_preparation.importers.ina.classes import INABroadcastIssue
from text_preparation.importers.ina.detect import (
    detect_issues,
    select_issues,
)

if __name__ == "__main__":
    print("inside INA importer")
    generic_importer.main(INABroadcastIssue, detect_issues, select_issues)
