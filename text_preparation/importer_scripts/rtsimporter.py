from text_preparation.importers.rts.classes import RTSBroadcastIssue
from text_preparation.importers.rts.detect import (
    detect_issues as rts_detect_issues,
    select_issues as rts_select_issues,
)
from text_preparation.importers import generic_importer

if __name__ == "__main__":
    generic_importer.main(RTSBroadcastIssue, rts_detect_issues, rts_select_issues)
