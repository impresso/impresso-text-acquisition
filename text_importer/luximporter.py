from text_importer.importers.lux.classes import LuxNewspaperIssue
from text_importer.importers.lux.detect import detect_issues as lux_detect_issues, select_issues as lux_select_issues
from text_importer.importers.mets_alto import metsalto_importer

if __name__ == '__main__':
    metsalto_importer.main(LuxNewspaperIssue, lux_detect_issues, lux_select_issues)
