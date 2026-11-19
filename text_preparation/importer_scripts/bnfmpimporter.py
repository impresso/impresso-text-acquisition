from text_preparation.importers import generic_importer
from text_preparation.importers.bnf.mp_olr.classes import BnfMpNewspaperIssue
from text_preparation.importers.bnf import detect

if __name__ == "__main__":
    # configure the importer to use the desired BNF format
    detect.set_json_file("MP-OLR")

    print(detect.JSON_FILE)

    generic_importer.main(BnfMpNewspaperIssue, detect.detect_issues, detect.select_issues)
