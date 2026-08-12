from text_preparation.importers import generic_importer
from text_preparation.importers.bnf.no_olr.classes import BnfNewspaperIssue
from text_preparation.importers.bnf import detect

if __name__ == "__main__":
    # configure the importer to use the desired BNF format
    detect.set_json_file("BNF-OCR")

    print(detect.JSON_FILE)

    generic_importer.main(BnfNewspaperIssue, detect.detect_issues, detect.select_issues)
