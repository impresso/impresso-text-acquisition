import os
import json
import logging
import fire
from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)

ROOT_DATA_DIR = "/mnt/project_impresso/original/KB"
OUTPUT_JSON = "../../data/sample_data/KB/titles_check.kb.json"
LOG_FILE = (
    "/scratch/piconti/impresso/all_logs/text_prep/preprocessing/KB/id_and_check_titles.kb.log"
)
PPNA_LIST_FILEPATH = "../../data/sample_data/KB/title_ppna_mapping.kb.json"

# Placeholder list of valid NPA IDs (to be replaced by the actual list)
NPA_list = [
    "040031721",
    "040987654",
    "040123456",
    # ... add other IDs here
]


def check_title_directory(title_path):
    """Check if a title directory contains 'alto' and 'img'."""
    alto_path = os.path.join(title_path, "alto")
    img_path = os.path.join(title_path, "img")

    has_alto = os.path.isdir(alto_path)
    has_img = os.path.isdir(img_path)

    return {
        "has_alto": has_alto,
        "has_img": has_img,
    }


def main():

    init_logger(logger, logging.INFO, LOG_FILE)

    # first read the list of titles which should have been included
    with open(PPNA_LIST_FILEPATH, "r", encoding="utf-8") as fin:
        ppna_title_list = json.load(fin)

    found_ppna_list = []
    # there can be duplicate PPNA in the list, but only when there is public and private domain data
    ppna_to_title = {x["PPNA"]: x["Title"] for x in ppna_title_list}
    results = {}

    for porche in ["PORCHE-1", "PORCHE-2"]:
        impresso_path = os.path.join(ROOT_DATA_DIR, porche, "impresso")
        msg = f"Scanning {impresso_path}"
        print(msg)
        logging.info(msg)

        if not os.path.isdir(impresso_path):
            msg = f"Missing directory: {impresso_path}"
            print(msg)
            logging.info(msg)
            continue

        for ppna in os.listdir(impresso_path):
            title_path = os.path.join(impresso_path, ppna)
            if not os.path.isdir(title_path):
                continue

            found_ppna_list.append(ppna)

            status = check_title_directory(title_path)

            ppna_in_list = True
            # Only consider IDs in NPA_list
            if ppna not in ppna_to_title:
                ppna_in_list = False

            key = "ppna_in_dsa_list" if ppna_in_list else "ppna_NOT_in_dsa_list"
            title = "to be fetched in media list" if not ppna_in_list else ppna_to_title[ppna]
            if key in results:
                if ppna in results[key]:
                    msg = f"Warning!! ppna in results[key] = True for ppna={ppna} and key={key}"
                    print(msg)
                    logger.info(msg)
                if status["has_alto"] and status["has_img"]:
                    results[key]["complete_data"][ppna] = {
                        "porche": porche,
                        "path": title_path,
                        "title": title,
                        **status,
                    }
                else:
                    results[key]["incomplete_data"][ppna] = {
                        "porche": porche,
                        "path": title_path,
                        "title": title,
                        **status,
                    }
            else:
                if status["has_alto"] and status["has_img"]:
                    results[key] = {
                        "complete_data": {
                            ppna: {
                                "porche": porche,
                                "path": title_path,
                                "title": title,
                                **status,
                            }
                        },
                        "incomplete_data": {},
                    }
                else:
                    results[key] = {
                        "complete_data": {},
                        "incomplete_data": {
                            ppna: {
                                "porche": porche,
                                "path": title_path,
                                "title": title,
                                **status,
                            }
                        },
                    }

            msg = f"Checked title {ppna} in {porche} ({key}): status={'OK' if status['has_alto'] and status['has_img'] else 'Not OK'}"
            print(msg)
            logging.info(msg)

    titles_in_dsa_not_shared = [x for x in ppna_title_list if x["PPNA"] not in found_ppna_list]
    results["titles_in_dsa_NOT_shared"] = titles_in_dsa_not_shared

    # -----------------------------
    # Write results
    # -----------------------------
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    logging.info(f"Results written to {OUTPUT_JSON}")
    logging.info("=== KB directory scan complete ===")


if __name__ == "__main__":
    main()
