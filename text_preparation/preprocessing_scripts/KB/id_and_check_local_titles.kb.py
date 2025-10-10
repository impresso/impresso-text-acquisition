import os
import json
import logging

# -----------------------------
# Configuration
# -----------------------------

ROOT_DIR = "KB"
OUTPUT_JSON = "kb_titles_check.json"
LOG_FILE = "kb_titles_check.log"

# Placeholder list of valid NPA IDs (to be replaced by the actual list)
NPA_list = [
    "040031721",
    "040987654",
    "040123456",
    # ... add other IDs here
]

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logging.info("=== Starting KB directory scan ===")


# -----------------------------
# Helper function
# -----------------------------
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


# -----------------------------
# Main logic
# -----------------------------
results = {}

for porche in ["PORCHE-1", "PORCHE-2"]:
    impresso_path = os.path.join(ROOT_DIR, porche, "impresso")
    logging.info(f"Scanning {impresso_path}")

    if not os.path.isdir(impresso_path):
        logging.warning(f"Missing directory: {impresso_path}")
        continue

    for title_id in os.listdir(impresso_path):
        title_path = os.path.join(impresso_path, title_id)
        if not os.path.isdir(title_path):
            continue

        # Only consider IDs in NPA_list
        if title_id not in NPA_list:
            logging.info(f"Skipping {title_id} (not in NPA list)")
            continue

        logging.info(f"Checking title {title_id} in {porche}")
        status = check_title_directory(title_path)

        results[title_id] = {"porche": porche, "path": title_path, **status}

# -----------------------------
# Write results
# -----------------------------
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

logging.info(f"Results written to {OUTPUT_JSON}")
logging.info("=== KB directory scan complete ===")
