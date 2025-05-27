import os
import json
import re
import logging
import shutil
from copy import deepcopy
from ast import literal_eval
from datetime import datetime
import fire
import pandas as pd
from tqdm import tqdm
from PIL import Image
from impresso_essentials.utils import init_logger, chunk

logger = logging.getLogger(__name__)

ALIAS_NLPS_TO_SKIP = {
    "BGCH": ["0003056", "0003057"],
    "IPJO": ["0000071", "0000191"],
}

BL_OCR_BASE_PATH = "/mnt/project_impresso/original/BL"
BL_IMG_BASE_PATH = "/mnt/impresso_images_BL"
ALIAS_TO_NLP_PATH = "../../data/sample_data/BL/BL_alias_to_NLP.json"
REPROCESS_FILEPATH = "../../data/sample_data/BL/pages_to_reprocess.json"
OCR_FORMATS_FILEPATH = "../../data/sample_data/BL/BL_ocr_formats.json"
RENAMING_INFO_FILENAME = "renaming_info.json"

img_filename_pattern = "^d{7}_d{8}_d{4}.jp2$"


def page_num_from_filename(filename: str, prefix_to_remove: str) -> int:
    return int(filename.replace(prefix_to_remove, "").split(".")[0])


def rename_jp2_files_for_issue(input_dir, issue_id, nlp, orc_issue_dir, rename_files=False):

    to_reprocess = {}
    # Get all .jp2 files and sort them to ensure consistent ordering
    # jp2_files = sorted([f for f in os.listdir(directory) if f.lower().endswith(".jp2")])
    date_str = "".join(input_dir.split("/")[-4:-1])
    img_file_prefix = f"{nlp}_{date_str}_"

    # see if there is an existing info dict for this issue, otherwise intialize a new one
    info_filepath = os.path.join(input_dir, RENAMING_INFO_FILENAME)
    if os.path.exists(info_filepath):
        msg = f"{input_dir} - {info_filepath} file already present, reading it."
        print(msg)
        # logger.debug(msg)
        with open(info_filepath, "r", encoding="utf-8") as f:
            og_info_dict = json.load(f)
        info_dict = deepcopy(og_info_dict)
    else:
        og_info_dict = None
        info_dict = {}

    for og_filename in os.listdir(input_dir):

        # based on the current filename, identify what should be done

        # don't process the renaming info file or if the file is not a jp2
        if og_filename == RENAMING_INFO_FILENAME or not og_filename.endswith(".jp2"):
            msg = f"{input_dir} - file {og_filename} is not a page, ignoring it."
            print(msg)
            logger.info(msg)
            continue

        # Check that the filename is indeed formatted according to BL's conventions
        if img_file_prefix in og_filename:

            page_num = page_num_from_filename(og_filename, img_file_prefix)

            # define the new image filename according to Impresso conventions
            new_filename = f"{issue_id}-p{str(page_num).zfill(4)}.jp2"
            new_path = os.path.join(input_dir, new_filename)

            original_path = os.path.join(input_dir, og_filename)
            # Open the image to get dimensions
            with Image.open(original_path) as img:
                width, height = img.size

            # Store info for each page for easy access during ingestion
            info_dict[page_num] = {
                "original_filename": og_filename,
                "new_filename": new_filename,
                "issue_id": issue_id,
                "original_nlp": nlp,
                "img_dir_path": input_dir,
                "ocr_dir_path": orc_issue_dir,
                "width": width,
                "height": height,
            }

            try:
                if rename_files:
                    os.rename(original_path, new_path)
                    msg = f"renamed {og_filename} to {new_filename}"
                    print(msg)
                    logger.debug(msg)
                else:
                    print(f"Would have renamed {og_filename} to {new_filename}")
            except Exception as e:
                # if there was a problem when renaming, store this in the info dict
                msg = f"{issue_id} - there was an exception when renaming {og_filename} to {new_filename}: {e}"
                print(msg)
                logger.error(msg)
                info_dict[page_num]["had_exception_when_renaming"] = True

            if not og_info_dict or info_dict != og_info_dict:
                # already save the info dict in case, if it's not already saved
                with open(info_filepath, "w", encoding="utf-8") as fout:
                    json.dump(info_dict, fout)

        # if the found file had already been renamed, ensure it's in the info dict and skip
        elif issue_id in og_filename:

            if page_num_from_filename(og_filename, f"{issue_id}-p") not in info_dict:
                # for now only log these cases, as they should not occur. If they do, add the record to the dict
                msg = f"{input_dir} - file {og_filename} has already been renamed, but not in info file!"
            else:
                msg = f"{input_dir} - file {og_filename} has already been renamed, skipping."
            print(msg)
            logger.info(msg)

        # if the file is .jp2 but does not have any of the expected formats, save it for future processings
        else:
            msg = f"{input_dir} - file {og_filename} does not have the expected form - to reprocess."
            print(msg)
            logger.warning(msg)
            if issue_id in to_reprocess:
                to_reprocess[issue_id].append(og_filename)
            else:
                to_reprocess[issue_id] = [og_filename]

    return info_dict, to_reprocess
