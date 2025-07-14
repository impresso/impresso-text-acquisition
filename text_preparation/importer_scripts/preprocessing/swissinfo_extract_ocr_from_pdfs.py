"""This script processes PDF files by converting them to JPEG2000 images (JP2) and extracting OCR data.

The main functionalities include:

- Rescaling the bounding box coordinates.
- Processing documents to define their canonical path and id.
- Converting PDF images to JP2 format.
- Extracting OCR text and saving it as a JSON file.

Usage:
    python script.py --log_file log.txt --input_base_dir /path/to/pdf --out_base_dir /path/to/output
"""

import os
import logging
from copy import deepcopy
from datetime import datetime
import json
import fire
from PIL import Image
import pymupdf
from tqdm import tqdm
from pdf2image import convert_from_path
from text_preparation.utils import rescale_coords
from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)


def remove_key_from_block(block: dict, key: str, page_num: int, block_idx: int) -> dict:
    """Remove a specified key from a block dictionary if it exists, and logs the action.

    Args:
        block (dict): The block dictionary from which the key should be removed.
        key (str): The key to remove from the block.
        page_num (int): The page number associated with the block (for logging purposes).
        block_idx (int): The index of the block on the page (for logging purposes).

    Returns:
        dict: The updated block dictionary with the key removed if it was present.
    """

    if key in block:
        msg = f"page {page_num}, removing '{key}' from block {block_idx+1}"
        # print(msg)
        logger.debug(msg)
        del block[key]


def rescale_block_coords(
    block: dict, curr_img_size: tuple[float], dest_img_size: tuple[float]
) -> dict:
    """Rescale bounding box coordinates in a block and its nested lines and spans.

    Args:
        block (dict): A dictionary representing a layout block that may contain a "bbox",
            and optionally a list of "lines", each of which may contain "spans".
        curr_img_size (tuple[float]): The current size of the image as (width, height).
        dest_img_size (tuple[float]): The target size of the image as (width, height).

    Returns:
        dict: The updated block dictionary with rescaled bounding boxes added under
        the key "rescaled_bbox" at each relevant level (block, lines, spans).
    """

    if "bbox" in block:
        block["rescaled_bbox"] = rescale_coords(block["bbox"], curr_img_size, dest_img_size)
        # print(" - rescaling block bbox.")
        logger.debug(" - rescaling block bbox.")

    if "lines" in block:
        for l_idx, line in enumerate(block["lines"]):
            if "bbox" in line:
                line["rescaled_bbox"] = rescale_coords(line["bbox"], curr_img_size, dest_img_size)
                msg = f"    - rescaling line bbox {l_idx+1}/{len(block['lines'])}."
                # print(msg)
                logger.debug(msg)

            for s_idx, span in enumerate(line["spans"]):
                if "bbox" in span:
                    span["rescaled_bbox"] = rescale_coords(
                        span["bbox"], curr_img_size, dest_img_size
                    )
                    msg = f"      - rescaling line {l_idx+1} span bbox {s_idx+1}/{len(line['spans'])}."
                    # print(msg)
                    logger.debug(msg)

    return block


def process_blocks_of_page(
    page_num: int, page_text_dict: dict, page_image_size: tuple[float]
) -> dict:
    """Process OCR blocks from a page by cleaning, rescaling, and organizing them.

    Cleans and prepares OCR block data for a specific page by:
    - Removing unnecessary keys (like images and masks) from each block.
    - Rescaling all bounding box coordinates to match the provided image size.
    - Separating blocks that contain lines from those that do not.

    Args:
        page_num (int): The number of the page being processed (used for logging and output).
        page_text_dict (dict): A dictionary representing the OCR data for the page.
            Must contain "width", "height", and a list of "blocks".
        page_image_size (tuple[float]): The target image size (width, height) for which
            bounding boxes should be rescaled.

    Returns:
        dict: A dictionary containing the processed information for the page, with keys:
            - "page_num": The page number.
            - "ocr_page_size": The original OCR coordinate space (width, height).
            - "jp2_img_size": The target image size used for rescaling.
            - "blocks_with_lines": List of blocks that contain text lines.
            - "blocks_without_lines": List of blocks that do not contain text lines.
    """
    # create a dict mapping each page number to its blocks, with and without lines
    # first extract the image size that the coordinates correspond to
    curr_ocr_coords_img_size = (page_text_dict["width"], page_text_dict["height"])
    lineless_blocks, blocks_w_lines = [], []

    # iterate over each block
    for i, og_block in enumerate(page_text_dict["blocks"]):

        msg = f"Processing block {i+1}/{len(page_text_dict['blocks'])}:"
        # print(msg)
        logger.debug(msg)
        block = deepcopy(og_block)

        # remove the images and masks from the block
        remove_key_from_block(block, "image", page_num, i)
        remove_key_from_block(block, "mask", page_num, i)

        # rescale all the bounding boxes inside the block to match the jp2 image size
        block = rescale_block_coords(block, curr_ocr_coords_img_size, page_image_size)

        if "lines" in block:
            blocks_w_lines.append(block)
        else:
            lineless_blocks.append(block)

    # return a dict containing all relevant information extracted for this page
    return {
        "page_num": page_num,
        "ocr_page_size": curr_ocr_coords_img_size,
        "jp2_img_size": page_image_size,
        "blocks_with_lines": blocks_w_lines,
        "blocks_without_lines": lineless_blocks,
    }


def get_canonical_path(full_img_path: str) -> str:
    """Generate a canonical path from a radio bulletin image file path.

    Extracts metadata from the file name (program, date, edition, language) and
    constructs a standardized ("canonical") directory path in the format:
    `SOC_<program>/<year>/<month>/<day>/<edition>`. Also returns the language
    extracted from the filename in lowercase.

    The filename is expected to follow this structure:
    `<prefix>_<prefix>_<program>_<YYYYMMDD>_<LANG>[_<EDITION>].<ext>`

    Args:
        full_img_path (str): The full file path to the image.

    Returns:
        tuple[str, str]: A tuple containing:
            - The canonical path as a string.
            - The language code as a lowercase string.

    Raises:
        ValueError: If the date string cannot be parsed or required elements are missing.
    """
    # create the canonical path for a given radio bulletin based on its orginal path
    # The canonical path is program/year/month/day/edition/files
    filename = os.path.basename(full_img_path)

    # all the relevant information is separated by underscores, excluding the extension
    elements = filename.split(".")[0].split("_")
    program = elements[2]
    date = datetime.strptime(elements[3], "%Y%m%d").date()
    lang = elements[4]

    # if the there are more than 5 underscore separated elements, there are multiple editions for this day
    edition = chr(elements[5] + 96) if len(elements) > 5 else "a"

    # reconstruct the canonical path
    can_path = os.path.join(
        f"SOC_{program}", str(date.year), str(date.month).zfill(2), str(date.day).zfill(2), edition
    )

    return can_path, lang.lower()


def save_as_jp2(pil_imgs: Image, canonical_path: str, out_base_dir: str) -> str:
    """Save a list of PIL images as JPEG 2000 (`.jp2`) files in a structured directory.

    The function constructs output file paths using a canonical issue ID, derived from
    the given `canonical_path`. Each image is saved with a sequential page number in the
    format: `"{canonical_issue_id}-pXXXX.jp2"`

    Args:
        pil_imgs (list[Image.Image]): A list of PIL images to be saved.
        canonical_path (str): The structured canonical path for the images.
        out_base_dir (str): The base output directory where the images should be saved.

    Returns:
        tuple[list[str], bool]: List of file paths where the images were saved and whether
            all images were successfully saved (`True`) or if an error occurred (`False`).

    Raises:
        OSError: If there is an issue creating directories or saving images.
    """
    canonical_issue_id = canonical_path.replace("/", "-")

    # define the full image out paths with their canonical page IDs from the current canonical issue ID for a given bulletin
    full_out_paths = [
        os.path.join(
            out_base_dir, "images", canonical_path, f"{canonical_issue_id}-p{str(i+1).zfill(4)}.jp2"
        )
        for i in range(len(pil_imgs))
    ]

    success = True
    try:
        for out_path, img in zip(full_out_paths, pil_imgs):
            msg = f"{os.path.basename(out_path)}: Saving image as JP2000 at path {out_path}"
            print(msg)
            logger.info(msg)

            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            img.save(out_path, format="JPEG2000")

    except Exception as e:
        msg = f"{canonical_issue_id}: There was an exception when trying to save the image as JP2000!! Exception: {e}"
        print(msg)
        logger.warning(msg)

        success = False

    return full_out_paths, success


def pdf_to_jp2_and_ocr_json(img_path: str, out_base_dir: str) -> tuple[str, bool]:
    """Convert a PDF to JPEG 2000 images and extracts OCR data into a JSON file.

    This function processes a given PDF file by:
    1. Determining its canonical path and ID.
    2. Converting its pages to JP2 (JPEG 2000) format.
    3. Extracting OCR text and bounding box data from each page.
    4. Saving the extracted OCR data into a JSON file.
    5. Returning the canonical ID and the success status of the operation.

    If the OCR JSON file already exists, the function skips processing and returns early.

    Args:
        img_path (str): The file path of the input PDF document.
        out_base_dir (str): The base directory where the processed images and JSON should be saved.
        all_filenames (list[str]): A list of all filenames in the dataset to determine the edition letter.

    Returns:
        tuple[str, bool]: A tuple containing:
            - The canonical issue ID of the processed document.
            - A boolean indicating whether processing was successful (`True`) or if an error occurred (`False`).

    Raises:
        OSError: If there is an issue with file I/O operations (e.g., saving JP2 images or writing the JSON).
        Exception: If any other unexpected error occurs.
    """

    canonical_path, lang = get_canonical_path(img_path)
    canonical_issue_id = canonical_path.replace("/", "-")
    full_json_out_path = os.path.join(out_base_dir, canonical_path, f"{canonical_issue_id}.json")

    # Not reprocessing bulletins that were already processed
    if os.path.exists(full_json_out_path):
        msg = f"{canonical_issue_id} - The JSON file corresponding to {img_path} already exists, it will not be processed again!"
        print(msg)
        logger.info(msg)

        return canonical_issue_id, True

    pil_img = convert_from_path(img_path)
    jp2_full_paths, jp2_success = save_as_jp2(pil_img, canonical_path, out_base_dir)

    # create the JSON only if the images could be converted to always ensure both files exist
    if not jp2_success:
        msg = f"{canonical_issue_id} - {img_path}: There was a problem saving the JP2000 version of the images!! Not processing it into a JSON"
        print(msg)
        logger.warning(msg)

        return canonical_issue_id, False

    ocr_json = {
        "canonical_id": canonical_issue_id,
        "lang": lang,
        "original_path": img_path,
        "jp2_full_paths": jp2_full_paths,
    }

    doc = pymupdf.open(img_path)

    # iterate over all pages and process its blocks:
    # - removing the images/masks
    # - rescaling the bounding boxes
    processed_pages = []

    for page_num in range(len(doc)):
        print(f"\n** PAGE {page_num} **")
        page = doc.load_page(page_num)
        page_dict = page.get_text("dict")
        processed_pages.append(process_blocks_of_page(page_num, page_dict, pil_img[page_num].size))

    ocr_json["ocr_pages"] = processed_pages

    # Allow a tracking of what images were successfully converted or not
    try:
        # save the resulting OCR JSON to disk:
        os.makedirs(os.path.dirname(full_json_out_path), exist_ok=True)

        msg = f"{canonical_issue_id}: Saving OCR as JSON at path {full_json_out_path}"
        print(msg)
        logger.info(msg)

        with open(full_json_out_path, mode="w") as fout:
            json.dump(ocr_json, fout)

        return canonical_issue_id, True

    except Exception as e:
        msg = f"{canonical_issue_id}: There was an exception when writing the OCR JSON of {img_path}!! Exception: {e}"
        print(msg)
        logger.warning(msg)

        return canonical_issue_id, False


def main(
    log_file: str,
    input_base_dir: str = "/mnt/project_impresso/original/SWISSINFO/WW2-SOC-bulletins/ww2-PDF",
    out_base_dir: str = "/mnt/impresso_swissinfo_json",
    verbose: bool = False,
) -> None:

    init_logger(logger, logging.INFO if not verbose else logging.DEBUG, log_file)
    processed_ids = []
    failed_ids = []
    filenames = os.listdir(input_base_dir)

    for file_num, filename in tqdm(enumerate(filenames)):

        msg = f"\n{15*'-'} Processing file {file_num+1}/{len(filenames)} {15*'-'}\n"
        print(msg)
        logger.info(msg)
        can_id, success = pdf_to_jp2_and_ocr_json(
            os.path.join(input_base_dir, filename), out_base_dir
        )

        if success:
            processed_ids.append(can_id)
            msg = f"{15*'-'} Done processing file {file_num+1}/{len(filenames)}, successfully processed {len(processed_ids)} files, failed processsing {len(failed_ids)} files {15*'-'}\n"
            print(msg)
            logger.info(msg)

        else:
            failed_ids.append(can_id)
            msg = f"{15*'-'} Problem processing file {file_num+1}/{len(filenames)}, successfully processed {len(processed_ids)} files, failed processsing {len(failed_ids)} files {15*'-'}\n"
            print(msg)
            logger.info(msg)


if __name__ == "__main__":
    fire.Fire(main)
