import os
import json
import fire
import logging
from PIL import Image
import pymupdf
from tqdm import tqdm
from pdf2image import convert_from_path
from copy import deepcopy
from datetime import datetime
from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)

def rescale_coords(
    coords: list[float], 
    curr_size: tuple[float, float] = None, 
    dest_size: tuple[float, float] = None, 
    curr_res: float = None, 
    dest_res: float = None,
    xy_format: bool = True, 
    int_sc_factor: bool = False
) -> list[float]:
    """Scales image or bounding box coordinates based on image size or resolution.

    This function rescales a set of coordinates (`coords`) based on either:
    - The current and target image sizes (`curr_size` and `dest_size`).
    - The current and target resolutions (`curr_res` and `dest_res`).
    
    If `xy_format` is False and `curr_res`/`dest_res` are not provided, the function 
    estimates a resolution-based scaling factor using image sizes (`width * height`).

    When `xy_format` is True, the function assumes coordinates are in "x1, y1, x2, y2" format.
    Otherwise, it assumes "x, y, width, height" format.

    Args:
        coords (list[float]): List of coordinates to be scaled.
        curr_size (tuple[float, float], optional): Current image size (width, height).
            Required if `xy_format=True`. Defaults to None.
        dest_size (tuple[float, float], optional): Target image size (width, height).
            Required if `xy_format=True`. Defaults to None.
        curr_res (float, optional): Current image resolution (optional for `xy_format=False`).
        dest_res (float, optional): Target image resolution (optional for `xy_format=False`).
        xy_format (bool, optional): If True, treats coordinates as "x1, y1, x2, y2".
            If False, treats coordinates as "x, y, width, height". Defaults to True.
        int_sc_factor (bool, optional): If True, scales using integer division for factor calculation. 
            Defaults to False.

    Returns:
        list[float]: Scaled coordinates.

    Raises:
        ValueError: If required parameters (size or resolution) are missing.
        ValueError: If `curr_size` or `curr_res` contain zero.

    Example:
        >>> scale_coords([10, 20, 30, 40], (100, 200), (200, 400))
        [20.0, 40.0, 60.0, 80.0]
    """  
    # Validate input parameters
    if xy_format:
        if curr_size is None or dest_size is None:
            raise ValueError("When `xy_format` is True, `curr_size` and `dest_size` must be provided.")
        if 0 in curr_size:
            raise ValueError("Current image size must be non-zero values.")
    else:
        if curr_res is None or dest_res is None:
            if curr_size is None or dest_size is None:
                raise ValueError("When `xy_format` is False, either (`curr_res` and `dest_res`) or (`curr_size` and `dest_size`) must be provided.")
            # Estimate resolution from image size
            curr_res = curr_size[0] * curr_size[1]
            dest_res = dest_size[0] * dest_size[1]
        if curr_res == 0:
            raise ValueError("Current image resolution or size must be non-zero values.")

    # Compute scaling factors
    if xy_format:
        x_scale = int(dest_size[0]) / int(curr_size[0]) if int_sc_factor else dest_size[0] / curr_size[0]
        y_scale = int(dest_size[1]) / int(curr_size[1]) if int_sc_factor else dest_size[1] / curr_size[1]

        return [c * (x_scale if i % 2 == 0 else y_scale) for i, c in enumerate(coords)]
    else:
        scale_factor = int(dest_res) / int(curr_res) if int_sc_factor else dest_res / curr_res

        return [c * scale_factor for c in coords]
    

def remove_key_from_block(block:dict, key:str, page_num:int, block_idx: int) -> dict:
    if key in block:
        msg = f"page {page_num}, removing '{key}' from block {block_idx+1}"
        #print(msg)
        logger.debug(msg)
        del block[key]


def rescale_block_coords(block: dict, curr_img_size:tuple[float], dest_img_size:tuple[float]) -> dict:
    if 'bbox' in block:
        block['rescaled_bbox'] = rescale_coords(block['bbox'], curr_img_size, dest_img_size)
        msg = f" - rescaling block bbox."
        #print(msg)
        logger.debug(msg)
    if 'lines' in block:
        for l_idx, line in enumerate(block["lines"]):
            if 'bbox' in line:
                line['rescaled_bbox'] = rescale_coords(line['bbox'], curr_img_size, dest_img_size)
                msg = f"    - rescaling line bbox {l_idx+1}/{len(block['lines'])}."
                #print(msg)
                logger.debug(msg)
            for s_idx, span in enumerate(line["spans"]):
                if 'bbox' in span:
                    span['rescaled_bbox'] = rescale_coords(span['bbox'], curr_img_size, dest_img_size)
                    msg = f"      - rescaling line {l_idx+1} span bbox {s_idx+1}/{len(line['spans'])}."
                    #print(msg)
                    logger.debug(msg)

    return block

def process_blocks_of_page(page_num: int, page_text_dict: dict, page_image_size:tuple[float]) -> dict:
    # create a dict mapping each page number to its blocks, with and without lines

    # first extract the image size that the coordinates correspond to
    curr_ocr_coords_img_size = (page_text_dict['width'], page_text_dict['height'])
    lineless_blocks, blocks_w_lines = [], []
    # iterate over each block
    for i, og_block in enumerate(page_text_dict['blocks']):
        msg = f"Processing block {i+1}/{len(page_text_dict['blocks'])}:"
        #print(msg)
        logger.debug(msg)
        block = deepcopy(og_block)
        #remove the images and masks from the block
        remove_key_from_block(block, 'image', page_num, i)
        remove_key_from_block(block, 'mask', page_num, i)
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
        "blocks_without_lines": lineless_blocks
    }

def get_canonical_path(full_img_path: str) -> str:
    # create the canonical path for a given radio bulletin based on its orginal path
    # The canonical path is program/year/month/day/edition/files    

    filename = os.path.basename(full_img_path)
    # all the relevant information is separated by underscores, excluding the extension
    elements = filename.split('.')[0].split('_')

    program = elements[2]
    date = datetime.strptime(elements[3], "%Y%m%d").date()
    lang = elements[4]
    # if the there are more than 5 underscore separated elements, there are multiple editions for this day
    edition = chr (elements[5] + 96) if len(elements) > 5 else "a"
    
    # reconstruct the canonical path
    can_path = os.path.join(f"SOC_{program}", str(date.year), str(date.month).zfill(2), str(date.day).zfill(2), edition)

    return can_path, lang.lower()

def save_as_jp2(pil_imgs:Image, canonical_path:str, out_base_dir: str) -> str:
    canonical_issue_id = canonical_path.replace('/', '-')
    # define the full image out paths with their canonical page IDs from the current canonical issue ID for a given bulletin
    full_out_paths = [os.path.join(out_base_dir, "images", canonical_path, f"{canonical_issue_id}-p{str(i+1).zfill(4)}.jp2") for i in range(len(pil_imgs))]

    success = True
    try:
        for out_path, img in zip(full_out_paths, pil_imgs):
            msg = f"{os.path.basename(out_path)}: Saving image as JP2000 at path {out_path}"
            print(msg)
            logger.info(msg)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            img.save(out_path, format='JPEG2000')
    except Exception as e:
        msg = f"{canonical_issue_id}: There was an exception when trying to save the image as JP2000!! Exception: {e}"
        print(msg)
        logger.warning(msg)
        success = False

    return full_out_paths, success


def pdf_to_jp2_and_ocr_json(img_path: str, out_base_dir: str) -> tuple[str, bool]:
    # From the original image path:
    # 1. define the canonical path and id
    # 2. convert the pdf image to jp2
    # 3. process pages to extract the OCR and write them to a JSON
    # 4. return the ID and whether the conversion was successful

    canonical_path, lang = get_canonical_path(img_path)
    canonical_issue_id = canonical_path.replace("/", '-')
    
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
        "jp2_full_paths": jp2_full_paths
    }

    doc = pymupdf.open(img_path)
    # iterate over all pages and process its blocks:
    # - removing the images/masks
    # - rescaling the bounding boxes
    processed_pages = []
    for page_num in range(len(doc)):
        print(f"\n** PAGE {page_num} **")
        page = doc.load_page(page_num)
        page_dict = page.get_text('dict')
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
        can_id, success = pdf_to_jp2_and_ocr_json(os.path.join(input_base_dir, filename), out_base_dir)
        
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