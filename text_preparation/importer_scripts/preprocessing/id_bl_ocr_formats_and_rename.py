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
import traceback
from tqdm import tqdm
from PIL import Image
from bs4 import BeautifulSoup
from impresso_essentials.utils import init_logger, chunk

logger = logging.getLogger(__name__)

ALIAS_NLPS_TO_SKIP = {
    "BGCH": ["0003056", "0003057"],
    "IPJO": ["0000071", "0000191"],
}

BL_OCR_BASE_PATH = "/mnt/project_impresso/original/BL"
BL_OCR_W_BASE_PATH = "/mnt/impresso_ocr_BL"
BL_IMG_BASE_PATH = "/mnt/impresso_images_BL"
ALIAS_TO_NLP_PATH = "../../data/sample_data/BL/BL_alias_to_NLP.json"
REPROCESS_FILEPATH = "../../data/sample_data/BL/renaming_errors_or_to_reprocess.json"
OCR_FORMATS_FILEPATH = "../../data/sample_data/BL/BL_ocr_formats.json"
RENAMING_INFO_FILENAME = "renaming_info.json"

KNOWN_OCR_FORMATS = ["OmniPage-NLP", "BL-ALIAS", "ABBYY-NLP", "ABBYY-ALIAS", "Nuance-NLP"]
VALID_OCR_FORMATS = ["OmniPage-NLP"]

img_filename_pattern = "^d{7}_d{8}_d{4}.jp2$"


def page_num_from_filename(filename: str, prefix_to_remove: str) -> int:
    return int(filename.replace(prefix_to_remove, "").split(".")[0])


def rename_jp2_files_for_issue(input_dir, issue_id, nlp, orc_issue_dir, rename_files=True):

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
                    # print(msg)
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
                # save the info dict, if it's not already saved
                with open(info_filepath, "w", encoding="utf-8") as fout:
                    json.dump(info_dict, fout)

        # if the found file had already been renamed, ensure it's in the info dict and skip
        elif issue_id in og_filename:
            pg_num = str(page_num_from_filename(og_filename, f"{issue_id}-p"))
            if pg_num not in info_dict:
                # for now only log these cases, as they should not occur. If they do, add the record to the dict
                msg = f"{input_dir} - file {og_filename} has already been renamed, but not in info file!"
                print(msg)
                logger.info(msg)
            elif pg_num == 1:
                msg = f"{input_dir} - file {og_filename} has already been renamed, skipping. (Only printing for page 1)"
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


def identify_edition(img_day_dir, nlp, nlps_for_alias):
    # if there are multiple NLPs for this alias,
    # check if we have multiple editions, and which one it corresponds to
    all_files_for_nlp = {}
    editions_with_files = set()
    nlps_per_edition = {}

    for root, dirs, files in os.walk(img_day_dir):
        if files and not dirs:
            year, month, day, edition = root.split("/")[-4:]
            ed_files_for_nlp = [
                os.path.join(root, f)
                for f in files
                if (nlp in f or f"{year}-{month}-{day}" in f) and f != RENAMING_INFO_FILENAME
            ]
            if ed_files_for_nlp:
                # only add this edition if there are files for this NLP inside
                if edition not in all_files_for_nlp:
                    all_files_for_nlp[edition] = ed_files_for_nlp
                else:
                    all_files_for_nlp[edition].extend(ed_files_for_nlp)
                editions_with_files.add(edition)

            if len(nlps_for_alias) > 1:
                other_nlps = [n for n in nlps_for_alias if n != nlp]
                files_of_other_nlps = [n for n in other_nlps if any([n in f for f in files])]
                if files_of_other_nlps:
                    nlps_per_edition[edition] = files_of_other_nlps

    editions_with_files = list(editions_with_files)

    if len(editions_with_files) == 1 and editions_with_files[0] not in nlps_per_edition:
        # if we find this nlp only in this edition, and no other nlps there, it's the correct one
        return editions_with_files[0], None

    elif len(editions_with_files) > 1:
        msg = f"{nlp} - {img_day_dir} - Edition not fully identifiable (case 1)! keeping in logs for next processings."
        print(msg)
        logger.warning(msg)
        return None, {
            img_day_dir: (
                nlp,
                1,
                f"Files for this NLP in multiple editions: all_files_for_nlp = {all_files_for_nlp}",
            )
        }
    elif any(e in nlps_per_edition for e in editions_with_files):
        msg = f"{nlp} - {img_day_dir} - Edition not fully identifiable (case 2)! keeping in logs for next processings."
        print(msg)
        logger.warning(msg)
        return None, {
            img_day_dir: (
                nlp,
                2,
                f"Mulitple NLPs represented for some editions: nlps_per_edition = {nlps_per_edition}",
            )
        }


def id_bl_alias_format(alias, ocr_files, ocr_issue_dir, ocr_formats) -> dict[str, list]:
    _, nlp, year, month, day = ocr_issue_dir.split(alias)[-1].split("/")
    expected_prefix = f"WO1_{alias}_{year}_{month}_{day}-"
    matching_files = [f for f in ocr_files if expected_prefix in f and f.endswith(".xml")]

    if matching_files:
        # add the matching files to the BL-ALIAS OCR format if there are any
        ocr_formats["BL-ALIAS"] = matching_files

    return ocr_formats


def id_mets_formats(alias, ocr_files, mets_file, ocr_issue_dir, ocr_formats):
    _, nlp, year, month, day = ocr_issue_dir.split(alias)[-1].split("/")

    # the alias present is not always the one chosen (although almost always)
    abbyy_alias_prefix = f"-{year}-{month}-{day}"
    nlp_prefix = f"{nlp}_{year}{month}{day}"

    if mets_file.startswith(nlp_prefix):
        # the possible OCR formats are ABBYY-NLP or OmniPage-NLP
        page_files = [f for f in ocr_files if f.startswith(nlp_prefix) and f != mets_file]

        pg_idx = 0
        software_names = ["ccs docworks"]
        while list(set(software_names)) == ["ccs docworks"]:

            if pg_idx >= len(page_files):
                msg = f"{ocr_issue_dir} - pg_idx>=len(page_files): pg_idx={pg_idx}, page_files={page_files}."
                print(msg)
                logger.warning(msg)
                break

            # open a page to differentiate between the two possible formats
            with open(
                os.path.join(ocr_issue_dir, page_files[pg_idx]), "r", encoding="utf-8"
            ) as f:
                raw_xml_page = f.read()

            nlp_page_doc = BeautifulSoup(raw_xml_page, "xml")

            software_names = [
                e.get_text().lower() for e in nlp_page_doc.find_all("softwareName")
            ]

            if pg_idx > 0:
                # if the page only has this software name, retry with another page
                msg = f"{ocr_issue_dir} - Only found software names {software_names} in page {pg_idx+1}, retrying with another page."
                print(msg)
                logger.info(msg)
            else:
                pg_idx += 1

        if "omnipage" in software_names:
            # Omni is the software name
            ocr_formats["OmniPage-NLP"] = page_files + [mets_file]
        elif "finereader" in software_names:
            ocr_formats["ABBYY-NLP"] = page_files + [mets_file]
        elif "nuance" in software_names:
            ocr_formats["Nuance-NLP"] = page_files + [mets_file]
        else:
            msg = f"{ocr_issue_dir} - The software name and creators don't match OmniPage nor ABBY (should be OmniPage or FineReader)! software_names:{software_names}"
            print(msg)
            logger.warning(msg)
    elif abbyy_alias_prefix in mets_file:
        # ABBYY-ALIAS case scenario
        page_files = [f for f in ocr_files if abbyy_alias_prefix in f and f != mets_file]

        # randomly open a page to differentiate between the two possible formats
        with open(os.path.join(ocr_issue_dir, page_files[0]), "r", encoding="utf-8") as f:
            raw_xml_page = f.read()

        alias_page_doc = BeautifulSoup(raw_xml_page, "xml")
        software_names = [
            e.get_text().lower() for e in alias_page_doc.find_all("softwareName")
        ]
        software_creators = [
            e.get_text().lower() for e in alias_page_doc.find_all("softwareCreator")
        ]
        if (
            "finereader" in software_names
            or alias_page_doc.find_all("UKP")
            or any("abbyy" in s for s in software_creators)
        ):
            ocr_formats["ABBYY-ALIAS"] = page_files + [mets_file]
        else:
            msg = f"{ocr_issue_dir} - The filenames indicate the format should be ABBYY-ALIAS, but the software name doens't match (should be FineReader)! software_names:{software_names}, software_creators={software_creators}"
            print(msg)
            logger.warning(msg)

    else:
        msg = f"{ocr_issue_dir} - The filenames structures don't match anything expected for mets files! mets_file:{mets_file}, ocr_files:{ocr_files}"
        print(msg)
        logger.warning(msg)

    return ocr_formats


def id_ocr_format(ocr_issue_dir, alias):
    ocr_formats = {}
    ocr_files = os.listdir(ocr_issue_dir)

    if RENAMING_INFO_FILENAME in ocr_files:
        ocr_files.remove(RENAMING_INFO_FILENAME)

    mets_files = [f for f in ocr_files if f.endswith("mets.xml")]

    # identify all possible OCR formats for which we have a mets file
    for mets_file in mets_files:
        ocr_formats = id_mets_formats(alias, ocr_files, mets_file, ocr_issue_dir, ocr_formats)

    # if there is no mets file, it's probably the last "BL" format, with specific article XMLs
    # there can be multiple OCR formats per title so still try
    ocr_formats = id_bl_alias_format(alias, ocr_files, ocr_issue_dir, ocr_formats)

    # identify any remaining files - they go under "unknown"
    all_ided_files = []
    for ocr_type, identified_files in ocr_formats.items():
        all_ided_files.extend(identified_files)

    if len(set(all_ided_files)) != len(all_ided_files):
        # if there are duplicated in the identified files, there is a problem
        msg = f"{ocr_issue_dir} - Some files were identified twice for different formats! {ocr_formats}"
        print(msg)
        logger.warning(msg)
    if set(all_ided_files) != set(ocr_files):
        # if all files were not identified, set the remaining to an "unknown" format
        ocr_formats["UNKNOWN"] = [f for f in ocr_files if f not in all_ided_files]
        msg = f"{ocr_issue_dir} - Warning, not all files were identified for an OCR format: {ocr_formats['UNKNOWN']}."
        print(msg)
        logger.warning(msg)

    return ocr_issue_dir, ocr_formats


def main(
    log_file: str,
    chunk_size: int = 93,
    chunk_idx: int = 0,
    verbose: bool = False,
) -> None:
    """Main function to process and copy BL original data into a impresso structured directory.

    This function reads a CSV file containing NLP-to-alias mappings, processes a chunk
    of NLPs by copying files from the source to the destination directory, and logs any
    issues encountered. It also tracks problem directories and failed file copies.

    By default, performs the copy of OCR files, but can be paramtrized to copy images.

    Args:
        log_file (str): Path to the log file.
        source_base_dir (str, optional): Root directory of the source NLP files.
            Defaults to "/mnt/project_impresso/original/BL_old".
        dest_base_dir (str, optional): Root directory where processed files will be copied.
            Defaults to "/mnt/impresso_ocr_BL".
        sample_data_dir (str, optional): Directory containing metadata files.
            Defaults to "/home/piconti/impresso-text-acquisition/text_preparation/data/sample_data/BL".
        title_alias_mapping_file (str, optional): CSV file mapping aliases to NLPs.
            Defaults to "BL_title_alias_mapping.csv".
        file_type_ext (str, optional): File extension to be processed (e.g., ".xml").
            Defaults to ".xml".
        chunk_size (int, optional): Number of NLP directories to process in each chunk.
            Defaults to 100.
        chunk_idx (int, optional): Index of the chunk to process. Defaults to 0.
        verbose (bool, optional): If True, sets logging level to DEBUG; otherwise, INFO.
            Defaults to False.

    Raises:
        AssertionError: If the provided file extension is not in POSSIBLE_EXTENTIONS.
        Exception: If an error occurs while processing an NLP directory.
    """

    init_logger(logger, logging.INFO if not verbose else logging.DEBUG, log_file)

    # read the alias-NLP mapping
    with open(ALIAS_TO_NLP_PATH, "r", encoding="utf-8") as fin:
        alias_to_nlp = json.load(fin)

    if os.path.exists(OCR_FORMATS_FILEPATH):
        with open(OCR_FORMATS_FILEPATH, "r", encoding="utf-8") as fin:
            all_ocr_formats = json.load(fin)
    else:
        all_ocr_formats = {}

    # create the dict tracking the problems encountered
    if os.path.exists(REPROCESS_FILEPATH):
        with open(REPROCESS_FILEPATH, "r", encoding="utf-8") as fin:
            all_errors_or_to_reprocess = json.load(fin)
    else:
        all_errors_or_to_reprocess = {}
    # all_failed_copies = {}

    # list all NLPs and identify the ones that will be processed now
    all_bl_titles = sorted(os.listdir(BL_OCR_BASE_PATH))

    alias_chunk = list(chunk(all_bl_titles, chunk_size))[chunk_idx]

    msg = (
        f"Separated the list of NLPs into {len(all_bl_titles)//chunk_size+1} chunks of {chunk_size} media titles, "
        f" will process chunk n°{chunk_idx} now: \n{alias_chunk}"
    )
    print(msg)
    logger.info(msg)

    for alias_idx, bl_alias in tqdm(enumerate(alias_chunk, start=1)):

        if bl_alias in ALIAS_NLPS_TO_SKIP:
            msg = f"Skipping alias {bl_alias}"
            print(msg)
            logger.info(msg)
            continue

        # initialize the dict of issues to reprocess for this alias
        to_reprocess_for_alias = {}
        edition_errors_for_alias = {}
        ocr_formats_for_alias = (
            {} if bl_alias not in all_ocr_formats else all_ocr_formats[bl_alias]
        )

        msg = (
            f"{'-'*10} Processing alias {bl_alias} - {alias_idx}/{len(all_bl_titles)} {'-'*10}"
        )
        print(msg)
        logger.info(msg)

        alias_root_dir = os.path.join(BL_OCR_BASE_PATH, bl_alias)

        for nlp in os.listdir(alias_root_dir):

            try:

                nlp_root_dir = os.path.join(BL_OCR_BASE_PATH, bl_alias, nlp)
                years_for_nlp = os.listdir(nlp_root_dir)

                for y_id, year in enumerate(years_for_nlp, start=1):

                    msg = f"-->  {bl_alias} - Starting year {year} ({y_id}/{len(years_for_nlp)}) for NLP {nlp}"
                    print(msg)
                    logger.info(msg)

                    # initialize the ocr formats dict for this year if it's not there yet
                    if year not in ocr_formats_for_alias:
                        ocr_formats_for_alias[year] = {}

                    first_day_of_year = True

                    year_root_dir = os.path.join(BL_OCR_BASE_PATH, bl_alias, nlp, year)

                    for root, dirs, files in os.walk(year_root_dir):

                        # identify when we are in the dirctory of an issue
                        if len(dirs) == 0 and len(files) != 0:

                            _, month, day = root.replace(year_root_dir, "").split("/")

                            # don't recompute the OCR format if it was already computed
                            if root in ocr_formats_for_alias[year]:
                                ocr_formats = ocr_formats_for_alias[year][root]
                            else:
                                # add here identification of OCR format and filtering.
                                _, ocr_formats = id_ocr_format(root, bl_alias)
                                # don't store the full list of files for formats with mets files
                                proc_formats = {
                                    form: (
                                        [f for f in files if "mets" in f]
                                        if form not in ["BL-ALIAS", "UNKNOWN"]
                                        else files
                                    )
                                    for form, files in ocr_formats.items()
                                }
                                ocr_formats_for_alias[year][root] = proc_formats

                            # only rename images for issues with an OCR format that we're ready to process
                            if any(
                                ocr_form in VALID_OCR_FORMATS
                                for ocr_form in ocr_formats.keys()
                            ):
                                if first_day_of_year:
                                    msg = f"{root} - valid ocr format, will rename the image files: ocr_formats={ocr_formats}"
                                    print(msg)
                                    logger.info(msg)
                                    first_day_of_year = False

                                img_day_dir = os.path.join(
                                    BL_IMG_BASE_PATH, bl_alias, year, month, day
                                )

                                # try to identify the correct edition
                                edition, to_log = identify_edition(
                                    img_day_dir, nlp, alias_to_nlp[bl_alias]
                                )

                                if edition:
                                    # edition is not None, we can go ahead with it (and log it!)
                                    img_input_dir = os.path.join(img_day_dir, edition)
                                    issue_id = "-".join([bl_alias, year, month, day, edition])
                                    info_dict, to_reprocess = rename_jp2_files_for_issue(
                                        img_input_dir, issue_id, nlp, root
                                    )

                                    if to_reprocess:
                                        if year in to_reprocess_for_alias:
                                            to_reprocess_for_alias[year].update(to_reprocess)
                                        else:
                                            to_reprocess_for_alias[year] = to_reprocess

                                    info_dict["ocr_formats"] = ocr_formats
                                    # write the info dict to disk also with the OCR, but it needs to be in the write-allowed dir
                                    with open(
                                        os.path.join(root, RENAMING_INFO_FILENAME).replace(
                                            BL_OCR_BASE_PATH, BL_OCR_W_BASE_PATH
                                        ),
                                        "w",
                                        encoding="utf-8",
                                    ) as fout:
                                        json.dump(info_dict, fout)

                                elif to_log:
                                    # log
                                    if year in edition_errors_for_alias:
                                        edition_errors_for_alias[year].update(to_log)
                                    else:
                                        edition_errors_for_alias[year] = to_log
                            else:
                                # only log a wrong format for the first day of the year
                                if first_day_of_year:
                                    msg = f"{root} - Not an OCR format we will process for now - waiting"
                                    print(msg)
                                    logger.info(msg)
                                    first_day_of_year = False

                # If there was anything to reprocess for this alias, save it
                if edition_errors_for_alias:
                    if bl_alias in all_errors_or_to_reprocess:
                        all_errors_or_to_reprocess[bl_alias][
                            "edition_errors"
                        ] = edition_errors_for_alias
                    else:
                        all_errors_or_to_reprocess[bl_alias] = {
                            "edition_errors": edition_errors_for_alias
                        }
                if to_reprocess_for_alias:
                    if bl_alias in all_errors_or_to_reprocess:
                        all_errors_or_to_reprocess[bl_alias][
                            "to_reprocess"
                        ] = to_reprocess_for_alias
                    else:
                        all_errors_or_to_reprocess[bl_alias] = {
                            "to_reprocess": to_reprocess_for_alias
                        }

                # save the ocr formats for future reference
                if bl_alias in all_ocr_formats:
                    all_ocr_formats[bl_alias].update(ocr_formats_for_alias)
                else:
                    all_ocr_formats[bl_alias] = ocr_formats_for_alias
            except Exception as e:
                msg = f"{bl_alias}-{nlp}: There was an exception when processing this alias!! Exception: {e}, {traceback.format_exc()}"
                print(msg)
                logger.error(msg)

            msg = (
                f"✅ {bl_alias}-{nlp} ({alias_idx}/{len(alias_chunk)}) - Done with the processing of {nlp}. "
                f"There was {len(to_reprocess_for_alias)} renamings to reprocess and {len(edition_errors_for_alias)} edition errors for alias."
            )
            print(msg)
            logger.info(msg)

        # save the information tracked about the failures and renamings to reprocess
        with open(REPROCESS_FILEPATH, "w", encoding="utf-8") as f:
            json.dump(all_errors_or_to_reprocess, f)

        # save the OCR formats which were already identified
        with open(OCR_FORMATS_FILEPATH, "w", encoding="utf-8") as f:
            json.dump(all_ocr_formats, f, indent=2)

    msg = f"✅✅ Success! Done with all {len(alias_chunk)} in this chunk: {alias_chunk}!!"
    print(msg)
    logger.info(msg)


if __name__ == "__main__":
    fire.Fire(main)
