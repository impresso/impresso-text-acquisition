"""Script copying BL's original OCR data and Images into Impresso's internal filestructure, according to the devised Alias-to-NLP mapping.

Example usage:
- To copy OCR files:
$ python reorganize_original_data.bl.py --log_file="ocr_logfile_2.log" --chunk_idx=2
- To copy image files:
$ python reorganize_original_data.bl.py --log_file="img_logfile_2.log" --file_type_ext=".jp2" --chunk_idx=2 --dest_base_dir="/mnt/impresso_images_BL"
"""

import os
import json
import re
import logging
import shutil
from ast import literal_eval
from datetime import datetime
import fire
import pandas as pd
from tqdm import tqdm
from impresso_essentials.utils import init_logger, chunk

logger = logging.getLogger(__name__)

# this script can either copy OCR files (.xml) or images (.jp2)
POSSIBLE_EXTENTIONS = [".xml", ".jp2"]


def extract_date(root_path: str) -> tuple[bool, str, str, str]:
    """Extracts the year, month, and day from a given root path.

    This function assumes the root path follows a specific format and attempts to
    extract date information (YYYY, MM, DD). It also handles cases where the path
    contains unexpected `.backup` components and logs relevant errors.

    Args:
        root_path (str): The file path from which to extract the date.

    Returns:
        tuple[bool, str, str, str]: A tuple containing:
            - bool: True if the date is valid, False otherwise.
            - str: Extracted year (YYYY) as a string.
            - str: Extracted month (MM) as a string.
            - str: Extracted day (DD) as a string.

    Raises:
        ValueError: If the extracted date is not a valid calendar date.

    Example:
        >>> extract_date("/mnt/project_impresso/original/BL_old/0002634/1820/0317/")
        (True, "1820", "03", "17")
    """
    # Edge case: Handle unexpected `.backup` component in the path
    if ".backup" in root_path:
        # remove the '.backup' and everything after to parse the date
        root_path = root_path.split(".backup")[0]
        msg = f"{root_path}: found an unexpected component to the path, removing all after '.backup'!"
        print(msg)
        logger.error(msg)

    try:
        path_tail = root_path.split("/")[6:]  # [-2:]
    except Exception as e:
        msg = f"{root_path}: Missing elements! error: {e}"
        print(msg)
        logger.error(msg)
        return False, "", "", ""

    # path_tail should be in format: ['YYYY', 'MMDD']
    y, m, d = path_tail[0], path_tail[1][:2], path_tail[1][2:]

    try:
        # Validate extracted date
        datetime(year=int(y), month=int(m), day=int(d))
        return True, y, m, d
    except ValueError as e:
        msg = f"{root_path}: Invalid date! {y, m, d}, error: {e}"
        print(msg)
        logger.error(msg)
        return False, y, m, d


def check_if_to_be_copied(
    source_dir_files: str, dest_issue_dir: str, possible_date_formats, file_ext=".xml"
) -> tuple[bool, list[str]]:
    """Determines whether files need to be copied to the destination issue directory.

    This function checks if a copy operation should be performed by verifying:
    - Whether the destination issue directory already contains the required files.
    - Whether the source dir contains files matching the expected date and file extension.

    Args:
        source_dir_files (str): A list of file names available in the source directory.
        dest_issue_dir (str): The destination issue directory where files should be copied.
        possible_date_formats (list[str]): A list of possible date formats that should be
        present in the file names.
        file_ext (str, optional): The file extension to check for (default is ".xml").

    Returns:
        tuple[bool, list[str]]: A tuple containing:
            - bool: True if the copy operation needs to be performed, False otherwise.
            - list[str]: A list of source files that match the criteria for copying.

    Raises:
        AssertionError: If the provided file extension is not in `POSSIBLE_EXTENTIONS`.

    Example:
        >>> check_if_to_be_copied(["18200317.xml", "18200317.jp2"], "/mnt/data/issues/18200317", ["18200317"])
        (True, ["18200317.xml"])
    """
    assert (
        file_ext in POSSIBLE_EXTENTIONS
    ), f"The file extension provided was {file_ext}. It should be in {POSSIBLE_EXTENTIONS}."

    # list all files to copy from the source dir: which have the correct date and extension
    src_files_to_copy = [
        f
        for f in source_dir_files
        for d in possible_date_formats
        if f.endswith(file_ext) and d in f
    ]

    if len(src_files_to_copy) == 0:
        # No matching files found in the source directory; log this as a potential issue
        msg = (
            f"{dest_issue_dir} - No files to copy in source dir: source_dir_files="
            f"{source_dir_files}, src_files_to_copy={src_files_to_copy}!"
        )
        print(msg)
        logger.warning(msg)
        return False, src_files_to_copy

    # check if destination directory exists and contains all required files
    if os.path.exists(dest_issue_dir):
        existing_dest_files = os.listdir(dest_issue_dir)
        if all(f in existing_dest_files for f in src_files_to_copy):
            # The copy operation was already completed successfully
            return False, src_files_to_copy

    # If destination directory is missing or doesn't contain all required files, copy is needed
    return True, src_files_to_copy


def copy_files_for_NLP(
    nlp: str,
    alias: str,
    source_dir: str,
    dest_dir: str,
    file_ext: str,
    date_fmt_chars: list[str] = ["-", "", "_"],
) -> tuple[list[str], list[str]]:
    """Copies files for a given NLP (BL title ID) into a structured directory.

    This function processes all files in the specified source directory, extracts date
    information from issue directories, and organizes them into a structured destination
    directory following the format `dest_dir/alias/nlp/YYYY/MM/DD`.

    Args:
        nlp (str): The name of the NLP process.
        alias (str): The alias under which the NLP process is categorized.
        source_dir (str): The root directory containing the source files.
        dest_dir (str): The destination root directory where files should be copied.
        file_ext (str): The file extension to be copied (e.g., ".xml").
        date_fmt_chars (list[str], optional): A list of characters to format date strings
            when matching files (default is ["-", "", "_"]).

    Returns:
        tuple[list[str], list[str]]: A tuple containing:
            - list[str]: A list of issue directories with invalid structures or date errors.
            - list[str]: A list of files that failed to copy.

    Raises:
        IOError: If file copying fails.

    Example:
        >>> copy_files_for_NLP("NLP1", "aliasX", "/mnt/source", "/mnt/dest", ".xml")
        ([], [])  # No issues or failed copies.
    """
    problem_input_dirs = []
    failed_copies = []
    nlp_dest_dir_path = (
        os.path.join(dest_dir, alias, nlp)
        if file_ext == ".xml"
        else os.path.join(dest_dir, alias)
    )

    # Create the NLP subdirectory inside the alias directory if it does not exist
    os.makedirs(nlp_dest_dir_path, exist_ok=True)

    prev_year = ""
    for root, dirs, files in os.walk(os.path.join(source_dir, nlp)):
        # Process directories that contain files (i.e., issue directories)
        if len(files) != 0:
            if len(dirs) == 0:
                valid_date, y, m, d = extract_date(root)

                if y != prev_year:
                    msg = f"{alias}-{nlp} — Processing year {y}."
                    print(msg)
                    logger.info(msg)
                    prev_year = y

                if valid_date:
                    # Define output directory for the issue
                    issue_out_dir = (
                        os.path.join(nlp_dest_dir_path, y, m, d)
                        if file_ext == ".xml"
                        else os.path.join(nlp_dest_dir_path, y, m, d, "a")
                    )
                    # Generate possible date formats for file matching
                    date_formats = [c.join([y, m, d]) for c in date_fmt_chars]
                    # Determine files to copy and whether a copy is needed
                    copy_to_do, src_files_to_copy = check_if_to_be_copied(
                        files, issue_out_dir, date_formats, file_ext
                    )

                    # Handle special cases for specific NLPs
                    if file_ext == ".jp2" and nlp == "0002425" and copy_to_do:
                        # For "0002424" and "0002425", we have a special case with sometimes two editions
                        issue_out_dir_ed_b = os.path.join(nlp_dest_dir_path, y, m, d, "b")
                        copy_to_do_2, _ = check_if_to_be_copied(
                            files, issue_out_dir_ed_b, date_formats, file_ext
                        )
                        msg = f"{alias}-{nlp}-{date_formats[0]} — Special case of {nlp}: copy_to_do={copy_to_do}, copy_to_do_2={copy_to_do_2}:\n"
                        print(msg)
                        logger.info(msg)
                        copy_to_do = copy_to_do and copy_to_do_2

                    if copy_to_do:
                        # Ensure dest issue dir exists
                        os.makedirs(issue_out_dir, exist_ok=True)
                        for f in src_files_to_copy:
                            try:
                                shutil.copy(os.path.join(root, f), issue_out_dir)
                            except IOError as e:
                                msg = f"{alias}-{nlp}-{date_formats[0]} — Copy of {os.path.join(root, f)} failed due to execption {e}, To copy again!."
                                print(msg)
                                logger.error(msg)
                                failed_copies.append(os.path.join(root, f))
                    else:
                        msg = f"{alias}-{nlp}-{date_formats[0]} — Skipping: no files to copy issue contents of {root} already exist in {issue_out_dir}:\n"
                        print(msg)
                        logger.info(msg)
                        if os.path.exists(issue_out_dir):
                            out_list = os.listdir(issue_out_dir)
                        else:
                            out_list = os.listdir(os.path.split(issue_out_dir)[0])
                        msg = (
                            f"   - source dir (contents: {os.listdir(root)}\n"
                            f"   - dest dir contents: {out_list}."
                        )
                        print(msg)
                        logger.debug(msg)

                else:
                    msg = f"{alias}-{nlp} — Invalid date!! {root}"
                    print(msg)
                    logger.warning(msg)
                    problem_input_dirs.append(root)
            else:
                msg = f"{alias}-{nlp} — Invalid directoy!! root:{root}, dirs:{dirs}, files={files}"
                print(msg)
                logger.warning(msg)
                problem_input_dirs.append(root)

    return problem_input_dirs, failed_copies


def main(
    log_file: str,
    source_base_dir: str = "/mnt/project_impresso/original/BL_old",
    dest_base_dir: str = "/mnt/impresso_ocr_BL",
    sample_data_dir: str = "/home/piconti/impresso-text-acquisition/text_preparation/data/sample_data/BL",
    title_alias_mapping_file: str = "BL_title_alias_mapping.csv",
    file_type_ext: str = ".xml",
    chunk_size: int = 100,
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

    assert (
        file_type_ext in POSSIBLE_EXTENTIONS
    ), f"The file extension provided was {file_type_ext}. It should be in {POSSIBLE_EXTENTIONS}."

    # read the csv containing the mapping from alias-NLP mapping
    nlp_alias_df = pd.read_csv(
        os.path.join(sample_data_dir, title_alias_mapping_file), index_col=0
    )
    alias_to_nlps = nlp_alias_df[["Alias", "NLPs"]].to_dict(orient="records")

    nlp_to_alias = {
        nlp: record["Alias"]
        for record in alias_to_nlps
        for nlp in literal_eval(record["NLPs"])
    }

    # create the dict tracking the problems encountered
    all_problem_input_dirs = {}
    all_failed_copies = {}
    problem_dir_json = os.path.join(sample_data_dir, f"problem_dirs_chunk_{chunk_idx}.json")
    failed_copies_json = os.path.join(sample_data_dir, f"failed_copied_chunk_{chunk_idx}.json")

    # list all NLPs and identify the ones that will be processed now
    all_dirs = sorted(os.listdir(source_base_dir))
    # ALL NLP dirs are 7 digits - ignore the non-NLP dirs:
    # 'BLIP_20190920_01.zip', 'BLIP_20190929_04.zip', 'final_nas_manifest.out', 'test', 'test-dir1'
    all_nlps = [d for d in all_dirs if re.fullmatch(r"\d{7}", d)]
    nlps_chunk = list(chunk(all_nlps, chunk_size))[chunk_idx]

    msg = (
        f"Separated the list of NLPs into {len(all_nlps)//chunk_size+1} chunks of {chunk_size} NLPs, "
        f" will process chunk n°{chunk_idx} now: \n{nlps_chunk}"
    )
    print(msg)
    logger.info(msg)

    for nlp_idx, nlp in tqdm(enumerate(nlps_chunk)):

        if nlp in ["0003056", "0003057", "0004683", "0000071", "0000191"]:
            print(f"Skipping {nlp} as it is still under work.")
            continue

        alias = nlp_to_alias[nlp]
        msg = (
            f"{10*'-'} Processing {alias} - NLP {nlp} ({nlp_idx+1}/{len(nlps_chunk)}) {10*'-'}"
        )
        print(msg)
        logger.info(msg)

        try:
            problem_input_dirs, failed_copies = copy_files_for_NLP(
                nlp, alias, source_base_dir, dest_base_dir, file_type_ext
            )
            all_problem_input_dirs[nlp] = problem_input_dirs
            all_failed_copies[nlp] = failed_copies
        except Exception as e:
            msg = f"{alias}-{nlp}: There was an exception when processing this NLP!! Exception: {e}"
            print(msg)
            logger.error(msg)

        msg = (
            f"✅ {alias}-{nlp} ({nlp_idx+1}/{len(nlps_chunk)}) - Done with the copy. "
            f"There was {len(failed_copies)} failed copies and problem_input_dirs:{problem_input_dirs}."
        )
        print(msg)
        logger.info(msg)

        # save the information tracked about the failures and problematic dirs
        if len(problem_input_dirs) != 0:
            # only update the problem dirs json if there was a problem dir
            with open(problem_dir_json, "w", encoding="utf-8") as f:
                json.dump(all_problem_input_dirs, f)

        if len(failed_copies) != 0:
            # only update the problem dirs json if there was a problem dir
            with open(failed_copies_json, "w", encoding="utf-8") as f:
                json.dump(all_failed_copies, f)

    msg = f"✅✅ Success! Done with all {len(nlps_chunk)} in this chunk: {nlps_chunk}!!"
    print(msg)
    logger.info(msg)


if __name__ == "__main__":
    fire.Fire(main)
