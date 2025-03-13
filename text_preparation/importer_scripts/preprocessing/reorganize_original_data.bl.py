"""Script copying BL's original OCR data into Impresso's internal filestructure, according to the devised Alias-to-NLP mapping.
"""
import json
import re
import pandas as pd
import os
import shutil
import numpy as np
from tqdm import tqdm
from datetime import datetime
import logging
import fire
from ast import literal_eval
from impresso_essentials.utils import init_logger, chunk

logger = logging.getLogger(__name__)

# this script can either copy OCR files (.xml) or images (.jp2)
POSSIBLE_EXTENTIONS = ['.xml', '.jp2']

def extract_date(root_path:str)-> tuple[bool, str, str, str]:
    # extract the year, month and day for a root path which has been format-checked

    # edge case for issue dir "/mnt/project_impresso/original/BL_old/0000071/1785/0618.backup"
    # "/mnt/project_impresso/original/BL_old/0002634/1820/0317.backup/0317/"
    if ".backup" in root_path:
        # remove the '.backup' and everything after to parse the date
        root_path = root_path.split('.backup')[0]
        msg = f"{root_path}: found an unexpected component to the path, removing all after '.backup'!"
        print(msg)
        logger.error(msg)
    
    try:
        path_tail = root_path.split('/')[6:]#[-2:]
    except Exception as e:
        msg = f"{root_path}: Missing elements! error: {e}"
        print(msg)
        logger.error(msg)
        return False, '','',''
    
    # path_tail should be in format: ['YYYY', 'MMDD']
    y, m, d = path_tail[0], path_tail[1][:2], path_tail[1][2:]
    
    try:
        # assert that this is a valid date
        datetime(year=int(y), month=int(m), day=int(d))
        return True, y, m, d
    except ValueError as e:
        msg = f"{root_path}: Invalid date! {y, m, d}, error: {e}"
        print(msg)
        logger.error(msg)
        return False, y, m, d
    

def check_if_to_be_copied(source_dir_files: str, dest_issue_dir: str, possible_date_formats, file_ext = '.xml') -> tuple[bool, list[str]]:
    # check if the copy needs to be done when within an issue dir.
    # should not be done if:
    # - it was already done (dest issue dir exists and has exactly the same xml/jp2 files)
    # - there are no files to copy in the source (error in the file structure for instance)

    assert file_ext in POSSIBLE_EXTENTIONS, f"The file extension provided was {file_ext}. It should be in {POSSIBLE_EXTENTIONS}."

    # list all the files to copy from the source dir: xml files which have the correct date
    src_files_to_copy = [f for f in source_dir_files for d in possible_date_formats if f.endswith(file_ext) and d in f]

    if len(src_files_to_copy) == 0:
        # if there are no files to copy at all, log it (might be an error)
        msg = f"{dest_issue_dir} - No files to copy in source dir: source_dir_files={source_dir_files}, src_files_to_copy={src_files_to_copy}!"
        print(msg)
        logger.warning(msg)
        return False, src_files_to_copy
    
    # check if dir exists and all xml or jp2 files from source are there
    if os.path.exists(dest_issue_dir):
        existing_dest_files = os.listdir(dest_issue_dir)
        if all(f in existing_dest_files for f in src_files_to_copy):
            # the copy wss already done correctly; to be copied = False
            return False, src_files_to_copy
        
    # if dest_issue_dir doesn't exist or not all xml or jp2 files from source dir are there, the copy needs to be redone
    return True, src_files_to_copy
    

def copy_files_for_NLP(nlp: str, alias: str, source_dir: str, dest_dir: str, file_ext: str, date_fmt_chars: list[str] = ['-', '', '_']) -> tuple[list[str], list[str]]:
    # given an NLP, copy all the files within it in the new desired structure
    problem_input_dirs = []
    failed_copies = []
    nlp_dest_dir_path = os.path.join(dest_dir, alias, nlp)
    # first create the subdir for the NLP, inside a director for the Alias, creating it if it does not exist yet
    os.makedirs(nlp_dest_dir_path, exist_ok=True)

    # then iterate on all the years, and for each one, recreate the structure (MM/DD) and copy the *.xml files
    prev_year = ''
    for root,dirs,files in os.walk(os.path.join(source_dir, nlp)):
        # identify the cases when we are in a issue's directory, and we are in the standard case scenario
        if len(files)!=0:
            if len(dirs) == 0:
                valid_date, y, m, d = extract_date(root)

                if y != prev_year:
                    msg = f"{alias}-{nlp} — Processing year {y}."
                    print(msg)
                    logger.info(msg)
                    prev_year = y
                # ensure the date identified is correct
                if valid_date:
                    # define the out_path where to copy the issue OCR data, to check if it was already processed
                    issue_out_dir = os.path.join(nlp_dest_dir_path, y, m, d)
                    # list the possible dates to find in the files to copy
                    date_formats = [c.join([y, m, d]) for c in date_fmt_chars]
                    # identify the list of files to copy and if there are files left to copy
                    copy_to_do, src_files_to_copy = check_if_to_be_copied(files, issue_out_dir, date_formats, file_ext)

                    if copy_to_do:
                        # ensure dest issue dir exists
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
                        msg = (
                            f"   - source dir (contents: {os.listdir(root)}\n"
                            f"   - dest dir contents: {os.listdir(issue_out_dir)}."
                        )
                        print(msg)
                        logger.debug(msg)
                else:
                    msg = (
                        f"{alias}-{nlp} — Invalid date!! {root}"
                    )
                    print(msg)
                    logger.warning(msg)
                    problem_input_dirs.append(root)
            else:
                msg = (
                    f"{alias}-{nlp} — Invalid directoy!! root:{root}, dirs:{dirs}, files={files}"
                )
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
    
    init_logger(logger, logging.INFO if not verbose else logging.DEBUG, log_file)

    assert file_type_ext in POSSIBLE_EXTENTIONS, f"The file extension provided was {file_type_ext}. It should be in {POSSIBLE_EXTENTIONS}."

    # read the csv containing the mapping from alias-NLP mapping
    nlp_alias_df = pd.read_csv(os.path.join(sample_data_dir, title_alias_mapping_file), index_col=0)
    alias_to_nlps = nlp_alias_df[['Alias', 'NLPs']].to_dict(orient='records')

    nlp_to_alias = {nlp: record['Alias'] for record in alias_to_nlps for nlp in literal_eval(record['NLPs'])}

    # create the dict tracking the problems encountered
    all_problem_input_dirs = {}
    all_failed_copies = {}
    problem_dir_json = os.path.join(sample_data_dir, f"problem_dirs_chunk_{chunk_idx}.json")
    failed_copies_json = os.path.join(sample_data_dir, f"failed_copied_chunk_{chunk_idx}.json")

    # list all NLPs and identify the ones that will be processed now
    all_dirs = sorted(os.listdir(source_base_dir))
    # ignore the non-NLP dirs: 'BLIP_20190920_01.zip', 'BLIP_20190929_04.zip', 'final_nas_manifest.out', 'test', 'test-dir1'
    all_nlps = [d for d in all_dirs if re.fullmatch(r"\d{7}", d)]
    nlps_chunk = list(chunk(all_nlps, chunk_size))[chunk_idx]

    msg = (
        f"Separated the list of NLPs into {len(all_nlps)//chunk_size+1} chunks of {chunk_size} NLPs, "
        f" will process chunk n°{chunk_idx} now: \n{nlps_chunk}"
    )
    print(msg)
    logger.info(msg)

    for nlp_idx, nlp in tqdm(enumerate(nlps_chunk)):

        alias = nlp_to_alias[nlp]
        msg = f"{10*'-'} Processing {alias} - NLP {nlp} ({nlp_idx+1}/{len(nlps_chunk)}) {10*'-'}"
        print(msg)
        logger.info(msg)

        try:
            problem_input_dirs, failed_copies = copy_files_for_NLP(nlp, alias, source_base_dir, dest_base_dir, file_type_ext)
            all_problem_input_dirs[nlp] = problem_input_dirs
            all_failed_copies[nlp] = failed_copies
        except Exception as e:
            msg = f"{alias}-{nlp}: There was an exception when processing this NLP!! Exception: {e}"
            print(msg)
            logger.error(msg)

        msg = f"✅ {alias}-{nlp} ({nlp_idx+1}/{len(nlps_chunk)}) - Done with the copy. There was {len(failed_copies)} failed copies and problem_input_dirs:{problem_input_dirs}."
        print(msg)
        logger.info(msg)

        # save the information tracked about the failures and problematic dirs
        if len(problem_input_dirs) != 0:
            # only update the problem dirs json if there was a problem dir
            with open(problem_dir_json, "w", encoding='utf-8') as f:
                json.dump(all_problem_input_dirs, f)

        if len(failed_copies) != 0:
            # only update the problem dirs json if there was a problem dir
            with open(failed_copies_json, "w", encoding='utf-8') as f:
                json.dump(all_failed_copies, f)

    msg = f"✅✅ Success! Done with all {len(nlps_chunk)} in this chunk: {nlps_chunk}!!"
    print(msg)
    logger.info(msg)


if __name__ == "__main__":
    fire.Fire(main)