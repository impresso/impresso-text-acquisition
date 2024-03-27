"""Command-line script to perform the patch #1 on the UZH canonical data (FedGaz, NZZ).

Usage:
    canonical_patch_7_find_issues.py [--img-base-path=<ibp> --og-data-path=<odp> --local-path=<lp> --log-file=<lf>]
    
Options:

--img-base-path=<ibp>  S3 input bucket.
--og-data-path=<odp>  S3 output bucket.
--local-path=<lp>  Path to the local impresso-text-acquisition git repository.
--log-file=<lf>  Path to log file.
"""

import os
import json
import logging
import shutil
from typing import Any, Callable
from zipfile import ZipFile, BadZipFile
from docopt import docopt

from impresso_commons.utils import s3
from text_importer.utils import init_logger

IMPRESSO_STORAGEOPT = s3.get_storage_options()
UZH_TITLES = ["FedGazDe", "FedGazFr", "NZZ"]
IMPRESSO_IIIF_BASE_URI = "https://impresso-project.ch/api/proxy/iiif/"
PROP_NAME = "iiif_img_base_uri"

logger = logging.getLogger()


def empty_folder(dir_path: str) -> None:
    """Empty a directoy given its path if it exists.

    Args:
        dir_path (str): Path to the directory to empty.
    """
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        logger.info("Emptied directory at %s", dir_path)
    os.mkdir(dir_path)


def write_error(
    thing_id: str, origin_function: str, error: Exception, failed_log: str
) -> None:
    """Write the given error of a failed import to the `failed_log` file.

    Adapted from `impresso-text-acquisition/text_importer/importers/core.py` to allow
    using a issue or page id, and provide the function in which the error took place.

    Args:
        thing_id (str): Canonical ID of the object/file for which the error occurred.
        origin_function (str): Function in which the exception occured.
        error (Exception): Error that occurred and should be logged.
        failed_log (str): Path to log file for failed imports.
    """
    note = f"Error in {origin_function} for {thing_id}: {error}"
    logger.exception(note)
    with open(failed_log, "a+", encoding="utf-8") as f:
        f.write(note + "\n")


def extract_zip_contents(zip_path: str) -> tuple[list[str], list[str]]:

    zip_contents = ZipFile(zip_path).namelist()

    pg_res_files = [f for f in zip_contents if "Img" in f and "Pg" in f]
    pg_res = [f for f in pg_res_files if "_" in f]
    # for f in pg_res_files:
    #    if "_" in f:
    #        pg = int(f.split("/")[0])
    #        res = int(os.path.basename(f).split(".")[0].split("_")[1])
    #        if pg in pg_res:
    #            pg_res[pg].append(res)
    #        else:
    #            pg_res[pg] = [res]
    return pg_res_files, pg_res


def load_json(f_path: str) -> dict:
    with open(f_path, mode="r", encoding="utf-8") as f_in:
        file = json.load(f_in)
    return file


def fetch_needed_info_for_title(title, og_data_path, img_data_path, out_path):

    # resume a listing in the middle
    if os.path.exists(out_path):
        title_info = load_json(out_path)
        logger.info(
            "Continuing to fetch for %s, restarting from %a issues",
            title,
            len(title_info),
        )
    else:
        title_info = {}

    msg = f"- Fetching info for: {title}"
    print(msg)
    logger.info(msg)
    missing_img_info_files = []
    mltp_img_info_files = []

    for dir_path, sub_dirs, files in os.walk(os.path.join(img_data_path, title)):
        # only consider the cases where we are in an issue directory
        if len(sub_dirs) == 0:
            issue_sub_path = dir_path.replace(img_data_path, "")
            issue_id = issue_sub_path.replace("/", "-")
            if title != "LCE" or issue_id not in title_info:

                # add the image info
                img_info_file = [f for f in files if f.endswith("image-info.json")]
                if len(img_info_file) == 1:
                    img_info_file_path = os.path.join(dir_path, img_info_file[0])
                    title_info[issue_id] = {
                        "img": {
                            "file_present": True,
                            "img_info_file": img_info_file_path,
                        }
                    }

                    img_info = load_json(img_info_file_path)
                    title_info[issue_id]["img"]["info_f_contents"] = {}
                    for p, p_info in enumerate(img_info):
                        title_info[issue_id]["img"]["info_f_contents"][p] = {
                            "source_used": p_info["s"],
                            "strat": p_info["strat"],
                            "s_dim": p_info["s_dim"],
                            "d_dim": p_info["d_dim"],
                        }

                elif len(img_info_file) == 0:
                    print(
                        f"Warining: Missing image-info file for {issue_id}: {dir_path}"
                    )
                    title_info[issue_id] = {
                        "img": {"file_present": False, "img_info_file": dir_path}
                    }
                    missing_img_info_files.append(dir_path)
                else:
                    print(
                        f"Warning: Mone than 1 image-info file for {issue_id}: {dir_path}"
                    )
                    mltp_img_info_files.append(dir_path)

                # fetch list of formats
                # create the path in the original data: there is no edition, so the final '/a' should be removed
                og_data_dir_path = dir_path.replace(img_data_path, og_data_path)[:-2]
                # if "Document.zip" in os.listdir(og_data_dir_path):
                doc_zip_path = os.path.join(og_data_dir_path, "Document.zip")
                if os.path.exists(doc_zip_path):
                    try:
                        title_info[issue_id]["original"] = {
                            "zip_doc_path": doc_zip_path
                        }
                        pg_res_files, pg_res = extract_zip_contents(
                            title_info[issue_id]["original"]["zip_doc_path"]
                        )
                        title_info[issue_id]["original"][
                            "zip_img_contents"
                        ] = pg_res_files
                        if len(pg_res) != 0:
                            title_info[issue_id]["original"]["resolutions"] = pg_res
                    except BadZipFile as e:
                        msg = f"Error: Problem with zip {doc_zip_path}: {e}!"
                        logger.error(msg)
                        print(msg)
                else:
                    msg = f"Warning: No 'Document.zip' found in {og_data_dir_path}!"
                    logger.info(msg)
                    print(msg)

                if len(title_info) % 50 == 0:
                    logger.info(
                        "Currently on issue %s, done %s issues.",
                        issue_id,
                        len(title_info),
                    )
                    if len(title_info) % 500 == 0:
                        logger.info("Done 500 issues, saving file temporarily.")
                        with open(out_path, "w", encoding="utf-8") as f_out:
                            json.dump(title_info, f_out, ensure_ascii=False, indent=4)

    return title_info, missing_img_info_files, mltp_img_info_files


def main():
    arguments = docopt(__doc__)
    images_base_path = (
        arguments["--img-base-path"]
        if arguments["--img-base-path"]
        else "/mnt/project_impresso/images/"
    )
    og_data_base_path = (
        arguments["--og-data-path"]
        if arguments["--og-data-path"]
        else "/mnt/project_impresso/original/RERO/"
    )
    local_base_path = (
        arguments["--local-path"]
        if arguments["--local-path"]
        else "/scratch/piconti/impresso/patch_7"
    )
    log_file = (
        arguments["--log-file"]
        if arguments["--log-file"]
        else f"{local_base_path}/find_issues.log"
    )

    init_logger(logger, logging.INFO, log_file)
    logger.info("Arguments: \n %s", arguments)

    all_titles_info = {}
    _, rero_journal_dirs, _ = next(os.walk(og_data_base_path))

    # set some titles at the front of the line as priority
    rero_titles = ["LCG", "DLE", "LNF", "LBP", "LSE", "EXP"]
    rero_titles.extend(rero_journal_dirs)

    logger.info("Will process titles: %s", rero_titles)

    for idx, journal in enumerate(rero_titles):
        logger.info("Title %s/%s:", idx, len(rero_journal_dirs))

        img_info_paths_file = f"{local_base_path}/{journal}_img_res_info.json"

        if not os.path.exists(img_info_paths_file) or journal == "LCE":
            title_info, missing_info_files, mltp_if = fetch_needed_info_for_title(
                journal, og_data_base_path, images_base_path, img_info_paths_file
            )

            all_titles_info[journal] = title_info

            with open(img_info_paths_file, "w", encoding="utf-8") as f_out:
                json.dump(title_info, f_out, ensure_ascii=False, indent=4)

            logger.info(
                "%s issues are missing their image-info.json file: ",
                len(missing_info_files),
            )
            if len(missing_info_files) > 0:
                with open(
                    f"{local_base_path}/{journal}_missing_info_issues.json",
                    "w",
                    encoding="utf-8",
                ) as f_out:
                    json.dump(title_info, f_out, ensure_ascii=False, indent=4)
            logger.info(missing_info_files)
            logger.info(
                "%s issues are have more than 1 image-info.json files: ",
                len(mltp_if),
            )
            logger.info(mltp_if)
        else:
            logger.info("Skipping %s as it's already been processed", journal)


if __name__ == "__main__":
    main()
