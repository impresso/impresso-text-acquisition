"""Command-line script to perform the patch #7 on the RERO 1 (Olive) canonical data.

Usage:
    canonical_patch_7_rero_olive.py [--local-path=<lp> --log-file=<lf> --input-bucket=<ib> --output-bucket=<ob> --canonical-repo-path=<crp> --error-log=<el>]
    
Options:

--local-path=<lp>  Path to the local impresso-text-acquisition git repository.
--log-file=<lf>  Path to log file.
--input-bucket=<ib>  S3 input bucket.
--output-bucket=<ob>  S3 output bucket.
--canonical-repo-path=<crp>  Path to the local impresso-text-acquisition git repository.
--error-log=<el>  Path to error log file.
"""

import os
import json
import logging

from typing import Any
from docopt import docopt
from impresso_commons.utils import s3
from impresso_commons.path.path_s3 import fetch_files
from impresso_commons.versioning.compute_manifest import create_manifest
from text_importer.utils import init_logger, empty_folder
from text_importer.scripts.patching.canonical_patch_1_uzh import (
    title_year_pair_to_issues,
    write_upload_issues,
    to_issue_id_pages_dict,
    nzz_write_upload_pages,
)

IMPRESSO_STORAGEOPT = s3.get_storage_options()

logger = logging.getLogger()


def scale_coords(
    coords: list[int], curr_res: str | int, des_res: str | int
) -> list[int]:
    """Rescale the given coordinates based on the current and destination resolutions.

    Args:
        coords (list[int]): Current coordinates to rescale.
        curr_res (str | int): Current resolution.
        des_res (str | int): Destination resolution.

    Returns:
        list[int]: Rescaled coordinates.
    """
    return [int(c * int(des_res) / int(curr_res)) for c in coords]


def convert_issue_coords(
    issue: dict[str, Any], res: dict[str, int | list]
) -> tuple[dict[str, Any], bool]:
    """Convert the coordinates present in 1 issue if they are present

    Typically, coordinates are present in an issue if it contains content-items of type
    "image", for which the coordiantes are stored.

    Args:
        issue (dict[str, Any]): JSON Canonical reprensetation of an issue.
        res (dict[str, int  |  list]): Resolutions according to which to rescale.

    Returns:
        tuple[dict[str, Any], bool]: Issue after operation and whether or not some
            coordinates were indeed rescaled.
    """
    scaled = False
    for i in issue["i"]:
        if "c" in i["m"]:
            i["m"]["c"] = scale_coords(i["m"]["c"], res["curr_res"], res["dest_res"])
            scaled = True
        elif "c" in i:
            i["c"] = scale_coords(i["c"], res["curr_res"], res["dest_res"])
            scaled = True
        elif "iiif_link" in i["m"] or "iiif_link" in i:
            iiif = i["m"]["iiif_link"] if "iiif_link" in i["m"] else i["iiif_link"]
            logger.warning(
                "%s: No coordinates but a IIIF link for item %s: %s",
                issue["id"],
                i["m"]["id"],
                iiif,
            )
    # return the issue as-is once it's been scaled
    return issue, scaled


def convert_page_coords(
    page: dict[str, Any], res: dict[str, int | list]
) -> tuple[dict[str, Any], bool]:
    """Rescale the coordinates within a canonical page representation.

    Args:
        page (dict[str, Any]): JSON Canonical reprensetation of a Page.
        res (dict[str, int  |  list]): Resolutions according to which to rescale.

    Returns:
        tuple[dict[str, Any], bool]: Page with rescaled coordinates and sanity check.
    """
    # Convert the coordinates present in 1 page
    scaled = 0
    # count the expected number of coordinates to rescale on page
    coords_count = len(page["r"])
    for region in page["r"]:
        region["c"] = scale_coords(region["c"], res["curr_res"], res["dest_res"])
        scaled += 1
        for para in region["p"]:
            coords_count += len(para["l"])
            for line in para["l"]:
                line["c"] = scale_coords(line["c"], res["curr_res"], res["dest_res"])
                scaled += 1
                coords_count += len(line["t"])
                for token in line["t"]:
                    token["c"] = scale_coords(
                        token["c"], res["curr_res"], res["dest_res"]
                    )
                    scaled += 1
    return page, scaled == coords_count


def find_convert_coords(
    elem: dict[str, Any],
    title: str,
    to_patch: dict[str, dict],
    to_inv: dict[str, dict],
    is_issue: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Find and converts all coordinates within the dict object given.

    The object is the canonical dict representation of either a Page or an Issue.

    Args:
        elem (dict[str, Any]): Page or Issue in which coordinates should be rescaled.
        title (str): Newspaper title of the page or issue.
        to_patch (dict[str, dict]): Dict with all issues to patch which had a non-empty
            image-info file to base it on.
        to_inv (dict[str, dict]): Dict with all issues to patch where the image-info file
            was missing or empty.
        is_issue (bool, optional): Whether the object is an issue. Defaults to True.

    Returns:
        tuple[dict[str, Any], dict[str, Any]]: Rescaled object and patching information.
    """
    # fetch convert info if issue/page needs conversion, and save info
    if is_issue:
        issue_id = elem["id"]
        key = "issue_patching_done"
        patch_info = {"issue_id": issue_id, key: False, "num_pages": len(elem["pp"])}
    else:
        issue_id = "-".join(elem["id"].split("-")[:-1])
        key = "page_patching_done"
        patch_info = {"issue_id": issue_id, key: False, "page_id": elem["id"]}

    # for LCG, only years later than 1891 need to be fixed
    if title != "LCG" or int(issue_id.split("-")[1]) > 1891:
        if issue_id in to_patch:
            res = to_patch[issue_id]
            # keep trace of whether or not we fetched the information from the image info file
            res["used_image_info_file"] = True
        elif issue_id in to_inv:
            res = to_inv[issue_id]
            res["used_image_info_file"] = False
        else:
            return elem, patch_info

        if is_issue:
            elem, scaled = convert_issue_coords(elem, res)
            # there may be no coordinated to scale in an issue
            res["scaled"] = scaled
        else:
            elem, scaled = convert_page_coords(elem, res)
            # sanity check that number of regions+lines+tokens=coords scaled
            res["all_scaled"] = scaled

        # keep trace of information about the patching performed.
        patch_info[key] = True
        patch_info.update(res)

    return elem, patch_info


def main():
    arguments = docopt(__doc__)
    local_base_path = (
        arguments["--local-path"]
        if arguments["--local-path"]
        else "/scratch/piconti/impresso/patch_7"
    )
    final_patches_output_path = os.path.join(local_base_path, "final_patch")
    log_file = (
        arguments["--log-file"]
        if arguments["--log-file"]
        else f"{final_patches_output_path}/patch_7_rero.log"
    )
    error_log = (
        arguments["--error-log"]
        if arguments["--error-log"]
        else f"{final_patches_output_path}/patch_7_rero_errors.log"
    )
    s3_input_bucket = (
        arguments["--input-bucket"] if arguments["--input-bucket"] else "canonical-data"
    )
    s3_output_bucket = (
        arguments["--output-bucket"]
        if arguments["--output-bucket"]
        else "canonical-staging"
    )
    canonical_repo_path = (
        arguments["--canonical-repo-path"]
        if arguments["--canonical-repo-path"]
        else "/home/piconti/impresso-text-acquisition"
    )

    init_logger(logger, logging.INFO, log_file)
    logger.info("Arguments: \n %s", arguments)

    RERO_1_TITLES = ["LCG", "LBP", "LTF", "DLE"]
    PROP_NAME = "c"
    temp_dir = os.path.join(local_base_path, "temp_dir")
    empty_folder(temp_dir)

    logger.info(
        "Patching titles %s: rescaling %s property at issue and page level",
        RERO_1_TITLES,
        PROP_NAME,
    )

    logger.info("Fetching the list of titles to patch")
    all_to_patch_path = os.path.join(local_base_path, "all_issues_to_patch_4.json")
    all_to_inv_path = os.path.join(local_base_path, "all_issues_to_investigate_4.json")

    with open(all_to_patch_path, mode="r", encoding="utf-8") as f:
        all_to_patch = json.load(f)

    with open(all_to_inv_path, mode="r", encoding="utf-8") as f:
        all_to_inv = json.load(f)

    logger.info("Fetching the page and issues files from S3...")
    # download the issues of interest for this patch
    rero_issues, rero_pages = fetch_files(s3_input_bucket, False, "both", RERO_1_TITLES)

    #### PERFORMING THE ACTUAL PATCHING

    logger.info("Fetched the page and issues files from S3, starting the patching...")

    # extract the title and convert the coordinates
    logger.info("Converting the coordinates inside the issues...")
    patched_rero_issues = (
        rero_issues.map_partitions(
            lambda i_list: [(i, i["id"].split("-")[0]) for i in i_list]
        ).map_partitions(
            lambda i_list: [
                find_convert_coords(i, np, all_to_patch[np], all_to_inv[np])
                for (i, np) in i_list
            ]
        )
    ).persist()

    logger.info("Converting the coordinates inside the pages...")
    patched_rero_pages = (
        rero_pages.map_partitions(
            lambda p_list: [(p, p["id"].split("-")[0]) for p in p_list]
        ).map_partitions(
            lambda p_list: [
                find_convert_coords(
                    p, np, all_to_patch[np], all_to_inv[np], is_issue=False
                )
                for (p, np) in p_list
            ]
        )
    ).persist()

    #### WRITING THE OUTPUT TO S3
    logger.info("Uploading the updated issues to s3 bucket %s...", s3_output_bucket)
    rero_issue_files = (
        patched_rero_issues.map_partitions(lambda i_l: [i[0] for i in i_l])
        .map_partitions(title_year_pair_to_issues)
        .map_partitions(
            lambda issues: write_upload_issues(
                issues[0],
                issues[1],
                output_dir=temp_dir,
                bucket_name=s3_output_bucket,
                failed_log=error_log,
            )
        )
    ).compute()

    # free the memory allocated
    del rero_issue_files

    logger.info("Uploading the updated page files to s3 bucket %s...", s3_output_bucket)
    rero_page_files = (
        patched_rero_pages.map_partitions(lambda pages: [p[0] for p in pages])
        .map_partitions(to_issue_id_pages_dict)
        .map_partitions(
            lambda issue_to_pages: nzz_write_upload_pages(
                issue_to_pages,
                output_dir=temp_dir,
                bucket_name=s3_output_bucket,
                failed_log=error_log,
            )
        )
        .flatten()
    ).compute()

    # free the memory allocated
    del rero_page_files

    #### KEEPING TRACK OF THE RESULTS: OUTPUT AND MANIFEST
    logger.info(
        "Aggregating the patching information of issues for future reference..."
    )
    # extract only the "patch_info" dict to keep track of which issue/page has been correctly patched
    patch_info_issues = (
        patched_rero_issues.map_partitions(
            lambda i_l: [i[1] for i in i_l]
        ).to_dataframe(
            meta={
                "issue_id": str,
                "issue_patching_done": bool,
                "num_pages": "Int64",
                "dest_res": "Int64",
                "curr_res": "Int64",
                "zip_contents": str,
                "used_image_info_file": bool,
            }
        )
    ).compute()

    logger.info("Aggregating the patching information of pages for future reference...")
    patch_info_pages = (
        patched_rero_pages.map_partitions(lambda i_l: [i[1] for i in i_l])
        .to_dataframe(
            meta={
                "issue_id": str,
                "page_patching_done": bool,
                "page_id": str,
                "dest_res": "Int64",
                "curr_res": "Int64",
                "zip_contents": str,
                "used_image_info_file": bool,
                "all_scaled": bool,
            }
        )
        .groupby(
            by=[
                "issue_id",
                "page_patching_done",
                "dest_res",
                "curr_res",
                "used_image_info_file",
                "all_scaled",
            ]
        )
        .agg({"page_id": "count"})
        .rename(columns={"page_id": "num_pages"})
        .reset_index()
    ).compute()

    logger.info("Merging the two and writing the dataframe to disk.")
    patched_info_merged_df = patch_info_issues.merge(patch_info_pages, how="outer")
    patched_info_merged_df.to_csv(
        os.path.join(final_patches_output_path, "all_patched_issues.csv")
    )

    logger.info("Finished with the patching, creating the manifest...")

    # create the config for the manifest computation
    manifest_config = {
        "data_stage": "canonical",
        "output_bucket": s3_output_bucket,
        "input_bucket": s3_input_bucket,
        "git_repository": canonical_repo_path,
        "newspapers": RERO_1_TITLES,
        "temp_directory": temp_dir,
        "previous_mft_s3_path": None,
        "is_staging": True,
        "is_patch": True,
        "patched_fields": [PROP_NAME],
        "push_to_git": True,
        "file_extensions": "issues.jsonl.bz2",
        "log_file": log_file,
        "notes": f"Patching RERO 1 data ({RERO_1_TITLES}) to rescale their coordinates (patch_7).",
    }
    # create and upload the manifest
    create_manifest(manifest_config)


if __name__ == "__main__":
    main()
