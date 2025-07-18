"""Command-line script to perform the patch #1 on the UZH canonical data (FedGaz, NZZ).

Usage:
    canonical_patch_1_uzh.py --input-bucket=<ib> --output-bucket=<ob> --canonical-repo-path=<crp> --prev-manifest-path=<pmp> --temp-dir=<td> --log-file=<lf> --error-log=<el> --patch-outputs-filename=<pof>
    
Options:

--input-bucket=<ib>  S3 input bucket.
--output-bucket=<ob>  S3 output bucket.
--canonical-repo-path=<crp>  Path to the local impresso-text-acquisition git repository.
--prev-manifest-path=<pmp>  S3 path of the previous version of the canonical manifest.
--temp-dir=<td>  Temporary directory to write files in.
--log-file=<lf>  Path to log file.
--error-log=<el>  Path to error log file.
--patch-outputs-filename=<pof>  Filename of the .txt file containing the output of the patches.
"""

import os
import logging
import copy
import shutil
from typing import Any, Callable
import git
import jsonlines
from smart_open import open as smart_open_function
import dask.bag as db
from filelock import FileLock
from docopt import docopt

from impresso_essentials.io.s3 import get_storage_options, fetch_files
from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.utils import init_logger
from text_preparation.importers.core import (
    upload_issues,
    upload_pages,
    remove_filelocks,
)


IMPRESSO_STORAGEOPT = get_storage_options()
IMPRESSO_IIIF_BASE_URI = "https://impresso-project.ch/api/proxy/iiif/"

logger = logging.getLogger()


def add_property(
    object_dict: dict[str, Any],
    prop_name: str,
    prop_function: Callable[[str], str],
    function_input: str,
) -> dict[str, Any]:
    """Add a property and value to a given object dict computed with a given function.

    Args:
        object_dict (dict[str, Any]): Object to which the property is added.
        prop_name (str): Name of the property to add.
        prop_function (Callable[[str], str]): Function computing the property value.
        function_input (str): Input to `prop_function` for this object.

    Returns:
        dict[str, Any]: Updated object.
    """
    object_dict[prop_name] = prop_function(function_input)
    logger.debug(
        "%s -> Added property %s: %s",
        object_dict["id"],
        prop_name,
        object_dict[prop_name],
    )
    return object_dict


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


def write_jsonlines_file(
    filepath: str,
    contents: str | list[str],
    content_type: str,
    failed_log: str | None = None,
) -> None:
    """Write the given contents to a JSONL file given its path.

    Filelocks are used here to prevent concurrent writing to the files.

    Args:
        filepath (str): Path to the JSONL file to write to.
        contents (str | list[str]): Dump contents to write to the file.
        content_type (str): Type of content that is being written to the file.
        failed_log (str | None, optional): Path to a log to keep track of failed
            operations. Defaults to None.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # put a file lock to avoid the overwriting of files due to parallelization
    lock = FileLock(filepath + ".lock", timeout=13)

    try:
        with lock:
            with smart_open_function(filepath, "ab") as fout:
                writer = jsonlines.Writer(fout)

                writer.write_all(contents)

                logger.info(
                    "Written %s %s to %s", len(contents), content_type, filepath
                )
                writer.close()
    except Exception as e:
        logger.error("Error for %s", filepath)
        logger.exception(e)
        if failed_log is not None:
            write_error(
                os.path.basename(filepath), "write_jsonlines_file()", e, failed_log
            )


def write_upload_issues(
    key: tuple[str, str],
    issues: list[dict[str, Any]],
    output_dir: str,
    bucket_name: str,
    failed_log: str | None = None,
) -> tuple[bool, str]:
    """Compress issues for a Journal-year in a json file and upload them to s3.

    The compressed ``.bz2`` output file is a JSON-line file, where each line
    corresponds to an individual issue document in the canonical format.

    Args:
        key (tuple[str, str]): Newspaper ID and year of input issues, e.g. `GDL,1900`.
        issues (list[dict[str, Any]]): A list of issues as dicts.
        output_dir (str): Local output directory.
        bucket_name (str): Name of S3 bucket where to upload the file.
        failed_log (str | None): Path to the log in which failed operations are logged.
            Defaults to None.

    Returns:
        tuple[bool, str]: Whether the upload was successful and the path to the
            uploaded file.
    """
    newspaper, year = key
    filename = f"{newspaper}-{year}-issues.jsonl.bz2"
    filepath = os.path.join(output_dir, newspaper, filename)
    logger.info("Compressing %s JSON files into %s", len(issues), filepath)

    if os.path.exists(filepath) and os.path.isfile(filepath):
        # file shsould only be modified once
        logger.warning("The file %s already exists, not modifying it.", filepath)
        return False, filepath

    write_jsonlines_file(filepath, issues, "issues", failed_log)

    return upload_issues("-".join(key), filepath, bucket_name)


def write_upload_pages(
    key: str,
    pages: list[dict[str, Any]],
    output_dir: str,
    bucket_name: str,
    failed_log: str | None = None,
    # uploaded_pages = UPLOADED_PAGES,
) -> tuple[str, tuple[bool, str]]:
    """Compress pages for a given edition in a json file and upload them to s3.

    The compressed ``.bz2`` output file is a JSON-line file, where each line
    corresponds to an individual page document in the canonical format.

    Args:
        key (str): Canonical ID of the newspaper issue (e.g. GDL-1900-01-02-a).
        pages (list[dict[str, Any]]): The list of pages for the provided key.
        output_dir (str): Local output directory.
        bucket_name (str): Name of S3 bucket where to upload the file.

    Returns:
        Tuple[str, str]: Label following the template `<NEWSPAPER>-<YEAR>` and
            the path to the the compressed `.bz2` file.
    """
    newspaper, year, _, _, _ = key.split("-")
    filename = f"{key}-pages.jsonl.bz2"
    filepath = os.path.join(output_dir, newspaper, f"{newspaper}-{year}", filename)
    logger.info("Compressing %s JSON files into %s", len(pages), filepath)

    if os.path.exists(filepath) and os.path.isfile(filepath):
        # file shsould only be modified once
        logger.info("The file %s already exists, not modifying it.", filepath)
        return key, (False, filepath)

    logger.info("uploading pages for %s", key)
    write_jsonlines_file(filepath, pages, "pages", failed_log)

    return key, (upload_pages(key, filepath, bucket_name))


# adapted from https://github.com/impresso/impresso-data-sanitycheck/blob/master/sanity_check/contents/stats.py#L241
def canonical_stats_from_issue_bag(fetched_issues: db.core.Bag) -> list[dict[str, Any]]:
    """Computes number of issues and pages per newspaper from canonical data in s3.

    :param str s3_canonical_bucket: S3 bucket with canonical data.
    :return: A pandas DataFrame with newspaper ID as the index and columns `n_issues`, `n_pages`.
    :rtype: pd.DataFrame

    """
    pages_count_df = (
        fetched_issues.map(
            lambda i: {
                "np_id": i["id"].split("-")[0],
                "year": i["id"].split("-")[1],
                "id": i["id"],
                "issue_id": i["id"],
                "n_pages": len(set(i["pp"])),
                "n_content_items": len(i["i"]),
                "n_images": len(
                    [item for item in i["i"] if item["m"]["tp"] == "image"]
                ),
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "id": str,
                "issue_id": str,
                "n_pages": int,
                "n_images": int,
                "n_content_items": int,
            }
        )
        .set_index("id")
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        pages_count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "n_pages": sum,
                "issue_id": "count",
                "n_content_items": sum,
                "n_images": sum,
            }
        )
        .rename(
            columns={
                "issue_id": "issues",
                "n_pages": "pages",
                "n_content_items": "content_items_out",
                "n_images": "images",
            }
        )
        .reset_index()
    )

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


# define patch function
def uzh_image_base_uri(
    page_id: str, impresso_iiif: str = IMPRESSO_IIIF_BASE_URI
) -> str:
    """
    https://impresso-project.ch/api/proxy/iiif/[page canonical ID]
    """
    return os.path.join(impresso_iiif, page_id)


def to_issue_id_pages_pairs(
    pages: list[dict[str, Any]]
) -> tuple[str, list[dict[str, Any]]]:
    issues_present = set()
    for page in pages:
        issue_id = "-".join(page["id"].split("-")[:-1])
        issues_present.add(issue_id)

    issues = list(issues_present)
    if len(issues) != 1:
        logger.warning(
            "Did not find exactly one issue in the pages: %s, %s",
            issues,
            [p["id"] for p in pages],
        )
        if len(issues) == 0:
            return "", pages
    assert len(issues) <= 1, "there should only be one issue"

    return issues[0], pages


def to_issue_id_pages_dict(
    pages: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:

    issues_present = {}
    for page in pages:
        issue_id = "-".join(page["id"].split("-")[:-1])
        if issue_id not in issues_present:
            issues_present[issue_id] = [page]
        else:
            issues_present[issue_id].append(page)

    if len(issues_present) != 1:
        logger.warning(
            "Did not find exactly one issue in the pages; issue(s): %s", issues_present
        )
        print(
            "Did not find exactly one issue in the pages; issue(s): %s",
            issues_present.keys(),
        )
        if len(issues_present) > 1:
            pairs = [((k, [p["id"] for p in ps]) for k, ps in issues_present)]
            print(f"Here are the specific contents of the pages: {pairs}")

    return issues_present


def title_year_pair_to_issues(
    issues: list[dict[str, Any]]
) -> tuple[tuple[str, str], list[dict[str, Any]]]:
    keys_present = set()
    for issue in issues:
        title, year = issue["id"].split("-")[:2]
        keys_present.add((title, year))

    keys = list(keys_present)
    if len(keys) != 1:
        logger.warning("Did not find exactly one key. Keys: %s", keys)
        if len(keys) == 0:
            return "", issues
    assert len(keys) >= 1, "there should only be one key"

    return keys[0], issues


def nzz_write_upload_pages(
    issues_to_pages: dict[str, list[dict[str, Any]]],
    output_dir: str,
    bucket_name: str,
    failed_log: str | None = None,
) -> list[str, tuple[bool, str]]:

    if len(issues_to_pages) == 0:
        return "", (False, "")

    upload_results = []
    for issue_id, pages in issues_to_pages.items():
        upload_results.append(
            write_upload_pages(issue_id, pages, output_dir, bucket_name, failed_log)
        )

    return upload_results


def main():
    arguments = docopt(__doc__)
    s3_input_bucket = arguments["--input-bucket"]
    s3_output_bucket = arguments["--output-bucket"]
    canonical_repo_path = arguments["--canonical-repo-path"]
    previous_manifest_path = arguments["--prev-manifest-path"]
    temp_dir = arguments["--temp-dir"]
    log_file = arguments["--log-file"]
    error_log = arguments["--error-log"]
    patch_outputs = arguments["--patch-outputs-filename"]

    # initialize values for patch
    UZH_TITLES = ["NZZ"]
    PROP_NAME = "iiif_img_base_uri"
    patched_fields = [PROP_NAME]
    canonical_repo = git.Repo(canonical_repo_path)

    schema_path = f"{canonical_repo_path}/text_importer/impresso-schemas/json/versioning/manifest.schema.json"
    final_patches_output_path = os.path.join(os.path.dirname(log_file), patch_outputs)

    init_logger(logging.INFO, log_file)
    logger.info(
        "Patching titles %s: adding %s property at page level", UZH_TITLES, PROP_NAME
    )

    # empty the temp folder before starting processing to prevent duplication of content inside the files.
    empty_folder(temp_dir)

    # initialise manifest to keep track of updates
    nzz_patch_1_manifest = DataManifest(
        data_stage="canonical",
        s3_output_bucket=s3_output_bucket,
        s3_input_bucket=s3_input_bucket,
        git_repo=canonical_repo,
        temp_dir=temp_dir,
        patched_fields=patched_fields,
        previous_mft_path=previous_manifest_path,
    )

    logger.info("Fetching the page and issues files from S3...")
    # download the issues of interest for this patch
    nzz_issues, nzz_pages = fetch_files("canonical-data", False, "both", UZH_TITLES)

    # compute the statistics that correspond to this
    logger.info("Computing the canonical statistics on the issues...")
    nzz_stats_from_issues = canonical_stats_from_issue_bag(nzz_issues)

    logger.info("Updating the page files and uploading them to s3...")
    # patch the pages and write them back to s3.
    nzz_patched_pages = (
        nzz_pages.map_partitions(
            lambda pages: [
                add_property(p, PROP_NAME, uzh_image_base_uri, p["id"]) for p in pages
            ]
        )
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
    del nzz_pages

    logger.info("Done uploading the page files to s3, filling in the manifest...")

    issue_stats = copy.deepcopy(nzz_stats_from_issues)

    # fill in the manifest statistics and prepare issues to be uploaded to their new s3 bucket.
    logger.info(
        "nzz_patched_pages[0], nzz_patched_pages[1]: %s, %s",
        nzz_patched_pages[0],
        nzz_patched_pages[1],
    )
    for issue_id, (success, path) in zip(
        nzz_patched_pages[::2], nzz_patched_pages[1::2]
    ):
        title, year, _, _, _ = issue_id.split("-")

        # write to file to track potential missing data.
        with open(final_patches_output_path, "a", encoding="utf-8") as outfile:
            outfile.write(f"{issue_id}: {success}, {path} \n")

        if success:
            if not nzz_patch_1_manifest.has_title_year_key(title, year):
                logger.info("Adding stats for %s-%s to manifest", title, year)
                current_stats = [
                    d for d in issue_stats if d["np_id"] == title and d["year"] == year
                ][0]
                # reduce the number of stats to consider at each step
                issue_stats.remove(current_stats)
                # remove unwanted keys from the dict
                del current_stats["np_id"]
                del current_stats["year"]

                nzz_patch_1_manifest.replace_by_title_year(title, year, current_stats)

                # remove all the filelocks for this title and year
                remove_filelocks(os.path.join(temp_dir, title, f"{title}-{year}"))

        elif not success:
            logger.warning(
                "The pages for issue %s were not correctly uploaded", issue_id
            )

    logger.info("Uploading the issue files to the new bucket")
    # write and upload the issues to the new s3 bucket
    yearly_issue_files = (
        nzz_issues.map_partitions(lambda issues: [i for i in issues])
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

    logger.info("Finalizing, computing and exporting the manifest")
    # finalize the manifest and export it
    note = f"Patching titles {UZH_TITLES}: adding {PROP_NAME} property at page level"
    nzz_patch_1_manifest.append_to_notes(note)
    nzz_patch_1_manifest.compute(export_to_git_and_s3=False)
    nzz_patch_1_manifest.validate_and_export_manifest(push_to_git=True)


if __name__ == "__main__":
    main()
