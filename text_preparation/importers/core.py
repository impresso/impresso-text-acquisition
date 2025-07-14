"""Core functions to perform large-scale import of OCR data.

Most of the functions in this module are meant to be used in conjuction
with `Dask <https://dask.org/>`_, the library we are using to parallelize
the ingestion process and run it on distributed computing infrastructures.

Note:
    The function :func:`import_issues` is the most important in this module
    as it keeps everything together, by calling all other functions.
"""

import gc
import json
import logging
import os
import random
import shutil
from copy import copy
from itertools import groupby
from json import JSONDecodeError
from pathlib import Path
from time import strftime
from typing import Tuple, Type
from botocore.exceptions import BotoCoreError
from jsonschema import ValidationError

import jsonlines
from dask import bag as db
from dask.distributed import Client
from filelock import FileLock
from impresso_essentials.utils import IssueDir, chunk, PARTNER_TO_MEDIA, get_src_info_for_alias
from impresso_essentials.io.fs_utils import canonical_path
from impresso_essentials.io.s3 import get_s3_resource
from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.versioning.aggregators import counts_for_canonical_issue
from smart_open import open as smart_open_function

from text_preparation.utils import (
    validate_page_schema,
    validate_issue_schema,
    validate_audio_schema,
)
from text_preparation.importers.classes import (
    CanonicalIssue,
    CanonicalPage,
    CanonicalAudioRecord,
)
from text_preparation.importers.olive.classes import OliveNewspaperIssue
from text_preparation.importers.swa.classes import SWANewspaperIssue

logger = logging.getLogger(__name__)


def write_error(
    thing: CanonicalIssue | CanonicalPage | IssueDir | str,
    error: Exception | str,
    failed_log: str | None,
) -> None:
    """Write the given error of a failed import to the `failed_log` file.

    Args:
        thing (CanonicalIssue | CanonicalPage | IssueDir | str): Object for which
            the error occurred, or corresponding canonical ID.
        error (Exception): Error that occurred and should be logged.
        failed_log (str): Path to log file for failed imports.
    """
    logger.error("Error when processing %s: %s", thing, error)
    logger.exception(error)

    if isinstance(thing, str):
        # if thing is a string, it's the canonical id of the object
        note = f"{thing}: {error}"
    else:
        if isinstance(thing, CanonicalPage):
            issuedir = thing.issue.issuedir
        if isinstance(thing, CanonicalAudioRecord):
            issuedir = thing.issue.issuedir
        elif isinstance(thing, CanonicalIssue):
            issuedir = thing.issuedir
        else:
            # if it's neither an issue nor a page it must be an issuedir
            issuedir = thing

        note = f"{canonical_path(issuedir)}: {error}"

    if failed_log is not None:
        with open(failed_log, "a+", encoding="utf-8") as f:
            f.write(note + "\n")


def cleanup(upload_success: bool, filepath: str) -> None:
    """Remove a file if it has been successfully uploaded to S3.

    Copied and adapted from impresso-pycommons.

    Args:
        upload_success (bool): Whether the upload was successful
        filepath (str): Path to the uploaded file
    """
    if upload_success and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info("Removed temporary file %s", filepath)
        except Exception as e:
            logger.warning("Error %s occurred when removing %s.", e, filepath)
    else:
        logger.info("Not removing %s as upload has failed", filepath)


def dir2issue(
    issue: IssueDir,
    issue_class: Type[CanonicalIssue],
    failed_log: str | None = None,
    image_dirs: str | None = None,
    temp_dir: str | None = None,
) -> CanonicalIssue | None:
    """Instantiate a `CanonicalIssue` object from an `IssueDir`.

    Any instantiation leading to an exception is logged to a specific file
    only containing issues which could not be imported.

    Args:
        issue (IssueDir): `IssueDir` representing the issue to instantiate.
        issue_class (Type[CanonicalIssue]): Type of `CanonicalIssue` to use.
        failed_log (str | None, optional): Path to the log file used if the
            instantiation was not successful. Defaults to None.
        image_dirs (str | None, optional): Path to the directory containing the
            information on images, only for Olive importer. Defaults to None.
        temp_dir (str | None, optional): Temporary directory to unpack the
            issue's zip archive into. Defaults to None.

    Returns:
        CanonicalIssue | None: A new `CanonicalIssue` instance, or `None` if
            the instantiation triggered an exception.
    """
    try:
        if issue_class is OliveNewspaperIssue:
            np_issue = OliveNewspaperIssue(issue, image_dirs, temp_dir)
        elif issue_class is SWANewspaperIssue:
            np_issue = SWANewspaperIssue(issue, temp_dir=temp_dir)
        else:
            np_issue = issue_class(issue)
        return np_issue
    except Exception as e:
        write_error(issue, e, failed_log)
        return None


def dirs2issues(
    issues: list[IssueDir],
    issue_class: Type[CanonicalIssue],
    failed_log: str | None = None,
    image_dirs: str | None = None,
    temp_dir: str | None = None,
) -> list[CanonicalIssue]:
    """Instantiate the `CanonicalIssue` objects to import to Impresso's format.

    Any `CanonicalIssue` for which the instantiation is unsuccessful
    will be logged, along with the triggered error.

    Args:
        issues (list[IssueDir]): List of issues to instantiate and import.
        issue_class (Type[CanonicalIssue]): Type of `CanonicalIssue` to use.
        failed_log (str | None, optional): Path to the log file used when an
            instantiation was not successful. Defaults to None.
        image_dirs (str | None, optional): Path to the directory containing the
            information on images, only for Olive importer. Defaults to None.
        temp_dir (str | None, optional): Temporary directory to unpack zip
            archives of issues into. Defaults to None.

    Returns:
        list[CanonicalIssue]: List of `CanonicalIssue` instances to import.
    """
    ret = []
    for issue in issues:
        np_issue = dir2issue(issue, issue_class, failed_log, image_dirs, temp_dir)
        if np_issue is not None:
            ret.append(np_issue)
    return ret


def is_audio_issue(issue: CanonicalIssue) -> bool:
    """Checks whether the given canonical issue corresponds to audio data.

    This is done by checking the "sm" (ie. "source medium") property.

    Args:
        issue (CanonicalIssue): Canonical Issue for which to identify the medium

    Raises:
        NotImplementedError: If the source medium found is invalid.

    Returns:
        bool: True if the issue corresponds to audio data, False otherwise.
    """
    # if "sm" is not in issue.issue_data, then it's a newspaper
    if "sm" not in issue.issue_data or (
        "sm" in issue.issue_data and issue.issue_data["sm"] in ["print", "typescript"]
    ):
        return False
    elif issue.issue_data["sm"] == "audio":
        return True
    else:
        msg = f"{issue.id} - Source medium for this issue is not valid! {issue.issue_data['sm']}"
        print(msg)
        raise NotImplementedError(msg)


def issue2supports(issue: CanonicalIssue) -> list[CanonicalPage] | list[CanonicalAudioRecord]:
    """Flatten an issue into a list of their pages or audio records.

    Case of newspapers and radio bulletins:
    As an issue consists of several pages, this function is useful
    in order to process each page in a truly parallel fashion.

    For radio audio broadcasts, it's more to keep a unified processing approach

    Args:
        issue (CanonicalIssue): Issue to collect the pages or audio records of.

    Returns:
        list[CanonicalPage] | list[CanonicalAudioRecord]: List of pages or audio records of the given issue.
    """
    if is_audio_issue(issue):
        support_list = issue.audio_records
    else:
        support_list = issue.pages

    supports = []
    for support in support_list:
        support.add_issue(issue)
        supports.append(support)

    return supports


def serialize_supports(
    supports: list[CanonicalPage] | list[CanonicalAudioRecord],
    output_dir: str | None = None,
    failed_log: str | None = None,
    is_audio: bool | None = None,
) -> list[Tuple[IssueDir, str]]:
    """Serialize a list of pages or audio records to an output directory.

    Args:
        pages (list[CanonicalPage] | list[CanonicalAudioRecord]): Input
            canonical pages or audio supports.
        output_dir (str | None, optional): Path to the output directory.
            Defaults to None.
        failed_log (str | None, optional): Path to the log file used when an
            instantiation was not successful. Defaults to None.

    Returns:
        list[Tuple[IssueDir, str]]: A list of tuples (`IssueDir`, `path`),
            where the `IssueDir` object represents the issue to which pages
            belong, and `path` the path to the individual page JSON file.
    """
    result = []

    for support in supports:

        issue_dir = copy(support.issue.issuedir)

        out_dir = os.path.join(output_dir, canonical_path(issue_dir, as_dir=True))

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        if is_audio:
            canonical_filename = canonical_path(
                issue_dir, "r" + str(support.number).zfill(4), ".json"
            )
        else:
            canonical_filename = canonical_path(
                issue_dir, "p" + str(support.number).zfill(4), ".json"
            )

        out_file = os.path.join(out_dir, canonical_filename)

        try:
            if is_audio:
                validate_audio_schema(support.record_data)
                validated_data = support.record_data
            else:
                validate_page_schema(support.page_data)
                validated_data = support.page_data
        except ValidationError as e:
            msg = f"Invalid schema for {canonical_filename}: {e}"
            logger.error(msg)
            print(msg)
            write_error(out_file, e, failed_log)

        with open(out_file, "w", encoding="utf-8") as jsonfile:
            json.dump(validated_data, jsonfile)
            logger.info(
                "Written %s '%s' to %s",
                "record" if is_audio else "page",
                support.number,
                out_file,
            )
        result.append((issue_dir, out_file))

    # this can be deleted, I believe as it has no effect
    gc.collect()
    return result


def process_supports(
    supports: list[CanonicalPage | CanonicalAudioRecord], failed_log: str
) -> list[CanonicalPage | CanonicalAudioRecord]:
    """Given a list of pages, trigger the ``.parse()`` method of each page.

    Args:
        supports (list[CanonicalPage | CanonicalAudioRecord]): Input canonical supports.
        failed_log (str): File path of failed log.

    Returns:
        list[CanonicalPage | CanonicalAudioRecord]: A list of processed supports.
    """
    result = []
    for support in supports:
        try:
            support.parse()
            result.append(support)
        except Exception as e:
            write_error(support, e, failed_log)
    return result


def import_issues(
    issues: list[IssueDir],
    out_dir: str,
    s3_bucket: str | None,
    issue_class: Type[CanonicalIssue],
    image_dirs: str | None,
    temp_dir: str | None,
    chunk_size: int | None,
    manifest: DataManifest,
    client: Client | None = None,
    is_audio: bool | None = False,
) -> None:
    """Import a bunch of canonical issues (newspaper or radio-broadcast).

    Args:
        issues (list[IssueDir]): Issues to import.
        out_dir (str): Output directory for the json files.
        s3_bucket (str | None): Output s3 bucket for the json files.
        issue_class (Type[CanonicalIssue]): Canonical issue class to import,
            (Child of ``CanonicalIssue``).
        image_dirs (str | None): Directory of images for Olive format,
            (can be multiple).
        temp_dir (str | None): Temporary directory for extracting archives
            (applies only to importers make use of ``ZipArchive``).
        chunk_size (int | None): Chunk size in years used to process issues.
        manifest (DataManifest): Data manifest instance, tracking the stats on the imported data.
        client (Client | None, optional): Dask client. Defaults to None.
        is_audio (bool | None, optional): The if the data being imported is audio. Defaults to False.
    """
    supports_name = "audios" if is_audio else "pages"
    print(f"supports_name: {supports_name}")
    msg = f"Issues to import: {len(issues)}"
    print(msg)
    logger.info(msg)
    failed_log_path = os.path.join(out_dir, f'failed-{strftime("%Y-%m-%d-%H-%M-%S")}.log')
    if chunk_size is not None:
        csize = int(chunk_size)
        chunks = groupby(
            sorted(issues, key=lambda x: x.date.year),
            lambda x: x.date.year - (x.date.year % csize),
        )

        chunks = [(year, list(issues)) for year, issues in chunks]
        msg = (
            f"Dividing issues into chunks of {chunk_size} years " f"({len(chunks)} chunks in total)"
        )
        logger.info(msg)
        for year, issue_chunk in chunks:
            msg = (
                f"Chunk of period {year} - {year + csize - 1} covers " f"{len(issue_chunk)} issues"
            )
            logger.info(msg)
    else:
        chunks = [(None, issues)]

    if logger.level == logging.DEBUG:
        print(f"chunks: {chunks}")

    for year, issue_chunk in chunks:
        if year is None:
            period = "all years"
        else:
            period = f"{year} - {year + csize - 1}"

        temp_issue_bag = db.from_sequence(issue_chunk, partition_size=20)

        issue_bag = temp_issue_bag.map_partitions(
            dirs2issues,
            issue_class=issue_class,
            failed_log=failed_log_path,
            image_dirs=image_dirs,
            temp_dir=temp_dir,
        ).persist()

        logger.info("Start compressing issues for %s", period)

        compressed_issue_files = (
            issue_bag.groupby(lambda i: (i.alias, i.date.year))
            .starmap(
                compress_issues,
                output_dir=out_dir,
                failed_log=failed_log_path,
                is_audio=is_audio,
            )
            .compute()
        )

        logger.info("Done compressing issues for %s, updating the manifest...", period)
        # Once the issues were written to the fs without issues, add their info to the manifest
        for index, (np_year, filepath, yearly_stats) in enumerate(compressed_issue_files):
            manifest.add_count_list_by_title_year(
                np_year.split("-")[0],
                np_year.split("-")[1],
                yearly_stats,
            )
            # remove the yearly stats from the filenames
            compressed_issue_files[index] = (np_year, filepath)

        logger.info("Start uploading issues for %s", period)

        # NOTE: As a function of the partitioning size and the number of issues,
        # the issues of a single year may be assigned to different partitions.
        # To prevent further processing before the compressed issues of a single year is completed,
        # the steps of compressing and uploading/deleting are separated from each other.
        # Moreover, the issues are deduplicated after compressing due to the unpredictable partitioning.
        # If not respected, the import may result in incomplete issue files and non-reproducible errors.
        # TODO: The issues should be processed within a dask dataframe instead of bag
        # to get cleaner code while ensuring proper partitioning.

        (
            db.from_sequence(set(compressed_issue_files))
            .starmap(upload_issues, bucket_name=s3_bucket, failed_log=failed_log_path)
            .starmap(cleanup)
            .compute()
        )

        logger.info("Done uploading issues for %s", period)

        processed_issues = list(issue_bag)
        random.shuffle(processed_issues)

        chunks = chunk(processed_issues, 400)

        for chunk_n, chunk_of_issues in enumerate(chunks):
            logger.info("Processing chunk %s of pages/audios for %s", chunk_n, period)

            supports_bag = (
                db.from_sequence(chunk_of_issues, partition_size=2)
                .map(issue2supports)
                .flatten()
                .map_partitions(process_supports, failed_log=failed_log_path)
                .map_partitions(
                    serialize_supports,
                    output_dir=out_dir,
                    failed_log=failed_log_path,
                    is_audio=is_audio,
                )
            )

            supports_out_dir = os.path.join(out_dir, supports_name)
            Path(supports_out_dir).mkdir(exist_ok=True)

            logger.info(
                "Start compressing and uploading %s of chunk %s for %s",
                supports_name,
                chunk_n,
                period,
            )
            supports_bag = (
                supports_bag.groupby(lambda x: canonical_path(x[0]))
                .starmap(
                    compress_supports,
                    suffix=supports_name,
                    output_dir=supports_out_dir,
                    failed_log=failed_log_path,
                )
                .starmap(
                    upload_supports,
                    bucket_name=s3_bucket,
                    failed_log=failed_log_path,
                    supports_name=supports_name,
                )
                .starmap(cleanup)
                .compute()
            )

            logger.info(
                "Done compressing and uploading %s of chunk %s for %s",
                supports_name,
                chunk_n,
                period,
            )

        # free some dask memory
        if client:
            # if client is defined here
            client.cancel(issue_bag)
        else:
            del issue_bag

    remove_filelocks(out_dir)

    # finalize and compute the manifest
    if s3_bucket is not None and "sandbox" in s3_bucket or "sandbox" in manifest.output_bucket_name:
        # don't push to git if output bucket is sandbox
        manifest.compute(export_to_git_and_s3=False)
        manifest.validate_and_export_manifest(push_to_git=False)
    else:
        manifest.compute()

    if temp_dir is not None and os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    logger.info("---------- Done ----------")

    if client:
        # shutdown dask client once processing is done.
        client.shutdown()


def compress_supports(
    key: str,
    json_files: list[str],
    output_dir: str,
    suffix: str = "",
    failed_log: str | None = None,
) -> Tuple[str, str]:
    """Merge a set of JSON line files into a single compressed archive.

    Args:
        key (str): Canonical ID of the issue (e.g. GDL-1900-01-02-a).
        json_files (list[str]): Paths of input JSON line files.
        output_dir (str): Directory where to write the output file.
        suffix (str, optional): Suffix to add to the filename. Defaults to "".

    Returns:
        Tuple[str, str]: Sorting key [0] and path to serialized file [1].
    """
    alias, year, month, day, edition = key.split("-")
    suffix_string = "" if suffix == "" else f"-{suffix}"
    filename = f"{alias}-{year}-{month}-{day}-{edition}{suffix_string}.jsonl.bz2"
    filepath = os.path.join(output_dir, filename)
    logger.info("Compressing %s JSON files into %s", len(json_files), filepath)

    with smart_open_function(filepath, "wb") as fout:
        writer = jsonlines.Writer(fout)

        items_count = 0
        for issue, json_file in json_files:

            with open(json_file, "r", encoding="utf-8") as inpf:
                try:
                    item = json.load(inpf)
                    writer.write(item)
                    items_count += 1
                except JSONDecodeError as e:
                    logger.error("Reading data from %s failed", json_file)
                    logger.exception(e)
                    write_error(filepath, e, failed_log)
            logger.info("Written %s docs from %s to %s", items_count, json_file, filepath)

        writer.close()

    return key, filepath


def compress_issues(
    key: Tuple[str, int],
    issues: list[CanonicalIssue],
    output_dir: str | None = None,
    failed_log: str | None = None,
    is_audio: bool | None = None,
) -> Tuple[str, str, list[dict[str, int]]]:
    """Compress issues of the same alias-year and save them in a json file.

    First check if the file exists, load it and then over-write/add the newly
    generated issues.
    The compressed ``.bz2`` output file is a JSON-line file, where each line
    corresponds to an individual and issue document in the canonical format.
    Finally, yearly statistics are computed on the issues and included in the
    returned values.

    Args:
        key (Tuple[str, int]): Newspaper or media alias and year of input issues
            (e.g. `(GDL, 1900)`).
        issues (list[CanonicalIssue]): A list of `CanonicalIssue` instances.
        output_dir (str | None, optional): Output directory. Defaults to None.
        failed_log (str | None, optional): Path to the log file used when an
            instantiation was not successful. Defaults to None.

    Returns:
        Tuple[str, str]: Label following the template `<ALIAS>-<YEAR>`, the path to
            the the compressed `.bz2` file, and the statistics computed on the issues.
    """
    alias, year = key
    filename = f"{alias}-{year}-issues.jsonl.bz2"
    filepath = os.path.join(output_dir, filename)
    logger.info("Compressing %s JSON files into %s", len(issues), filepath)

    # put a file lock to avoid the overwriting of files due to parallelization
    lock = FileLock(filepath + ".lock", timeout=13)

    items = []
    yearly_stats = []
    last_id = None
    try:
        for issue in issues:
            last_id = issue.id
            validate_issue_schema(issue.issue_data)
            if alias not in PARTNER_TO_MEDIA["SWISSINFO"]:
                items.append(issue.issue_data)
            elif issue.has_pages:
                # for swissinfo, only add the issue when there is actually data
                items.append(issue.issue_data)
            else:
                msg = f"{issue} - Issue has no pages (no OCR text), it's not ingested."
                write_error(issue, msg, failed_log)

        with lock:
            with smart_open_function(filepath, "ab") as fout:
                writer = jsonlines.Writer(fout)

                # items = [issue.issue_data for issue in issues]
                writer.write_all(items)

                logger.info("Written %s issues to %s", len(items), filepath)
                writer.close()

    except ValidationError as e:
        msg = f"Invalid schema for {filepath} - {last_id}: {e}"
        logger.error(msg)
        print(msg)
        write_error(filepath, e, failed_log)
    except Exception as e:
        msg = f"Error for {filepath}: {e}"
        print(msg)
        logger.error(msg)
        write_error(filepath, e, failed_log)
    else:
        # Once the issues were written without problems, add their info to the manifest
        # src mediuem can be something else, but it does not affect here
        src_medium = get_src_info_for_alias(alias)
        for i in items:
            yearly_stats.append(counts_for_canonical_issue(i, src_medium=src_medium))

    return f"{alias}-{year}", filepath, yearly_stats


def upload_issues(
    sort_key: str,
    filepath: str,
    bucket_name: str | None = None,
    failed_log: str | None = None,
) -> Tuple[bool, str]:
    """Upload an issues JSON-line file to a given S3 bucket.

    `sort_key` is expected to be the concatenation of title alias and year.

    Args:
        sort_key (str): Key used to group articles (e.g. "GDL-1900").
        filepath (str): Path of the file to upload to S3.
        bucket_name (str | None, optional): Name of S3 bucket where to upload
            the file. Defaults to None.
        failed_log (str | None, optional): Path to file where to log errors.

    Returns:
        Tuple[bool, str]: Whether the upload was successful and the path to the
            uploaded file.
    """
    # create connection with bucket
    # copy contents to s3 key
    alias, _ = sort_key.split("-")
    key_name = os.path.join(alias, "issues", os.path.basename(filepath))
    # key_name = "{}/{}/{}".format(alias, "issues", os.path.basename(filepath))
    s3 = get_s3_resource()
    if bucket_name is not None:
        try:
            bucket = s3.Bucket(bucket_name)
            bucket.upload_file(filepath, key_name)
            logger.info("Uploaded %s to %s", filepath, key_name)
            return True, filepath
        except BotoCoreError as e:
            logger.error("The upload of %s failed with error %s", filepath, e)
            write_error(filepath, e, failed_log)
    else:
        logger.info("Bucket name is None, not uploading issue %s.", filepath)
    return False, filepath


def upload_supports(
    sort_key: str,
    filepath: str,
    bucket_name: str | None = None,
    failed_log: str | None = None,
    supports_name: str | None = None,
) -> Tuple[bool, str]:
    """Upload a page JSON file to a given S3 bucket.

    Args:
        sort_key (str): the key used to group articles (e.g. "GDL-1900-01-01-a").
        filepath (str): Path of the file to upload to S3.
        bucket_name (str | None, optional): Name of S3 bucket where to upload
            the file. Defaults to None.
        failed_log (str | None, optional): Path to file where to log errors.

    Returns:
        Tuple[bool, str]: Whether the upload was successful and the path to the
            uploaded file.
    """
    # create connection with bucket
    # copy contents to s3 key
    alias, year, _, _, _ = sort_key.split("-")
    key_name = os.path.join(alias, supports_name, f"{alias}-{year}", os.path.basename(filepath))
    # key_name = "{}/pages/{}/{}".format(alias, f"{alias}-{year}", os.path.basename(filepath))
    # or key_name = "{}/audios/{}/{}".format(alias, f"{alias}-{year}", os.path.basename(filepath))
    s3 = get_s3_resource()
    if bucket_name is not None:
        try:
            bucket = s3.Bucket(bucket_name)
            bucket.upload_file(filepath, key_name)
            logger.info("Uploaded %s to %s", filepath, key_name)
            return True, filepath
        except BotoCoreError as e:
            logger.error("The upload of %s failed with error %s", filepath, e)
            write_error(filepath, e, failed_log)
    else:
        logger.info("Bucket name is None, not uploading page %s.", filepath)

    return False, filepath


def remove_filelocks(output_dir: str) -> None:
    """Remove all files ending with .lock in a directory.

    Args:
        output_dir (str): Path to directory containing file locks.
    """
    files = os.listdir(output_dir)

    for file in files:
        try:
            if file.endswith(".lock"):
                os.remove(os.path.join(output_dir, file))
        except FileNotFoundError as e:
            logger.error("File %s could not be removed as it does not exist: %s.", file, e)
