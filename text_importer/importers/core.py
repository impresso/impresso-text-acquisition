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

import jsonlines
from dask import bag as db
from dask.distributed import Client
from filelock import FileLock
from impresso_commons.path.path_fs import IssueDir, canonical_path
from impresso_commons.text.rebuilder import cleanup
from impresso_commons.utils import chunk
from impresso_commons.utils.s3 import get_s3_resource
from impresso_commons.versioning.data_manifest import DataManifest
from impresso_commons.versioning.helpers import DataStage, counts_for_canonical_issue
from smart_open import open as smart_open_function

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.olive.classes import OliveNewspaperIssue
from text_importer.importers.swa.classes import SWANewspaperIssue

logger = logging.getLogger(__name__)


def write_error(
    thing: NewspaperIssue | NewspaperPage | IssueDir,
    error: Exception, 
    failed_log: str
) -> None:
    """Write the given error of a failed import to the `failed_log` file.

    Args:
        thing (NewspaperIssue | NewspaperPage | IssueDir): Object for which
            the error occurred.
        error (Exception): Error that occurred and should be logged.
        failed_log (str): Path to log file for failed imports.
    """
    logger.error(f'Error when processing {thing}: {error}')
    logger.exception(error)
    if isinstance(thing, NewspaperPage):
        issuedir = thing.issue.issuedir
    elif isinstance(thing, NewspaperIssue):
        issuedir = thing.issuedir
    else:
        # if it's neither an issue nor a page it must be an issuedir
        issuedir = thing

    note = (
        f"{canonical_path(issuedir, path_type='dir').replace('/', '-')}: "
        f"{error}"
    )

    with open(failed_log, "a+") as f:
        f.write(note + "\n")


def dir2issue(
    issue: IssueDir,
    issue_class: Type[NewspaperIssue],
    failed_log: str | None = None,
    image_dirs: str | None = None, 
    temp_dir: str | None = None
) -> NewspaperIssue | None:
    """Instantiate a `NewspaperIssue` object from an `IssueDir`.

    Any instantiation leading to an exception is logged to a specific file
    only containing issues which could not be imported.

    Args:
        issue (IssueDir): `IssueDir` representing the issue to instantiate.
        issue_class (Type[NewspaperIssue]): Type of `NewspaperIssue` to use.
        failed_log (str | None, optional): Path to the log file used if the
            instantiation was not successful. Defaults to None.
        image_dirs (str | None, optional): Path to the directory containing the
            information on images, only for Olive importer. Defaults to None.
        temp_dir (str | None, optional): Temporary directory to unpack the 
            issue's zip archive into. Defaults to None.

    Returns:
        NewspaperIssue | None: A new `NewspaperIssue` instance, or `None` if
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
    issue_class: Type[NewspaperIssue],
    failed_log: str | None = None,
    image_dirs: str | None = None, 
    temp_dir: str | None = None
) -> list[NewspaperIssue]:
    """Instantiate the `NewspaperIssue` objects to import to Impresso's format.

    Any `NewspaperIssue` for which the instantiation is unsuccessful
    will be logged, along with the triggered error.

    Args:
        issues (list[IssueDir]): List of issues to instantiate and import.
        issue_class (Type[NewspaperIssue]): Type of `NewspaperIssue` to use.
        failed_log (str | None, optional): Path to the log file used when an
            instantiation was not successful. Defaults to None.
        image_dirs (str | None, optional): Path to the directory containing the
            information on images, only for Olive importer. Defaults to None.
        temp_dir (str | None, optional): Temporary directory to unpack zip
            archives of issues into. Defaults to None.

    Returns:
        list[NewspaperIssue]: List of `NewspaperIssue` instances to import.
    """
    ret = []
    for issue in issues:
        np_issue = dir2issue(
            issue, issue_class, failed_log, image_dirs, temp_dir
        )
        if np_issue is not None:
            ret.append(np_issue)
    return ret


def issue2pages(issue: NewspaperIssue) -> list[NewspaperPage]:
    """Flatten an issue into a list of their pages.

    As an issue consists of several pages, this function is useful
    in order to process each page in a truly parallel fashion. 

    Args:
        issue (NewspaperIssue): Issue to collect the pages of.

    Returns:
        list[NewspaperPage]: List of pages of the given issue.
    """
    pages = []
    for page in issue.pages:
        page.add_issue(issue)
        pages.append(page)
    return pages


def serialize_pages(
    pages: list[NewspaperPage],
    output_dir: str | None = None
) -> list[Tuple[IssueDir, str]]:
    """Serialize a list of pages to an output directory.

    Args:
        pages (list[NewspaperPage]): Input newspaper pages.
        output_dir (str | None, optional): Path to the output directory. 
            Defaults to None.

    Returns:
        list[Tuple[IssueDir, str]]: A list of tuples (`IssueDir`, `path`), 
            where the `IssueDir` object represents the issue to which pages 
            belong, and `path` the path to the individual page JSON file.
    """
    result = []

    for page in pages:

        issue_dir = copy(page.issue.issuedir)

        out_dir = os.path.join(
            output_dir,
            canonical_path(issue_dir, path_type="dir")
        )

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        canonical_filename = canonical_path(
            issue_dir,
            "p" + str(page.number).zfill(4),
            ".json"
        )

        out_file = os.path.join(out_dir, canonical_filename)

        with open(out_file, 'w', encoding='utf-8') as jsonfile:
            json.dump(page.page_data, jsonfile)
            logger.info(f"Written page \'{page.number}\' to {out_file}")
        result.append((issue_dir, out_file))

    # TODO: this can be deleted, I believe as it has no effect
    gc.collect()
    return result


def process_pages(
    pages: list[NewspaperPage], failed_log: str
) -> list[NewspaperPage]:
    """Given a list of pages, trigger the ``.parse()`` method of each page.

    Args:
        pages (list[NewspaperPage]): Input newspaper pages.
        failed_log (str): File path of failed log.

    Returns:
        list[NewspaperPage]: A list of processed pages.
    """
    result = []
    for page in pages:
        try:
            page.parse()
            result.append(page)
        except Exception as e:
            write_error(page, e, failed_log)
    return result


def import_issues(
    issues: list[IssueDir],
    out_dir: str,
    s3_bucket: str | None,
    issue_class: Type[NewspaperIssue],
    image_dirs: str | None,
    temp_dir: str | None,
    chunk_size: int | None,
    manifest: DataManifest,
    client: Client | None = None,
) -> None:
    """Import a bunch of newspaper issues.

    Args:
        issues (list[IssueDir]): Issues to import.
        out_dir (str): Output directory for the json files.
        s3_bucket (str | None): Output s3 bucket for the json files.
        issue_class (Type[NewspaperIssue]): Newspaper issue class to import,
            (Child of ``NewspaperIssue``).
        image_dirs (str | None): Directory of images for Olive format, 
            (can be multiple).
        temp_dir (str | None): Temporary directory for extracting archives
            (applies only to importers make use of ``ZipArchive``).
        chunk_size (int | None): Chunk size in years used to process issues.
    """
    msg = f'Issues to import: {len(issues)}'
    logger.info(msg)
    failed_log_path = os.path.join(
        out_dir,
        f'failed-{strftime("%Y-%m-%d-%H-%M-%S")}.log'
    )
    if chunk_size is not None:
        csize = int(chunk_size)
        chunks = groupby(
            sorted(issues, key=lambda x: x.date.year), 
            lambda x: x.date.year - (x.date.year % csize)
        )

        chunks = [(year, list(issues)) for year, issues in chunks]
        logger.info(f"Dividing issues into chunks of {chunk_size} years "
                    f"({len(chunks)} chunks in total)")
        for year, issue_chunk in chunks:
            logger.info(f"Chunk of period {year} - {year + csize - 1} covers "
                        f"{len(issue_chunk)} issues")
    else:
        chunks = [(None, issues)]

    for year, issue_chunk in chunks:
        if year is None:
            period = 'all years'
        else:
            period = f'{year} - {year + csize - 1}'

        temp_issue_bag = db.from_sequence(issue_chunk, partition_size=20)

        issue_bag = temp_issue_bag.map_partitions(
            dirs2issues,
            issue_class=issue_class,
            failed_log=failed_log_path,
            image_dirs=image_dirs,
            temp_dir=temp_dir
        ).persist()

        logger.info(f'Start compressing issues for {period}')

        compressed_issue_files = (
            issue_bag.groupby(lambda i: (i.journal, i.date.year)) 
            .starmap(compress_issues, 
                     manifest=manifest,
                     output_dir=out_dir,
                     failed_log=failed_log_path) 
            .compute()
        )

        # recover the updated manifest
        manifest = compressed_issue_files[0][2]
        
        logger.info(f'Done compressing issues for {period}')

        logger.info(f'Start uploading issues for {period}')

        # NOTE: As a function of the partitioning size and the number of issues,
        # the issues of a single year may be assigned to different partitions.
        # To prevent further processing before the compressed issues of a single year is completed,
        # the steps of compressing and uploading/deleting are separated from each other.
        # Moreover, the issues are deduplicated after compressing due to the unpredictable partitioning.
        # If not respected, the import may result in incomplete issue files and non-reproducible errors.
        # TODO: The issues should be processed within a dask dataframe instead of bag
        # to get cleaner code while ensuring proper partitioning.

        (db.from_sequence(set(compressed_issue_files)) 
         .map(lambda tuple: (tuple[0], tuple[1])) 
         .starmap(upload_issues, bucket_name=s3_bucket)
         .starmap(cleanup).compute())

        logger.info(f'Done uploading issues for {period}')

        processed_issues = list(issue_bag)
        random.shuffle(processed_issues)

        chunks = chunk(processed_issues, 400)

        for chunk_n, chunk_of_issues in enumerate(chunks):
            logger.info(f'Processing chunk {chunk_n} of pages for {period}')

            pages_bag = (
                db.from_sequence(chunk_of_issues, partition_size=2)
                .map(issue2pages)
                .flatten()
                .map_partitions(process_pages, failed_log=failed_log_path)
                .map_partitions(serialize_pages, output_dir=out_dir)
            )

            pages_out_dir = os.path.join(out_dir, 'pages')
            Path(pages_out_dir).mkdir(exist_ok=True)

            logger.info(f'Start compressing and uploading pages '
                        f'of chunk {chunk_n} for {period}')
            pages_bag = (
                pages_bag.groupby(
                    lambda x: canonical_path(
                        x[0], path_type='dir'
                    ).replace('/', '-')
                )
                .starmap(
                    compress_pages, suffix='pages', output_dir=pages_out_dir
                )
                .starmap(upload_pages, bucket_name=s3_bucket) 
                .starmap(cleanup) 
                .compute()
            )

            logger.info(f'Done compressing and uploading pages '
                        f'of chunk {chunk_n} for {period}')

        # free some dask memory 
        if client:
            # if client is defined here
            client.cancel(issue_bag) 
        else:
            del issue_bag

    remove_filelocks(out_dir)

    # finalize and compute the manifest
    manifest.compute(export_to_git_and_s3 = False)
    manifest.validate_and_export_manifest(push_to_git=False)

    if temp_dir is not None and os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    logger.info("---------- Done ----------")

    if client:
        # shutdown dask client once processing is done.
        client.shutdown()


def compress_pages(
    key: str,
    json_files: list[str],
    output_dir: str,
    suffix: str = ""
) -> Tuple[str, str]:
    """Merge a set of JSON line files into a single compressed archive.

    Args:
        key (str): Canonical ID of the newspaper issue (e.g. GDL-1900-01-02-a).
        json_files (list[str]): Paths of input JSON line files.
        output_dir (str): Directory where to write the output file.
        suffix (str, optional): Suffix to add to the filename. Defaults to "".

    Returns:
        Tuple[str, str]: Sorting key [0] and path to serialized file [1].
    """
    newspaper, year, month, day, edition = key.split('-')
    suffix_string = "" if suffix == "" else f"-{suffix}"
    filename = (
        f'{newspaper}-{year}-{month}-{day}-{edition}'
        f'{suffix_string}.jsonl.bz2'
    )
    filepath = os.path.join(output_dir, filename)
    logger.info(f'Compressing {len(json_files)} JSON files into {filepath}')

    with smart_open_function(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)

        items_count = 0
        for issue, json_file in json_files:

            with open(json_file, 'r') as inpf:
                try:
                    item = json.load(inpf)
                    writer.write(item)
                    items_count += 1
                except JSONDecodeError as e:
                    logger.error(f'Reading data from {json_file} failed')
                    logger.exception(e)
            logger.info(
                f'Written {items_count} docs from {json_file} to {filepath}'
            )

        writer.close()

    return key, filepath


def compress_issues(
    key: Tuple[str, int],
    issues: list[NewspaperIssue],
    manifest: DataManifest,
    output_dir: str | None = None,
    failed_log: str | None = None,
) -> Tuple[str, str, str]:
    """Compress issues of the same Journal-year and save them in a json file.

    First check if the file exists, load it and then over-write/add the newly
    generated issues.
    The compressed ``.bz2`` output file is a JSON-line file, where each line
    corresponds to an individual and issue document in the canonical format.

    Args:
        key (Tuple[str, int]): Newspaper ID and year of input issues 
            (e.g. `(GDL, 1900)`).
        issues (list[NewspaperIssue]): A list of `NewspaperIssue` instances.
        output_dir (str | None, optional): Output directory. Defaults to None.
        failed_log (str | None, optional): Path to the log file used when an
            instantiation was not successful. Defaults to None.

    Returns:
        Tuple[str, str]: Label following the template `<NEWSPAPER>-<YEAR>` and 
            the path to the the compressed `.bz2` file.
    """
    newspaper, year = key
    filename = f'{newspaper}-{year}-issues.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    logger.info(f'Compressing {len(issues)} JSON files into {filepath}')

    # put a file lock to avoid the overwriting of files due to parallelization
    lock = FileLock(filepath + ".lock", timeout=13)
    items = [issue.issue_data for issue in issues]
    try:
        with lock:
            with smart_open_function(filepath, 'ab') as fout:
                writer = jsonlines.Writer(fout)

                #items = [issue.issue_data for issue in issues]
                writer.write_all(items)

                logger.info(f'Written {len(items)} issues to {filepath}')
                writer.close()
    except Exception as e:
        logger.error(f"Error for {filepath}")
        logger.exception(e)
        write_error(filepath, e, failed_log)

    # Once the issues were written without issues, add their info to the manifest
    for i in items:
        manifest.add_by_title_year(newspaper, year, counts_for_canonical_issue(i))

    return f'{newspaper}-{year}', filepath, manifest


def upload_issues(
    sort_key: str,
    filepath: str,
    bucket_name: str | None = None
) -> Tuple[bool, str]:
    """Upload an issues JSON-line file to a given S3 bucket.

    `sort_key` is expected to be the concatenation of newspaper ID and year.

    Args:
        sort_key (str): Key used to group articles (e.g. "GDL-1900").
        filepath (str): Path of the file to upload to S3.
        bucket_name (str | None, optional): Name of S3 bucket where to upload
            the file. Defaults to None.

    Returns:
        Tuple[bool, str]: Whether the upload was successful and the path to the
            uploaded file.
    """
    # create connection with bucket
    # copy contents to s3 key
    newspaper, year = sort_key.split('-')
    key_name = "{}/{}/{}".format(
        newspaper,
        "issues",
        os.path.basename(filepath)
    )
    s3 = get_s3_resource()
    if bucket_name is not None:
        try:
            bucket = s3.Bucket(bucket_name)
            bucket.upload_file(filepath, key_name)
            logger.info(f'Uploaded {filepath} to {key_name}')
            return True, filepath
        except Exception as e:
            logger.error(f'The upload of {filepath} failed with error {e}')
    else:
        logger.info(f'Bucket name is None, not uploading issue {filepath}.')
    return False, filepath


def upload_pages( 
    sort_key: str,
    filepath: str,
    bucket_name: str | None = None
) -> Tuple[bool, str]:
    """Upload a page JSON file to a given S3 bucket.

    Args:
        sort_key (str): the key used to group articles (e.g. "GDL-1900").
        filepath (str): Path of the file to upload to S3.
        bucket_name (str | None, optional): Name of S3 bucket where to upload
            the file. Defaults to None.

    Returns:
        Tuple[bool, str]: Whether the upload was successful and the path to the
            uploaded file.
    """
    # create connection with bucket
    # copy contents to s3 key
    newspaper, year, month, day, edition = sort_key.split('-')
    key_name = "{}/pages/{}/{}".format(
        newspaper,
        f'{newspaper}-{year}',
        os.path.basename(filepath)
    )
    s3 = get_s3_resource()
    if bucket_name is not None:
        try:
            bucket = s3.Bucket(bucket_name)
            bucket.upload_file(filepath, key_name)
            logger.info(f'Uploaded {filepath} to {key_name}')
            return True, filepath
        except Exception as e:
            logger.error(f'The upload of {filepath} failed with error {e}')
    else:
        logger.info(f'Bucket name is None, not uploading page {filepath}.')
    return False, filepath


def remove_filelocks(output_dir: str) -> None:
    """Remove all files ending with .lock in a directory.

    Args:
        output_dir (str): Path to directory containing file locks.
    """
    files = os.listdir(output_dir)

    for file in files:
        if file.endswith(".lock"):
            os.remove(os.path.join(output_dir, file))
