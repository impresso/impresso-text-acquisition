"""Core functions to perform large-scale import of OCR data.

Most of the functions in this module are meant to be used in conjuction
with `Dask <https://dask.org/>`_, the library we are using to parallelize
the ingestion process and run it on distributed computing infrastructures.

.. note::

    The function :func:`import_issues` is the most important in this module
    as it keeps everything together, by calling all other functions.
"""

import codecs
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
from typing import List, Optional, Tuple, Type

import jsonlines
from dask import bag as db
from impresso_commons.path.path_fs import IssueDir, canonical_path
from impresso_commons.text.rebuilder import cleanup
from impresso_commons.utils import chunk
from impresso_commons.utils.s3 import get_s3_resource
from smart_open import open as smart_open_function

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.olive.classes import OliveNewspaperIssue
from text_importer.importers.swa.classes import SWANewspaperIssue

logger = logging.getLogger(__name__)


def write_error(thing, error, failed_log):
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
        failed_log=None,
        image_dirs=None, temp_dir=None) -> Optional[NewspaperIssue]:
    """Instantiate a ``NewspaperIssue`` instance from an ``IssueDir``."""
    try:
        if issue_class is OliveNewspaperIssue:
            np_issue = OliveNewspaperIssue(
                    issue,
                    image_dirs=image_dirs,
                    temp_dir=temp_dir
                    )
        elif issue_class is SWANewspaperIssue:
            np_issue = SWANewspaperIssue(issue, temp_dir=temp_dir)
        else:
            np_issue = issue_class(issue)
        return np_issue
    except Exception as e:
        write_error(issue, e, failed_log)
        return None


def dirs2issues(
        issues: List[IssueDir],
        issue_class: Type[NewspaperIssue],
        failed_log=None,
        image_dirs=None, temp_dir=None) -> List[NewspaperIssue]:
    ret = []
    for issue in issues:
        np_issue = dir2issue(issue, issue_class, failed_log, image_dirs, temp_dir)
        if np_issue is not None:
            ret.append(np_issue)
    return ret


def issue2pages(issue: NewspaperIssue) -> List[NewspaperPage]:
    """Flatten a list of issues into  a list of their pages.

    As an issue consists of several pagaes, this function is useful
    in order to process each page in a truly parallel fashion.
    """
    pages = []
    for page in issue.pages:
        page.add_issue(issue)
        pages.append(page)
    return pages


def serialize_pages(
        pages: List[NewspaperPage],
        output_dir: str = None
        ) -> List[Tuple[IssueDir, str]]:
    """Serialise a list of pages to an output directory.

    :param List[NewspaperPage] pages: Input newspaper pages.
    :param str output_dir: Path to the output directory.
    :return: A list of tuples, where each tuple contains ``[0]`` the
        ``IssueDir`` object representing the issue to which pages belong
        and ``[1]`` the path to the individual page JSON file.
    :rtype: List[Tuple[IssueDir, str]]

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

        with codecs.open(out_file, 'w', 'utf-8') as jsonfile:
            json.dump(page.page_data, jsonfile)
            logger.info(
                    "Written page \'{}\' to {}".format(page.number, out_file)
                    )
        result.append((issue_dir, out_file))

    # TODO: this can be deleyted, I believe as it has no effect
    gc.collect()
    return result


def process_pages(pages: List[NewspaperPage], failed_log: str) -> List[NewspaperPage]:
    """Given a list of pages, trigger the ``.parse()`` method of each page.

    :param List[NewspaperPage] pages: Input newspaper pages.
    :param str failed_log: File path of failed log
    :return: A list of processed pages.
    :rtype: List[NewspaperPage]

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
        issues: List[IssueDir],
        out_dir: str,
        s3_bucket: Optional[str],
        issue_class: Type[NewspaperIssue],
        image_dirs: Optional[str],
        temp_dir: Optional[str],
        chunk_size: Optional[int]):
    """Import a bunch of newspaper issues.

    :param list issues: Description of parameter `issues`.
    :param str out_dir: Description of parameter `out_dir`.
    :param str s3_bucket: Description of parameter `s3_bucket`.
    :param issue_class: The newspaper issue class to import
        (Child of ``NewspaperIssue``).
    :param image_dirs: Directory of images (can be multiple)
    :param temp_dir: Temporary directory for extracting archives
        (applies only to importers make use of ``ZipArchive``).
    :param int chunk_size: The chunk size in years
    :return: Description of returned object.
    :rtype: tuple

    """

    msg = f'Issues to import: {len(issues)}'
    logger.info(msg)
    failed_log_path = os.path.join(
            out_dir,
            f'failed-{strftime("%Y-%m-%d-%H-%M-%S")}.log'
            )
    if chunk_size is not None:
        csize = int(chunk_size)
        chunks = groupby(sorted(issues, key=lambda x: x.date.year), lambda x: x.date.year - (x.date.year % csize))

        chunks = [(year, list(issues)) for year, issues in chunks]
        logger.info(f"Dividing issues into chunks of {chunk_size} years ({len(chunks)} chunks in total)")
        for year, issue_chunk in chunks:
            logger.info(f"Chunk of period {year} - {year + csize - 1} covers {len(issue_chunk)} issues")
    else:
        chunks = [(None, issues)]

    for year, issue_chunk in chunks:
        if year is None:
            period = 'all years'
        else:
            period = f'{year} - {year + csize - 1}'
            logger.info(f"Processing chunk of period {period}")

        temp_issue_bag = db.from_sequence(issue_chunk, partition_size=200)

        issue_bag = temp_issue_bag.map_partitions(
                dirs2issues,
                issue_class=issue_class,
                failed_log=failed_log_path,
                image_dirs=image_dirs,
                temp_dir=temp_dir).persist()

        logger.info(f'Start compressing and uploading issues for {period}')
        issue_bag.groupby(lambda i: (i.journal, i.date.year)) \
            .starmap(compress_issues, output_dir=out_dir) \
            .starmap(upload_issues, bucket_name=s3_bucket) \
            .starmap(cleanup_fix) \
            .compute()

        logger.info(f'Done compressing and uploading issues for {period}')

        processed_issues = list(issue_bag)
        random.shuffle(processed_issues)

        chunks = chunk(processed_issues, 400)

        for chunk_n, chunk_of_issues in enumerate(chunks):
            logger.info(f'Processing chunk {chunk_n} of pages for {period}')

            pages_bag = db.from_sequence(chunk_of_issues, partition_size=2) \
                .map(issue2pages) \
                .flatten() \
                .map_partitions(process_pages, failed_log=failed_log_path) \
                .map_partitions(serialize_pages, output_dir=out_dir)

            pages_out_dir = os.path.join(out_dir, 'pages')
            Path(pages_out_dir).mkdir(exist_ok=True)

            logger.info(f'Start compressing and uploading pages for {period}')
            pages_bag = pages_bag.groupby(
                    lambda x: canonical_path(
                            x[0], path_type='dir'
                            ).replace('/', '-')
                    ) \
                .starmap(compress_pages, prefix='pages', output_dir=pages_out_dir) \
                .starmap(upload_pages, bucket_name=s3_bucket) \
                .starmap(cleanup) \
                .compute()

            logger.info(f'Done compressing and uploading pages for {period}')

        del issue_bag

    if temp_dir is not None and os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    logger.info("---------- Done ----------")

def cleanup_fix(upload_success, filepath):

    """Removes a file if it has been successfully uploaded to S3.
    :param upload_success: whether the upload was successful
    :type upload_success: bool
    :param filepath: path to the uploaded file
    :type filepath: str
    """
    try:
        if upload_success:
            os.remove(filepath)
            logger.info(f'Removed temporary file {filepath}')
        else:
            logger.info(f'Not removing {filepath} as upload has failed')
    except FileNotFoundError:
        logger.warning(f'FileNotFoundError for {filepath}')



def compress_pages(
        key: str,
        json_files: List,
        output_dir: str,
        prefix: str = ""
        ) -> Tuple[str, str]:
    """Merges a set of JSON line files into a single compressed archive.

    :param key: signature of the newspaper issue (e.g. GDL-1900)
    :param json_files: input JSON line files
    :param output_dir: directory where to write the output file
    :param prefix:
    :return: a tuple with: sorting key [0] and path to serialized file [1].

    .. note::

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
    """

    newspaper, year, month, day, edition = key.split('-')
    prefix_string = "" if prefix == "" else f"-{prefix}"
    filename = (
        f'{newspaper}-{year}-{month}-{day}-{edition}'
        f'{prefix_string}.jsonl.bz2'
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
                    logger.error(
                            f'Reading data from {json_file} failed'
                            )
                    logger.exception(e)
            logger.info(
                    f'Written {items_count} docs from {json_file} to {filepath}'
                    )

        writer.close()

    return key, filepath


def compress_issues(
        key: Tuple[str, int],
        issues: List[NewspaperIssue],
        output_dir: str = None
        ) -> Tuple[str, str]:
    """This function compresses a list of issues belonging to the same Journal-year, and saves them in the output directory
    First, it will check if the file already exists, loads it and then over-writes/adds the newly generated issues.

    :param tuple key: Tuple with newspaper ID and year of input issues
        (e.g. ``(GDL, 1900)``).
    :param list issues: A list of `NewspaperIssue` instances.
    :param type output_dir: Description of parameter `output_dir`.
    :return: a tuple with: ``tuple[0]`` a label following the template
        ``<NEWSPAPER>-<YEAR>`` and ``tuple[1]`` the path to the the compressed
        ``.bz2``file containing the input issues as separate documents in
         a JSON-line file.
    :rtype: tuple
    """
    newspaper, year = key
    filename = f'{newspaper}-{year}-issues.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    logger.info(f'Compressing {len(issues)} JSON files into {filepath}')

    try:
        to_dump = set(issue.id for issue in issues)
        to_keep = []
        if os.path.exists(filepath) and os.path.isfile(filepath):
            with smart_open_function(filepath, 'rb') as f:
                reader = jsonlines.Reader(f)
                to_keep = []
                for issue in reader:
                    logger.info(issue.keys())
                    if 'id' in issue and issue['id'] not in to_dump:
                        to_keep.append(issue)
                reader.close()

        with smart_open_function(filepath, 'wb') as fout:
            writer = jsonlines.Writer(fout)

            items = [issue.issue_data for issue in issues] + to_keep  # Add the new ones/to overwrite

            writer.write_all(items)
            logger.info(
                    f'Written {len(items)} issues to {filepath}'
                    )
            writer.close()
    except Exception as e:
        logger.error(f"Error for {filepath}")
        logger.exception(e)

    return f'{newspaper}-{year}', filepath


def upload_issues(
        sort_key: str,
        filepath: str,
        bucket_name: str = None
        ) -> Tuple[bool, str]:
    """Upload a file to a given S3 bucket.

    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :param filepath: path of the file to upload to S3
    :param bucket_name: name of S3 bucket where to upload the file
    :return: a tuple with [0] whether the upload was successful (boolean) and
        [1] the path to the uploaded file.
    :rtype: Tuple[bool, str]

    .. note::

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).

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
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(filepath, key_name)
        logger.info(f'Uploaded {filepath} to {key_name}')
        return True, filepath
    except Exception as e:
        logger.error(e)
        logger.error(f'The upload of {filepath} failed with error {e}')
        return False, filepath


def upload_pages(
        sort_key: str,
        filepath: str,
        bucket_name: str = None
        ) -> Tuple[bool, str]:
    """Upload a page JSON file to a given S3 bucket.

    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :param filepath: path of the file to upload to S3
    :param bucket_name: name of S3 bucket where to upload the file
    :return: a tuple with [0] whether the upload was successful (boolean) and
        [1] the path of the uploaded file (string)
    :rtype: Tuple[bool, str]

    .. note::

        ``sort_key`` is expected to be the concatenation of newspaper ID and
        year (e.g. GDL-1900).

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
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(filepath, key_name)
        logger.info(f'Uploaded {filepath} to {key_name}')
        return True, filepath
    except Exception as e:
        logger.error(f'The upload of {filepath} failed with error {e}')
        return False, filepath
