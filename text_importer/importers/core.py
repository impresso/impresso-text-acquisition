import codecs
import gc
import json
import logging
import os
import random
from copy import copy
from json import JSONDecodeError
from pathlib import Path
from typing import List, Optional, Tuple, Type

import jsonlines
from dask import bag as db
from impresso_commons.path.path_fs import IssueDir, canonical_path
from impresso_commons.text.rebuilder import cleanup
from impresso_commons.utils import chunk
from impresso_commons.utils.s3 import get_s3_resource
from smart_open import smart_open as smart_open_function

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.olive.classes import OliveNewspaperIssue

logger = logging.getLogger(__name__)


def dir2issue(issue: IssueDir, issue_class: Type[NewspaperIssue], image_dirs=None, temp_dir=None) -> \
        Tuple[Optional[NewspaperIssue], Optional[str]]:
    """Instantiates a NewspaperIssue instance from an IssueDir."""
    try:
        if issue_class is OliveNewspaperIssue:
            np_issue = OliveNewspaperIssue(issue, image_dirs=image_dirs, temp_dir=temp_dir)
        else:
            np_issue = issue_class(issue)
        return np_issue, None
    except Exception as e:
        logger.error(f'Error when processing issue {issue}: {e}')
        logger.exception(e)
        note = f"{canonical_path(issue, path_type='dir').replace('/', '-')}: {e}"
        return None, note


def issue2pages(issue: NewspaperIssue) -> List[NewspaperPage]:
    """Processes the pages of the given issue"""
    pages = []
    for page in issue.pages:
        page.add_issue(issue)
        pages.append(page)
    return pages


def serialize_pages(pages: List[NewspaperPage], output_dir: str = None) -> List[Tuple[IssueDir, str]]:
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
    
    # del pages
    
    gc.collect()
    return result


def process_pages(pages: List[NewspaperPage]) -> List[NewspaperPage]:
    result = []
    for page in pages:
        try:
            page.parse()
            result.append(page)
        except Exception as e:
            logger.error(f'Error when processing page {page}: {e}')
            # logger.exception(e)
    return result


def import_issues(issues: List[IssueDir], out_dir: str, s3_bucket: str, issue_class: Type[NewspaperIssue],
                  image_dirs: List[str], temp_dir: str):
    """Imports a bunch of newspaper issues.

    :param list issues: Description of parameter `issues`.
    :param str out_dir: Description of parameter `out_dir`.
    :param str s3_bucket: Description of parameter `s3_bucket`.
    :param issue_class: The newspaper issue class to import (Child of NewspaperIssue)
    :param image_dirs: Directory of images (can be multiple)
    :param temp_dir: Temporary directory for extracting archives (For Olive format)
    :return: Description of returned object.
    :rtype: tuple

    """
    msg = f'Issues to import: {len(issues)}'
    logger.info(msg)
    
    issue_bag = db.from_sequence(issues, partition_size=60) \
        .map(dir2issue, issue_class=issue_class, image_dirs=image_dirs, temp_dir=temp_dir)
    
    failed_log = issue_bag.filter(lambda x: x[0] is None).map(lambda x: x[1]).persist()
    issue_bag = issue_bag.filter(lambda x: x[0] is not None).map(lambda x: x[0]).persist()
    
    logger.info('Start compressing and uploading issues')
    issue_bag.groupby(lambda i: (i.journal, i.date.year)) \
        .starmap(compress_issues, output_dir=out_dir) \
        .starmap(upload_issues, bucket_name=s3_bucket) \
        .starmap(cleanup) \
        .compute()
    logger.info('Done compressing and uploading')
    
    processed_issues = list(issue_bag)
    random.shuffle(processed_issues)
    
    chunks = chunk(processed_issues, 400)
    
    for chunk_n, chunk_of_issues in enumerate(chunks):
        logger.info(f'Processing chunk {chunk_n}')
        
        pages_bag = db.from_sequence(chunk_of_issues, partition_size=2) \
            .map(issue2pages) \
            .flatten() \
            .map_partitions(process_pages) \
            .map_partitions(serialize_pages, output_dir=out_dir)
        
        pages_out_dir = os.path.join(out_dir, 'pages')
        Path(pages_out_dir).mkdir(exist_ok=True)
        
        logger.info('Now compress and upload pages')
        pages_bag = pages_bag.groupby(
                lambda x: canonical_path(
                        x[0], path_type='dir'
                        ).replace('/', '-')
                ) \
            .starmap(compress_pages, prefix='pages', output_dir=pages_out_dir) \
            .starmap(upload_pages, bucket_name=s3_bucket) \
            .starmap(cleanup)
        
        pages_bag.compute()

    log_path = os.path.join(out_dir, 'failed.log')
    failed_log = failed_log.compute()
    
    with open(log_path, 'w') as f:
        f.writelines(failed_log)
        logger.info(f"Dumped {len(failed_log)} failed issues with errors in {log_path}")
    
    logger.info("---------- Done ----------")


def compress_pages(key: str, json_files: List, output_dir: str, prefix: str = "") -> Tuple[str, str]:
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
    filename = f'{newspaper}-{year}-{month}-{day}-{edition}{prefix_string}.jsonl.bz2'
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


def compress_issues(key: Tuple[str, int], issues: List[NewspaperIssue], output_dir: str = None) -> Tuple[str, str]:
    """Short summary.

    :param type key: Description of parameter `key`.
    :param list issues: A list of `NewspaperIssue` instances.
    :param type output_dir: Description of parameter `output_dir`.
    :return: a tuple with [0] being NEWSPAPER-YEAR and
        [1] the path of the compressed file
    """
    newspaper, year = key
    filename = f'{newspaper}-{year}-issues.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    logger.info(f'Compressing {len(issues)} JSON files into {filepath}')
    
    with smart_open_function(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)
        items = [
                issue.issue_data
                for issue in issues
                ]
        writer.write_all(items)
        logger.info(
                f'Written {len(items)} issues to {filepath}'
                )
        writer.close()
    
    return f'{newspaper}-{year}', filepath


def upload_issues(sort_key: str, filepath: str, bucket_name: str = None) -> Tuple[bool, str]:
    """Uploads a file to a given S3 bucket.
    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :param filepath: path of the file to upload to S3
    :param bucket_name: name of S3 bucket where to upload the file
    :return: a tuple with [0] whether the upload was successful (boolean) and
        [1] the path of the uploaded file (string)
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


def upload_pages(sort_key: str, filepath: str, bucket_name: str = None) -> Tuple[bool, str]:
    """Uploads a file to a given S3 bucket.
    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :param filepath: path of the file to upload to S3
    :param bucket_name: name of S3 bucket where to upload the file
    :return: a tuple with [0] whether the upload was successful (boolean) and
        [1] the path of the uploaded file (string)
    .. note::
        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
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
