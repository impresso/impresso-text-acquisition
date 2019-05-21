#!/usr/bin/env python
# coding: utf-8

from impresso_commons.path.path_fs import (KNOWN_JOURNALS,
                                           detect_canonical_issues)
import dask.bag as db
import jsonlines
from dask.distributed import Client, progress
import os
from smart_open import smart_open
from impresso_commons.text.rebuilder import cleanup
from impresso_commons.utils.s3 import get_s3_resource


def find_issue_files(key, issues):
    issue_files = []
    for issue in issues:
        basedir = issue.path
        try:
            filename = [
                file
                for file in os.listdir(basedir)
                if 'issue.json' in file
            ][0]
            issue_files.append(os.path.join(basedir, filename))
        except Exception:
            pass
    return (key, issue_files)


def find_page_files(key, issues):
    page_files = []
    for issue in issues:
        basedir = issue.path
        try:
            page_filenames = [
                os.path.join(basedir, file)
                for file in os.listdir(basedir)
                if '-p' in file
            ]
            page_files += page_filenames
        except:
            pass
    return (key, page_files)


# TODO: move this function to the codebase, it's generic enough!
def compress_pages(key, json_files, output_dir, prefix=""):
    """Merges a set of JSON line files into a single compressed archive.

    :param key: signature of the newspaper issue (e.g. GDL-1900)
    :type key: str
    :param json_files: input JSON line files
    :type json_files: list
    :param output_dir: directory where to write the output file
    :type outp_dir: str
    :return: a tuple with: sorting key [0] and path to serialized file [1].
    :rytpe: tuple

    .. note::

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
    """
    newspaper, year, month, day, edition = key.split('-')
    prefix_string = "" if prefix == "" else f"-{prefix}"
    filename = f'{newspaper}-{year}-{month}-{day}-{edition}{prefix_string}.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    print(f'Compressing {len(json_files)} JSON files into {filepath}')

    with smart_open(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)

        for json_file in json_files:
            with open(json_file, 'r') as inpf:
                reader = jsonlines.Reader(inpf)
                items = list(reader)
                writer.write_all(items)
            print(
                f'Written {len(items)} docs from {json_file} to {filepath}'
            )

        writer.close()

    return (key, filepath)
    print(len(json_files))


def upload_issues(sort_key, filepath, bucket_name=None):
    """Uploads a file to a given S3 bucket.
    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :type sort_key: str
    :param filepath: path of the file to upload to S3
    :type filepath: str
    :param bucket_name: name of S3 bucket where to upload the file
    :type bucket_name: str
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
        # logger.info(f'Uploaded {filepath} to {key_name}')
        return True, filepath
    except Exception as e:
        # logger.error(e)
        # logger.error(f'The upload of {filepath} failed with error {e}')
        return False, filepath


def upload_pages(sort_key, filepath, bucket_name=None):
    """Uploads a file to a given S3 bucket.
    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :type sort_key: str
    :param filepath: path of the file to upload to S3
    :type filepath: str
    :param bucket_name: name of S3 bucket where to upload the file
    :type bucket_name: str
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
        #logger.info(f'Uploaded {filepath} to {key_name}')
        return True, filepath
    except Exception as e:
        #logger.error(e)
        #logger.error(f'The upload of {filepath} failed with error {e}')
        return False, filepath


# TODO: move this function to the codebase, it's generic enough!
def compress(key, json_files, output_dir, prefix=""):
    """Merges a set of JSON line files into a single compressed archive.

    :param key: signature of the newspaper issue (e.g. GDL-1900)
    :type key: str
    :param json_files: input JSON line files
    :type json_files: list
    :param output_dir: directory where to write the output file
    :type outp_dir: str
    :return: a tuple with: sorting key [0] and path to serialized file [1].
    :rytpe: tuple

    .. note::

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
    """
    newspaper, year = key.split('-')
    prefix_string = "" if prefix == "" else f"-{prefix}"
    filename = f'{newspaper}-{year}{prefix_string}.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    print(f'Compressing {len(json_files)} JSON files into {filepath}')

    with smart_open(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)

        for json_file in json_files:
            with open(json_file, 'r') as inpf:
                reader = jsonlines.Reader(inpf)
                items = list(reader)
                writer.write_all(items)
            print(
                f'Written {len(items)} docs from {json_file} to {filepath}'
            )

        writer.close()

    return (key, filepath)
    print(len(json_files))


input_dir = "/scratch/matteo/impresso-canonical-compressed"
outp_dir = '/scratch/matteo/impresso-compressed'
s3_bucket = None

local_issues = detect_canonical_issues(
        input_dir,
        KNOWN_JOURNALS
)


dask_client = Client('localhost:8786')
# dask_client = Client()
issue_bag = db.from_sequence(local_issues)

grouped_bag = issue_bag.groupby(lambda i: f'{i.journal}-{i.date}-{i.edition}')

# .starmap(cleanup).persist()
result = grouped_bag.starmap(find_page_files)\
    .starmap(
        compress_pages,
        prefix="pages",
        output_dir=os.path.join(outp_dir, 'pages/')
    )\
    .starmap(upload_pages, bucket_name=s3_bucket)\
    .persist()

progress(result)


issue_bag = db.from_sequence(local_issues)
grouped_bag = issue_bag.groupby(
    lambda issue: f'{issue.journal}-{issue.date.year}'
)

grouped_bag.starmap(find_issue_files)\
    .starmap(
        compress,
        prefix="issues",
        output_dir=os.path.join(outp_dir, 'issues/')
    )\
    .starmap(upload_issues, bucket_name=s3_bucket)\
    .compute()
