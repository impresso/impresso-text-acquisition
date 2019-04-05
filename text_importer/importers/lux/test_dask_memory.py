#!/usr/bin/env python
# coding: utf-8

import codecs
import gc
import json
import os

import jsonlines
from dask import bag as db
from dask.distributed import Client, progress
from impresso_commons.path.path_fs import canonical_path
from impresso_commons.text.rebuilder import cleanup, upload
from impresso_commons.utils import chunk
from impresso_commons.utils.s3 import get_s3_resource
from smart_open import smart_open

from memory_profiler import profile
from text_importer.importers.lux.classes import LuxNewspaperIssue
from text_importer.importers.lux.core import (compress_issues, issue2pages,
                                              mets2issue, process_page,
                                              serialize_page, upload_issues)
from text_importer.importers.lux.detect import \
    detect_issues as lux_detect_issues


def serialize_pages(pages, output_dir=None):

    result = []

    for luxpage in pages:

        issue_dir = luxpage.issue.issuedir

        out_dir = os.path.join(
            output_dir,
            canonical_path(issue_dir, path_type="dir")
        )

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        canonical_filename = canonical_path(
            issue_dir,
            "p" + str(luxpage.number).zfill(4),
            ".json"
        )

        out_file = os.path.join(out_dir, canonical_filename)

        with codecs.open(out_file, 'w', 'utf-8') as jsonfile:
            json.dump(luxpage.data, jsonfile)
            print(
                "Written page \'{}\' to {}".format(luxpage.number, out_file)
            )
        result.append((issue_dir, out_file))

    del pages

    gc.collect()
    return result


def process_pages(pages):
    result = []
    for page in pages:
        try:
            page.parse()
            result.append(page)
        except Exception as e:
            logger.error(f'Error when processing page {page}')
            logger.exception(e)
    return result


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

        items = []
        for issue, json_file in json_files:

            with open(json_file, 'r') as inpf:
                item = json.load(inpf)
                items.append(item)

        writer.write_all(items)
        print(
            f'Written {len(items)} docs from {json_file} to {filepath}'
        )

        writer.close()

    return (key, filepath)


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
    key_name = "{}/{}/{}".format(
        newspaper,
        f'{newspaper}-{year}',
        os.path.basename(filepath)
    )
    s3 = get_s3_resource()
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(filepath, key_name)
        print(f'Uploaded {filepath} to {key_name}')
        return True, filepath
    except Exception as e:
        #logger.error(e)
        #logger.error(f'The upload of {filepath} failed with error {e}')
        print(f'The upload of {filepath} failed with error {e}')
        return False, filepath

@profile
def main():

    #client = Client(n_workers=48)
    client = Client('localhost:8786')
    print(client)
    input_dir = "/mnt/project_impresso/original/BNL/"
    out_dir = "/scratch/matteo/impresso-canonical/BNL/"
    s3_bucket = 'original-canonical-data'
    issues = lux_detect_issues(input_dir)
    selected_issues = db.from_sequence(issues)\
        .filter(lambda i: i.journal == 'courriergdl').compute()
    chunks = chunk(selected_issues, 1000)

    for chunk_n, chunk_of_issues in enumerate(chunks):

        print(f'Processing chunk {chunk_n}')

        issue_bag =  db.from_sequence(chunk_of_issues)\
                .map(mets2issue)\
                .filter(lambda i: i is not None)\
                .persist()


        progress(issue_bag)

        pages_bag = issue_bag\
            .map(issue2pages)\
            .flatten()\
            .persist()

        progress(pages_bag)
        print(f'Pages to process: {pages_bag.count().compute()}\n')

        pages_bag = pages_bag\
            .repartition(1000)\
            .map_partitions(process_pages)\
            .map_partitions(serialize_pages, output_dir=out_dir)\
            .persist()

        progress(pages_bag)

        print('Now compress and upload pages')
        pages_bag = pages_bag.groupby(
                lambda x: canonical_path(
                    x[0], path_type='dir'
                ).replace('/', '-')
            )\
            .starmap(compress_pages, prefix='pages', output_dir=out_dir)\
            .starmap(upload_pages, bucket_name='original-canonical-data')\
            .starmap.cleanup()\
            .persist()

        progress(pages_bag)

        print(gc.collect())

main()
