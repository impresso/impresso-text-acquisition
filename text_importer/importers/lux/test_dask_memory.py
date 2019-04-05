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


@profile
def main():

    client = Client(n_workers=48)
    print(client)
    input_dir = "/mnt/project_impresso/original/BNL/"
    out_dir = "/scratch/matteo/impresso-canonical/BNL/"
    s3_bucket = 'original-canonical-data'
    issues = lux_detect_issues(input_dir)
    issue_bag = db.from_sequence(issues[:1000])
    issue_bag =  issue_bag.filter(lambda i: i.journal == 'courriergdl')\
            .map(mets2issue)\
            .filter(lambda i: i is not None)\
            .persist()


    progress(issue_bag)

    pages_bag = issue_bag\
        .map(issue2pages)\
        .flatten()\
        .persist()

    progress(pages_bag)
    print(f'Pages to process: {pages_bag.count().compute()}')

    pages_bag = pages_bag\
        .repartition(1000)\
        .map_partitions(process_pages)\
        .map_partitions(serialize_pages, output_dir=out_dir)\
        .persist()

    progress(pages_bag)
    print(gc.collect())

main()
import ipdb; ipdb.set_trace()
