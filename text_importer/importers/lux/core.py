"""Importer for the newspapers data of the Luxembourg National Library"""

import codecs
import json
import logging
import os

import jsonlines
from dask import bag as db
from dask.distributed import Client, progress
from impresso_commons.path.path_fs import canonical_path
from impresso_commons.utils.s3 import get_s3_resource
from smart_open import smart_open as smart_open_function

from text_importer.importers.lux.classes import LuxNewspaperIssue

# from text_importer.helpers import get_issue_schema, serialize_issue

logger = logging.getLogger(__name__)


def compress_issues(key, issues, output_dir=None):
    """Short summary.

    :param type key: Description of parameter `key`.
    :param list issues: A list of `LuxNewspaperIssue` instances.
    :param type output_dir: Description of parameter `output_dir`.
    :return: TODO: Description of returned object.
    :rtype: tuple

    """
    newspaper, year = key
    filename = f'{newspaper}-{year}-issues.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    logger.info(f'Compressing {len(issues)} JSON files into {filepath}')

    with smart_open_function(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)
        items = [
            issue._issue_data
            for issue in issues
        ]
        writer.write_all(items)
        print(
            f'Written {len(items)} docs from to {filepath}'
        )
        writer.close()

    return (f'{newspaper}-{year}', filepath)


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
        logger.error(e)
        logger.error(f'The upload of {filepath} failed with error {e}')
        return False, filepath


def mets2issue(issue):
    """Instantiates a LuxNewspaperIssue instance from an IssueDir."""
    try:
        return LuxNewspaperIssue(issue)
    except Exception as e:
        logger.error(f'Error when processing issue {issue}')
        logger.exception(e)
        return None


def issue2pages(issue):
    pages = []
    for page in issue.pages:
        page.add_issue(issue)
        pages.append(page)
    return pages


def serialize_page(luxpage, output_dir=None):

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
    del luxpage
    return (issue_dir, out_file)


def process_page(page):
    try:
        page.parse()
        return page
    except Exception as e:
        logger.error(f'Error when processing page {page}')
        logger.exception(e)
        return None


def import_issues(issues, out_dir, s3_bucket):
    """Imports a bunch of BNL newspaper issues in Mets/Alto format.

    :param list issues: Description of parameter `issues`.
    :param str out_dir: Description of parameter `out_dir`.
    :param str s3_bucket: Description of parameter `s3_bucket`.
    :return: Description of returned object.
    :rtype: tuple

    """

    issue_bag = db.from_sequence(issues)
    logger.info(f'Issues to import: {issue_bag.count().compute()}')

    # .repartition(1000)
    # .filter(lambda i: i.journal == 'luxzeit1858')
    issue_bag =  issue_bag.filter(lambda i: i.journal == 'luxzeit1858')\
        .map(mets2issue)\
        .filter(lambda i: i is not None)\
        .persist()

    # .starmap(upload_issues, bucket_name=s3_bucket)\
    result = issue_bag.groupby(lambda i: (i.journal, i.date.year))\
        .starmap(compress_issues, output_dir=out_dir)\
        .starmap(upload_issues, bucket_name=s3_bucket)\
        .compute()

    pages_bag = issue_bag\
        .map(issue2pages)\
        .flatten()\
        .persist()

    print(f'Pages to process: {pages_bag.count().compute()}')

    result = pages_bag\
        .repartition(1000)\
        .map(process_page)\
        .map(serialize_page, output_dir=out_dir)\
        .persist()
    progress(result)
    return result
