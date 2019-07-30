"""A bunch of helper functions."""

import codecs
import logging
import os
from pathlib import Path

import boto
import boto.s3.connection
from boto.s3.key import Key

from impresso_commons.path.path_fs import canonical_path

logger = logging.getLogger(__name__)


# TODO: from impresso_commons.utils.s3 import get_s3_connection
def get_s3_connection(host="os.zhdk.cloud.switch.ch"):
    """Create a connection to impresso's S3 drive.

    Assumes that two environment variables are set: `SE_ACCESS_KEY` and
        `SE_SECRET_KEY`.
    """
    try:
        access_key = os.environ["SE_ACCESS_KEY"]
    except Exception:
        raise
    
    try:
        secret_key = os.environ["SE_SECRET_KEY"]
    except Exception:
        raise
    
    return boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            host=host,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            )


def serialize_page(page_number, page, issue_dir, out_dir=None, s3_bucket=None):
    # write the json page to file
    
    canonical_filename = canonical_path(
            issue_dir,
            "p" + str(page_number).zfill(4),
            ".json"
            )
    
    if out_dir is not None and s3_bucket is None:
        out_file = os.path.join(out_dir, canonical_filename)
        
        with codecs.open(out_file, 'w', 'utf-8') as f:
            f.write(page.serialize(ensure_ascii=False))
            logger.info(
                    "Written page \'{}\' to {}".format(page_number, out_file)
                    )
    
    elif s3_bucket is not None and out_dir is None:
        s3_connection = get_s3_connection()
        bucket_names = [b.name for b in s3_connection.get_all_buckets()]
        
        if s3_bucket not in bucket_names:
            bucket = s3_connection.create_bucket(s3_bucket)
        else:
            bucket = s3_connection.get_bucket(s3_bucket)
        
        assert bucket is not None
        
        k = Key(bucket)
        k.key = os.path.join(
                canonical_path(issue_dir, path_type="dir"),
                canonical_filename
                )
        k.set_contents_from_string(page.serialize())
        s3_connection.close()
        logger.info("Written output to s3 (bucket={}, key={})".format(
                bucket.name,
                k.key
                ))
    
    else:
        raise Exception


def serialize_issue(issue, issue_dir, out_dir=None, s3_bucket=None):
    # write the json page to file
    
    canonical_filename = canonical_path(issue_dir, "issue", extension=".json")
    
    if out_dir is not None and s3_bucket is None:
        Path(out_dir).mkdir(exist_ok=True)
        out_file = os.path.join(
                out_dir,
                canonical_filename
                )
        
        with codecs.open(out_file, 'w', 'utf-8') as f:
            f.write(issue.serialize())
            logger.info(
                    "Written issue info file to {}".format(out_file)
                    )
    
    elif s3_bucket is not None and out_dir is None:
        s3_connection = get_s3_connection()
        bucket_names = [b.name for b in s3_connection.get_all_buckets()]
        
        if s3_bucket not in bucket_names:
            bucket = s3_connection.create_bucket(s3_bucket)
        else:
            bucket = s3_connection.get_bucket(s3_bucket)
        
        assert bucket is not None
        
        k = Key(bucket)
        k.key = os.path.join(
                canonical_path(issue_dir, path_type="dir"),
                canonical_filename
                )
        k.set_contents_from_string(issue.serialize())
        s3_connection.close()
        logger.info("Written output to s3 (bucket={}, key={})".format(
                bucket.name,
                k.key
                ))
    
    else:
        raise Exception


def normalize_language(language):
    mappings = {
            "french": "fr",
            "english": "en",
            "german": "de"
            }
    return mappings[language.lower()]


