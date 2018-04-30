"""
CLI script to upload impresso canonical data to an S3 drive.

Usage:
    impresso-txt-uploader --input-dir=<id> --log-file=<f> --s3-bucket=<b> [--overwrite]

Options:
    --input-dir=<id>    Base directory containing one sub-directory for each journal
    --s3-bucket=<b>     If provided, writes output to an S3 drive, in the specified bucket
    --log-file=<f>      Log file; when missing print log to stdout
    --overwrite         Overwrite files on S3 if already present
"""  # noqa: E501

import logging
import os
import ipdb as pdb
import pickle

from docopt import docopt

from boto.s3.connection import Key
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get as mp_get
from impresso_commons.path import (KNOWN_JOURNALS, detect_canonical_issues)
from impresso_commons.utils.s3 import get_s3_connection

logger = logging.getLogger(__name__)


def s3_upload_issue(local_issue, input_dir, ouput_bucket, overwrite=False):
    """Upload a canonical newspaper issue to an S3 bucket.

    :param local_issue: the issue to upload
    :type local_issue: an instance of `IssueDir`
    :param output_bucket: the target bucket
    :type output_bucket: `boto.s3.connection.Bucket`
    :return: a list of tuples `t` where `t[0]` contains the issue,
        and `t[1]` is a boolean indicating whether the upload was
        successful or not.
    """
    my_dir = local_issue.path
    files = [os.path.join(my_dir, f) for f in os.listdir(my_dir)]
    try:
        for f in files:
            k = Key(ouput_bucket)
            # remove the input_dir when setting the key's name
            k.key = f.replace(input_dir, "")

            if not overwrite and k.exists() is True:
                logger.info(
                    f'Skipping: {f} file present and overwrite == {overwrite}'
                )
            else:
                # copy the content of the file into the key
                k.set_contents_from_filename(f)
                logger.info(
                    f'Uploaded {f} to s3://{ouput_bucket.name}/{k.key}'
                )

            k.close()
        return (local_issue, True)
    except Exception as e:
        logger.error(f'Failed uploading {local_issue} with error = {e}')
        return (local_issue, False)


def main():
    args = docopt(__doc__)
    input_dir = args["--input-dir"]
    bucket_name = args["--s3-bucket"]
    log_file = args["--log-file"]
    overwrite = False if args["--overwrite"] is None else args["--overwrite"]

    # fetch the s3 bucket
    conn = get_s3_connection()
    bucket = [
        bucket
        for bucket in conn.get_all_buckets()
        if bucket.name == bucket_name
    ][0]

    # configure logger
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(filename=log_file, mode='w')
    logger.addHandler(handler)

    # gather issues to upload
    local_issues = detect_canonical_issues(
        input_dir,
        KNOWN_JOURNALS
    )
    print(f"Starting import of {len(local_issues)} issues")

    tasks = [
        delayed(s3_upload_issue)(l, input_dir, bucket, overwrite=overwrite)
        for l in local_issues
    ]

    with ProgressBar():
        result = compute(*tasks, get=mp_get)

    errors = [
        issue
        for issue, success in result
        if not success
    ]

    try:
        assert len(errors) == 0
    except AssertionError:
        logger.error(f"Upload of {len(errors)} failed (see pikcle file)")
        with open('./failed_s3_uploads.pkl', 'wb') as pickle_file:
            pickle.dump(
                [i.path for i in errors],
                pickle_file
            )


if __name__ == '__main__':
    main()
