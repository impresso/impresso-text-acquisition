"""Functions and CLI to rebuild text from impresso's canonical format.
For EPFL members, this script can be scaled by running it using Runai,
as documented on https://github.com/impresso/impresso-infrastructure/blob/main/howtos/runai.md.

Usage:
    rebuilder.py rebuild_articles --input-bucket=<b> --log-file=<f> --output-dir=<od> --filter-config=<fc> [--format=<fo> --scheduler=<sch> --output-bucket=<ob> --verbose --clear --languages=<lgs> --nworkers=<nw> --git-repo=<gr> --temp-dir=<tp> --prev-manifest=<pm>]

Options:

--input-bucket=<b>  S3 bucket where canonical JSON data will be read from
--output-bucket=<ob>  Rebuilt data will be uploaded to the specified s3 bucket (otherwise no upload)
--log-file=<f>  Path to log file
--scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
--filter-config=<fc>  A JSON configuration file specifying which newspaper issues will be rebuilt
--verbose  Set logging level to DEBUG (by default is INFO)
--clear  Remove output directory before and after rebuilding
--format=<fo>  Rebuilt format to use (can be "solr" or "passim")
--languages=<lgs>  Languages to filter the articles to rebuild on.
--nworkers=<nw>  number of workers for (local) Dask client.
--git-repo=<gr>   Local path to the "impresso-text-acquisition" git directory (including it).
--temp-dir=<tp>  Temporary directory in which to clone the impresso-data-release git repository.
--prev-manifest=<pm>  Optional S3 path to the previous manifest to use for the manifest generation.
"""  # noqa: E501

import sys
import traceback
import json
import pathlib
import logging
import os
import shutil
import signal
import git

import dask.bag as db
import jsonlines
from dask.distributed import Client, progress
from docopt import docopt
from smart_open import smart_open

from impresso_essentials.io.s3 import read_s3_issues, get_s3_resource
from impresso_essentials.utils import SourceMedium, SourceType, init_logger

from impresso_essentials.versioning.data_manifest import DataManifest
from impresso_essentials.versioning.aggregators import compute_stats_in_rebuilt_bag

from text_preparation.rebuilders.helpers import (
    read_issue_supports,
    rejoin_cis,
    ci_has_problem,
    ci_without_problem,
    rebuild_for_solr,
    rebuild_for_passim,
)

logger = logging.getLogger(__name__)


def compress(key, json_files, output_dir):
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

    alias, year = key.split("-")
    filename = f"{alias}-{year}.jsonl.bz2"
    filepath = os.path.join(output_dir, filename)
    logger.info("Compressing %s JSON files into %s", len(json_files), filepath)
    print(f"Compressing {len(json_files)} JSON files into {filepath}")

    with smart_open(filepath, "wb") as fout:
        writer = jsonlines.Writer(fout)

        for json_file in json_files:
            with open(json_file, "r", encoding="utf-8") as inpf:
                reader = jsonlines.Reader(inpf)
                cis = list(reader)
                writer.write_all(cis)
            logger.info("Written %s docs from %s to %s", len(cis), json_file, filepath)

        writer.close()

    for json_file in json_files:
        os.remove(json_file)

    temp_dir = os.path.dirname(json_files[0])
    os.rmdir(temp_dir)
    logger.info("Removed temporary directory and files in %s", temp_dir)

    return (key, filepath)


def upload(sort_key, filepath, bucket_name=None):
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

        `sort_key` is expected to be the concatenation of media title alias and year
        (e.g. GDL-1900).
    """
    # create connection with bucket
    # copy contents to s3 key
    alias, _ = sort_key.split("-")
    key_name = f"{alias}/{os.path.basename(filepath)}"
    if "/" in bucket_name:
        # if the provided bucket also contains a partition, add it to the key name
        bucket_name, partition = bucket_name.split("/")
        key_name = f"{partition}/{key_name}"
    s3 = get_s3_resource()
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(filepath, key_name)
        msg = f"Uploaded {filepath} to {key_name}"
        print(msg)
        logger.info(msg)
        return True, filepath
    except Exception as e:
        err_msg = f"The upload of {filepath} failed with error {e}"
        logger.error(err_msg)
        print(err_msg)
        return False, filepath


def cleanup(upload_success, filepath):
    """Removes a file if it has been successfully uploaded to S3.
    :param upload_success: whether the upload was successful
    :type upload_success: bool
    :param filepath: path to the uploaded file
    :type filepath: str
    """
    if upload_success and os.path.exists(filepath):
        try:
            os.remove(filepath)
            logger.info("Removed temporary file %s", filepath)
        except Exception as e:
            msg = f"Error {e} occurred when removing {filepath}"
            print(msg)
            logger.warning(msg)
    else:
        logger.info("Not removing %s as upload has failed", filepath)


def filter_and_process_cis(issues_bag, input_bucket, issue_medium, _format):
    # Process the issues into rebuilt CIs

    is_audio = issue_medium == "audio" if SourceMedium.has_value(issue_medium) else None

    # determine which rebuild function to apply
    if _format == "solr":
        rebuild_function = rebuild_for_solr
    elif _format == "passim":
        rebuild_function = rebuild_for_passim
    else:
        raise NotImplementedError

    support_property = "rr" if is_audio else "pp"
    faulty_issues = (
        issues_bag.filter(lambda i: len(i[1][support_property]) == 0)
        .map(lambda i: i[1])
        .pluck("id")
        .compute()
    )

    val_to_print = "audio record" if is_audio else "pages"
    msg = f"{len(faulty_issues)} Issues with no {val_to_print} (will be skipped): {faulty_issues}"
    logger.debug(msg)
    print(msg)
    del faulty_issues
    msg = f"Number of partitions: {issues_bag.npartitions}"
    logger.debug(msg)
    print(msg)

    cis_bag = (
        issues_bag.filter(lambda i: len(i[1][support_property]) > 0)
        .starmap(read_issue_supports, is_audio=is_audio, bucket=input_bucket)
        .starmap(rejoin_cis)
        .flatten()
        .persist()
    )

    faulty_cis_n = cis_bag.filter(ci_has_problem).pluck("m").pluck("id").compute()
    msg = f"Skipped content-items: {faulty_cis_n}"
    logger.debug(msg)
    print(msg)
    del faulty_cis_n

    cis_bag = cis_bag.filter(ci_without_problem).map(rebuild_function).persist()

    return cis_bag


def rebuild_issues(
    issues, input_bucket, output_dir, dask_client, _format="solr", filter_language=None
):
    """Rebuild a set of newspaper issues into a given format.

    :param issues: issues to rebuild
    :type issues: list of `IssueDir` objects
    :param input_bucket: name of input s3 bucket
    :type input_bucket: str
    :param outp_dir: local directory where to store the rebuilt files
    :type outp_dir: str
    :return: a list of tuples (see return type of `upload`)
    :rtype: list of tuples
    """

    def mkdir(path):
        if not os.path.exists(path):
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        else:
            for f in os.listdir(path):
                os.remove(os.path.join(path, f))

    # create a temporary output directory named after media title and year
    # e.g. IMP-1994
    issue_dir, issue_json = issues[0]
    key = f"{issue_dir.alias}-{issue_dir.date.year}"
    issue_out_dir = os.path.join(output_dir, key)
    mkdir(issue_out_dir)

    # identify the source type and medium of the issue (and thus media title)
    if "st" not in issue_json or "sm" not in issue_json:
        # when the source type and medium are not in the issue,
        # we know it's a newspaper (old format), add them
        issue_json["st"] = SourceType.NP.value
        issue_json["sm"] = SourceMedium.PT.value

    issue_medium = issue_json["sm"]

    # warning about large graph comes here
    print("Fleshing out articles by issue...")
    issues_bag = db.from_sequence(issues, partition_size=3)

    cis_bag = filter_and_process_cis(issues_bag, input_bucket, issue_medium, _format)

    def has_language(ci):
        if "lg" not in ci:
            return False
        return ci["lg"] in filter_language

    if filter_language:
        filtered_cis = cis_bag.filter(has_language).persist()
        print(f"filtered_cis.count().compute(): {filtered_cis.count().compute()}")
        # TODO provide sm and st to manifest
        stats_for_issues = compute_stats_in_rebuilt_bag(
            filtered_cis, key, title=issue_dir.alias
        )
        result = filtered_cis.map(json.dumps).to_textfiles(f"{issue_out_dir}/*.json")
    else:
        # TODO provide sm and st to manifest
        print(
            f"cis_bag.count().compute(): {cis_bag.count().compute()}, out_dirs: {issue_out_dir}/*.json, cis_bag.take(3): {cis_bag.take(3)}"
        )
        stats_for_issues = compute_stats_in_rebuilt_bag(cis_bag, key, title=issue_dir.alias)
        result = cis_bag.map(json.dumps).to_textfiles(f"{issue_out_dir}/*.json")

    dask_client.cancel(issues_bag)
    logger.info("done.")
    print("done.")

    return (key, result, stats_for_issues)


def init_logging(level, file):
    """Initialises the root logger.

    :param level: desired level of logging (default: logging.INFO)
    :type level: int
    :param file:
    :type file: str
    :return: the initialised logger
    :rtype: `logging.RootLogger`

    .. note::

        It's basically a duplicate of `impresso_commons.utils.init_logger` but
        I could not get it to work properly, so keeping this duplicate.
    """
    # Initialise the logger
    root_logger = logging.getLogger("")
    root_logger.setLevel(level)

    if file is not None:
        handler = logging.FileHandler(filename=file, mode="w")
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.info("Logger successfully initialised")

    return root_logger


def main() -> None:

    def signal_handler():
        # Handle any cleanup here
        print(
            "SIGINT or CTRL-C detected. Exiting gracefully"
            " and shutting down the dask local cluster"
        )
        client.shutdown()
        sys.exit(0)

    arguments = docopt(__doc__)
    clear_output = arguments["--clear"]
    bucket_name = f's3://{arguments["--input-bucket"]}'
    output_bucket_name = arguments["--output-bucket"]
    outp_dir = arguments["--output-dir"]
    filter_config_file = arguments["--filter-config"]
    output_format = arguments["--format"]
    scheduler = arguments["--scheduler"]
    log_file = arguments["--log-file"]
    nworkers = arguments["--nworkers"] if arguments["--nworkers"] else 8
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    languages = arguments["--languages"]
    repo_path = arguments["--git-repo"]
    temp_dir = arguments["--temp-dir"]
    prev_manifest_path = arguments["--prev-manifest"] if arguments["--prev-manifest"] else None

    signal.signal(signal.SIGINT, signal_handler)

    if languages:
        languages = languages.split(",")

    init_logger(log_level, log_file)

    # clean output directory if existing
    if outp_dir is not None and os.path.exists(outp_dir):
        if clear_output is not None and clear_output:
            shutil.rmtree(outp_dir)
            os.mkdir(outp_dir)

    with open(filter_config_file, "r", encoding="utf-8") as file:
        config = json.load(file)

    # start the dask local cluster
    if scheduler is None:
        client = Client(n_workers=nworkers, threads_per_worker=1)
    else:
        client = Client(scheduler)

    dask_cluster_msg = f"Dask local cluster: {client}"
    logger.info(dask_cluster_msg)
    print(dask_cluster_msg)

    # the created manifest is not the same based on the output format
    data_stage = "rebuilt" if output_format == "solr" else "passim"

    # initialize manifest
    manifest = DataManifest(
        data_stage=data_stage,
        s3_output_bucket=output_bucket_name,
        s3_input_bucket=bucket_name,
        git_repo=git.Repo(repo_path),
        temp_dir=temp_dir,
        previous_mft_path=prev_manifest_path if prev_manifest_path != "" else None,
    )
    titles = set()

    if arguments["rebuild_articles"]:

        try:
            for n, batch in enumerate(config):
                rebuilt_issues = []
                proc_b_msg = f"Processing batch {n + 1}/{len(config)} [{batch}]"
                logger.info(proc_b_msg)
                print(proc_b_msg)
                alias = list(batch.keys())[0]
                start_year, end_year = batch[alias]

                for year in range(start_year, end_year):
                    proc_year_msg = f"Processing year {year} \nRetrieving issues..."
                    logger.info(proc_year_msg)
                    print(proc_year_msg)

                    try:
                        input_issues = read_s3_issues(alias, year, bucket_name)
                    except FileNotFoundError:
                        fnf_msg = f"{alias}-{year} not found in {bucket_name}"
                        logger.info(fnf_msg)
                        print(fnf_msg)
                        continue

                    issue_key, json_files, year_stats = rebuild_issues(
                        issues=input_issues,
                        input_bucket=bucket_name,
                        output_dir=outp_dir,
                        dask_client=client,
                        _format=output_format,
                        filter_language=languages,
                    )
                    rebuilt_issues.append((issue_key, json_files))
                    del input_issues

                    msg = f"{issue_key} - year_stats: {year_stats}"
                    print(msg)
                    logger.debug(msg)
                    manifest.add_by_title_year(alias, year, year_stats[0])
                    titles.add(alias)

                msg = (
                    f"Uploading {len(rebuilt_issues)} rebuilt bz2files "
                    f"to {output_bucket_name}"
                )
                logger.info(msg)
                print(msg)

                if len(rebuilt_issues) == 1:
                    # if there is only one issue, we should not use starmap
                    b = (
                        db.from_sequence(rebuilt_issues)
                        .map(compress, output_dir=outp_dir)
                        .map(upload, bucket_name=output_bucket_name)
                        .map(cleanup)
                    )
                else:
                    b = (
                        db.from_sequence(rebuilt_issues)
                        .starmap(compress, output_dir=outp_dir)
                        .starmap(upload, bucket_name=output_bucket_name)
                        .starmap(cleanup)
                    )
                future = b.persist()
                progress(future)
                # clear memory of objects once computations are done
                client.restart()
                rstrt_msg = f"Restarted client after finishing processing batch {n + 1}"
                print(rstrt_msg)
                logger.info(rstrt_msg)

        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print(e)
            client.shutdown()
        finally:
            client.shutdown()

        manifest_note = f"Rebuilt of newspaper articles for {list(titles)}."
        manifest.append_to_notes(manifest_note)
        # finalize and compute the manifest
        manifest.compute(export_to_git_and_s3=False)
        manifest.validate_and_export_manifest(push_to_git=False)

        logger.info("---------- Done ----------")
        print("---------- Done ----------")

    elif arguments["rebuild_pages"]:
        print("\nFunction not yet implemented (sorry!).\n")

    client.close()


if __name__ == "__main__":
    main()
