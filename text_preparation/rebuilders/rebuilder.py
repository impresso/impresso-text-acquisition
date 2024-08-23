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
import datetime
import json
import pathlib
import logging
import os
import shutil
import signal
from typing import Any, Optional
import git

import dask.bag as db
import jsonlines
from dask.distributed import Client, progress
from docopt import docopt
from smart_open import smart_open

from impresso_commons.path import parse_canonical_filename
from impresso_commons.path.path_s3 import read_s3_issues
from impresso_commons.text.helpers import (
    read_issue_pages,
    rejoin_articles,
    reconstruct_iiif_link,
    insert_whitespace,
)
from impresso_commons.utils import Timer, timestamp
from impresso_commons.utils.s3 import get_s3_resource

from impresso_commons.versioning.data_manifest import DataManifest
from impresso_commons.versioning.helpers import compute_stats_in_rebuilt_bag

logger = logging.getLogger(__name__)


TYPE_MAPPINGS = {
    "article": "ar",
    "ar": "ar",
    "advertisement": "ad",
    "ad": "ad",
    "pg": None,
    "image": "img",
    "table": "tb",
    "death_notice": "ob",
    "weather": "w",
}
# TODO KB data: add familial announcement?


def rebuild_text(
    page: list[dict], language: Optional[str], string: Optional[str] = None
) -> tuple[str, dict[list], dict[list]]:
    """Rebuild the text of an article for Solr ingestion.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, dict[list], dict[list]]: [0] Article fulltext, [1] offsets and
            [2] coordinates of token regions.
    """

    coordinates = {"regions": [], "tokens": []}

    offsets = {"line": [], "para": [], "region": []}

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)
    for region in page:

        if len(string) > 0:
            offsets["region"].append(len(string))

        coordinates["regions"].append(region["c"])

        for para in region["p"]:

            if len(string) > 0:
                offsets["para"].append(len(string))

            for line in para["l"]:

                for n, token in enumerate(line["t"]):
                    region = {}
                    if "c" not in token:
                        print(f"'c' was not present in token: {token}, line: {line}")
                        continue
                    region["c"] = token["c"]
                    region["s"] = len(string)

                    if "hy" in token:
                        region["l"] = len(token["tx"][:-1]) - 1
                        region["hy1"] = True
                    elif "nf" in token:
                        region["l"] = len(token["nf"])
                        region["hy2"] = True

                        token_text = token["nf"] if token["nf"] is not None else ""
                    else:
                        if token["tx"]:
                            region["l"] = len(token["tx"])
                        else:
                            region["l"] = 0

                        token_text = token["tx"] if token["tx"] is not None else ""

                    # don't add the tokens corresponding to the first part of a hyphenated word
                    if "hy" not in token:
                        next_token = (
                            line["t"][n + 1]["tx"] if n != len(line["t"]) - 1 else None
                        )
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=next_token,
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        string += f"{token_text} " if ws else f"{token_text}"

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        if "hy" in token:
                            offsets["line"].append(region["s"])
                        else:
                            token_length = len(token["tx"]) if token["tx"] else 0
                            offsets["line"].append(region["s"] + token_length)

                    coordinates["tokens"].append(region)

    return (string, coordinates, offsets)


def rebuild_text_passim(
    page: list[dict], language: Optional[str], string: Optional[str] = None
) -> tuple[str, list[dict]]:
    """The text rebuilding function from pages for passim.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, list[dict]]: [0] article fulltext, [1] coordinates of token regions.
    """

    regions = []

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)

    for region in page:

        for para in region["p"]:

            for line in para["l"]:

                for n, token in enumerate(line["t"]):

                    region_string = ""

                    if "c" not in token:
                        # if the coordniates are missing, they should be skipped
                        logger.debug("Missing 'c' in token %s", token)
                        print(f"Missing 'c' in token {token}")
                        continue

                    # each page region is a token
                    output_region = {
                        "start": None,
                        "length": None,
                        "coords": {
                            "x": token["c"][0],
                            "y": token["c"][1],
                            "w": token["c"][2],
                            "h": token["c"][3],
                        },
                    }

                    if len(string) == 0:
                        output_region["start"] = 0
                    else:
                        output_region["start"] = len(string)

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        tmp = f"{token['tx']}\n"
                        region_string += tmp
                    else:
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=line["t"][n + 1]["tx"],
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        region_string += f"{token['tx']} " if ws else f"{token['tx']}"

                    string += region_string
                    output_region["length"] = len(region_string)
                    regions.append(output_region)

    return (string, regions)


def rebuild_for_solr(content_item: dict[str, Any]) -> dict[str, Any]:
    """Rebuilds the text of an article content-item given its metadata as input.

    Note:
        This rebuild function is thought especially for ingesting the newspaper
        data into our Solr index.

    Args:
        content_item (dict[str, Any]): The content-item to rebuilt using its metadata.

    Returns:
        dict[str, Any]: The rebuilt content-item following the Impresso JSON Schema.
    """
    t = Timer()

    article_id = content_item["m"]["id"]
    logger.debug("Started rebuilding article %s", article_id)

    issue_id = "-".join(article_id.split("-")[:-1])

    page_file_names = {
        p: f"{issue_id}-p{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]
    }

    year, month, day, _, ci_num = article_id.split("-")[1:]
    d = datetime.date(int(year), int(month), int(day))

    raw_type = content_item["m"]["tp"]

    if raw_type in TYPE_MAPPINGS:
        mapped_type = TYPE_MAPPINGS[raw_type]
    else:
        mapped_type = raw_type

    fulltext = ""
    linebreaks = []
    parabreaks = []
    regionbreaks = []

    article_lang = content_item["m"]["l"] if "l" in content_item["m"] else None

    # if the reading order is not defined, use the number associated to each CI
    reading_order = (
        content_item["m"]["ro"] if "ro" in content_item["m"] else int(ci_num[1:])
    )

    article = {
        "id": article_id,
        "pp": content_item["m"]["pp"],
        "d": d.isoformat(),
        "olr": False if mapped_type is None else True,
        "ts": timestamp(),
        "lg": article_lang,
        "tp": mapped_type,
        "ro": reading_order,
        "s3v": content_item["m"]["s3v"] if "s3v" in content_item["m"] else None,
        "ppreb": [],
        "lb": [],
        "cc": content_item["m"]["cc"],
    }

    if mapped_type == "img":
        article["iiif_link"] = reconstruct_iiif_link(content_item)

    if "t" in content_item["m"]:
        article["t"] = content_item["m"]["t"]

    if mapped_type != "img":
        for n, page_no in enumerate(article["pp"]):

            page = content_item["pprr"][n]

            if fulltext == "":
                fulltext, coords, offsets = rebuild_text(page, article_lang)
            else:
                fulltext, coords, offsets = rebuild_text(page, article_lang, fulltext)

            linebreaks += offsets["line"]
            parabreaks += offsets["para"]
            regionbreaks += offsets["region"]

            page_doc = {
                "id": page_file_names[page_no].replace(".json", ""),
                "n": page_no,
                "t": coords["tokens"],
                "r": coords["regions"],
            }
            article["ppreb"].append(page_doc)
        article["lb"] = linebreaks
        article["pb"] = parabreaks
        article["rb"] = regionbreaks
        logger.debug("Done rebuilding article %s (Took %s)", article_id, t.stop())
        article["ft"] = fulltext

    return article


def rebuild_for_passim(content_item: dict[str, Any]) -> dict[str, Any]:
    """Rebuilds the text of an article content-item to be used with passim.

    Args:
        content_item (dict[str, Any]): The content-item to rebuilt using its metadata.

    Returns:
        dict[str, Any]: The rebuilt content-item built for passim.
    """
    np, date, _, _, _, _ = parse_canonical_filename(content_item["m"]["id"])

    article_id = content_item["m"]["id"]
    logger.debug("Started rebuilding article %s", article_id)
    issue_id = "-".join(article_id.split("-")[:-1])

    page_file_names = {
        p: f"{issue_id}-p{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]
    }

    article_lang = content_item["m"]["l"] if "l" in content_item["m"] else None

    passim_document = {
        "series": np,
        "date": f"{date[0]}-{date[1]}-{date[2]}",
        "id": content_item["m"]["id"],
        "cc": content_item["m"]["cc"],
        "lg": article_lang,
        "pages": [],
    }

    if "t" in content_item["m"]:
        passim_document["title"] = content_item["m"]["t"]

    fulltext = ""
    for n, page_no in enumerate(content_item["m"]["pp"]):

        page = content_item["pprr"][n]

        if fulltext == "":
            fulltext, regions = rebuild_text_passim(page, article_lang)
        else:
            fulltext, regions = rebuild_text_passim(page, article_lang, fulltext)

        page_doc = {
            "id": page_file_names[page_no].replace(".json", ""),
            "seq": page_no,
            "regions": regions,
        }
        passim_document["pages"].append(page_doc)

    passim_document["text"] = fulltext

    return passim_document


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

    newspaper, year = key.split("-")
    filename = f"{newspaper}-{year}.jsonl.bz2"
    filepath = os.path.join(output_dir, filename)
    logger.info("Compressing %s JSON files into %s", len(json_files), filepath)
    print(f"Compressing {len(json_files)} JSON files into {filepath}")

    with smart_open(filepath, "wb") as fout:
        writer = jsonlines.Writer(fout)

        for json_file in json_files:
            with open(json_file, "r", encoding="utf-8") as inpf:
                reader = jsonlines.Reader(inpf)
                articles = list(reader)
                writer.write_all(articles)
            logger.info(
                "Written %s docs from %s to %s", len(articles), json_file, filepath
            )

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

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
    """
    # create connection with bucket
    # copy contents to s3 key
    newspaper, _ = sort_key.split("-")
    key_name = f"{newspaper}/{os.path.basename(filepath)}"
    if "/" in bucket_name:
        # if the provided bucket also contains a partition, add it to the key name
        bucket_name, partition = bucket_name.split("/")
        key_name = f"{partition}/{key_name}"
    s3 = get_s3_resource()
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(filepath, key_name)
        logger.info("Uploaded %s to %s", filepath, key_name)
        return True, filepath
    except Exception as e:
        logger.error(e)
        logger.error("The upload of %s failed with error %s", filepath, e)
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
            logger.warning("Error %s occurred when removing %s", e, filepath)
    else:
        logger.info("Not removing %s as upload has failed", filepath)


def _article_has_problem(article):
    """Helper function to keep articles with problems.

    :param article: input article
    :type article: dict
    :return: `True` or `False`
    :rtype: boolean
    """
    return article["has_problem"]


def _article_without_problem(article):
    """Helper function to keep articles without problems.

    :param article: input article
    :type article: dict
    :return: `True` or `False`
    :rtype: boolean
    """
    if article["has_problem"]:
        logger.warning("Article %s won't be rebuilt.", article["m"]["id"])
    return not article["has_problem"]


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

    # determine which rebuild function to apply
    if _format == "solr":
        rebuild_function = rebuild_for_solr
    elif _format == "passim":
        rebuild_function = rebuild_for_passim
    else:
        raise NotImplementedError

    # create a temporary output directory named after newspaper and year
    # e.g. IMP-1994
    issue, _ = issues[0]
    key = f"{issue.journal}-{issue.date.year}"
    issue_dir = os.path.join(output_dir, key)
    mkdir(issue_dir)

    # warning about large graph comes here
    print("Fleshing out articles by issue...")
    issues_bag = db.from_sequence(issues, partition_size=3)

    faulty_issues = (
        issues_bag.filter(lambda i: len(i[1]["pp"]) == 0)
        .map(lambda i: i[1])
        .pluck("id")
        .compute()
    )
    msg = f"Issues with no pages (will be skipped): {faulty_issues}"
    logger.debug(msg)
    print(msg)
    del faulty_issues
    msg = f"Number of partitions: {issues_bag.npartitions}"
    logger.debug(msg)
    print(msg)

    articles_bag = (
        issues_bag.filter(lambda i: len(i[1]["pp"]) > 0)
        .starmap(read_issue_pages, bucket=input_bucket)
        .starmap(rejoin_articles)
        .flatten()
        .persist()
    )

    faulty_articles_n = (
        articles_bag.filter(_article_has_problem).pluck("m").pluck("id").compute()
    )
    msg = f"Skipped articles: {faulty_articles_n}"
    logger.debug(msg)
    print(msg)
    del faulty_articles_n

    articles_bag = (
        articles_bag.filter(_article_without_problem).map(rebuild_function).persist()
    )

    def has_language(ci):
        if "lg" not in ci:
            return False
        return ci["lg"] in filter_language

    if filter_language:
        filtered_articles = articles_bag.filter(has_language).persist()
        print(filtered_articles.count().compute())
        stats_for_issues = compute_stats_in_rebuilt_bag(filtered_articles, key)
        result = filtered_articles.map(json.dumps).to_textfiles(f"{issue_dir}/*.json")
    else:
        stats_for_issues = compute_stats_in_rebuilt_bag(articles_bag, key)
        result = articles_bag.map(json.dumps).to_textfiles(f"{issue_dir}/*.json")

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
    prev_manifest_path = (
        arguments["--prev-manifest"] if arguments["--prev-manifest"] else None
    )

    signal.signal(signal.SIGINT, signal_handler)

    if languages:
        languages = languages.split(",")

    init_logging(log_level, log_file)

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
                newspaper = list(batch.keys())[0]
                start_year, end_year = batch[newspaper]

                for year in range(start_year, end_year):
                    proc_year_msg = f"Processing year {year} \nRetrieving issues..."
                    logger.info(proc_year_msg)
                    print(proc_year_msg)

                    try:
                        input_issues = read_s3_issues(newspaper, year, bucket_name)
                    except FileNotFoundError:
                        fnf_msg = f"{newspaper}-{year} not found in {bucket_name}"
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

                    logger.debug("year_stats: %s", year_stats)
                    manifest.add_by_title_year(newspaper, year, year_stats[0])
                    titles.add(newspaper)

                msg = (
                    f"Uploading {len(rebuilt_issues)} rebuilt bz2files "
                    f"to {output_bucket_name}"
                )
                logger.info(msg)
                print(msg)

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


if __name__ == "__main__":
    main()
