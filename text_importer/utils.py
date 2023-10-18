import json
import logging
import os
import contextlib
import pathlib
import importlib_resources

import python_jsonschema_objects as pjs

logger = logging.getLogger(__name__)


def init_logger(
    _logger: logging.RootLogger, log_level: int, log_file: str
) -> logging.RootLogger:
    """Initialise the logger.

    :param logging.RootLogger _logger: Logger instance to initialise.
    :param int log_level: Desidered logging level (e.g. ``logging.INFO``).
    :param str log_file: Path to destination file for logging output. If no
        output file is provided (``log_file`` is ``None``) logs will be written
        to standard output.
    :return: The initialised logger object.
    :rtype: logging.RootLogger

    """
    # Initialise the logger
    _logger.setLevel(log_level)

    if log_file is not None:
        handler = logging.FileHandler(filename=log_file, mode="w")
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)

    _logger.info("Logger successfully initialised")
    return _logger

def get_pkg_resource(
    file_manager: contextlib.ExitStack, path: str, package: str = "text_importer"
) -> pathlib.PosixPath:
    """Return the resource at `path` in `package`, using a context manager.

    Note: 
        The context manager `file_manager` needs to be instantiated prior to 
        calling this function and should be closed once the package resource 
        is no longer of use.

    Args:
        file_manager (contextlib.ExitStack): Context manager.
        path (str): Path to the desired resource in given package.
        package (str, optional): Package name. Defaults to "text_importer".

    Returns:
        pathlib.PosixPath: Path to desired managed resource.
    """
    ref = importlib_resources.files(package)/path
    return file_manager.enter_context(importlib_resources.as_file(ref))


def get_page_schema(
    schema_folder: str = "impresso-schemas/json/newspaper/page.schema.json"
):
    """Generate a list of python classes starting from a JSON schema.

    :param schema_folder: path to the schema folder (default="./schemas/")
    :rtype: `python_jsonschema_objects.util.Namespace`
    """
    file_manager = contextlib.ExitStack()
    schema_path = get_pkg_resource(file_manager, schema_folder)
    with open(os.path.join(schema_path), "r") as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().NewspaperPage
    file_manager.close()
    return ns


def get_issue_schema(
    schema_folder: str = "impresso-schemas/json/newspaper/issue.schema.json"
):
    """Generate a list of python classes starting from a JSON schema.

    :param schema_folder: path to the schema folder (default="./schemas/")
    :type schema_folder: string
    :rtype: `python_jsonschema_objects.util.Namespace`
    """
    file_manager = contextlib.ExitStack()
    schema_path = get_pkg_resource(file_manager, schema_folder)
    with open(os.path.join(schema_path), "r") as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().NewspaperIssue
    file_manager.close()
    return ns


def get_access_right(journal: str, date, access_rights: dict) -> str:
    rights = access_rights[journal]
    if rights["time"] == "all":
        return rights["access-right"].replace("-", "_")
    else:
        # TODO: this should rather be a custom exception
        logger.warning(f"Access right not defined for {journal}-{date}")


def verify_imported_issues(actual_issue_json: dict, expected_issue_json: dict):
    """Short summary.

    :param dict actual_issue_json: Description of parameter `actual_issue_json`.
    :param dict expected_issue_json: Description of parameter `expected_issue_json`.

    """
    # FIRST CHECK: number of content items
    actual_ids = set([i["m"]["id"] for i in actual_issue_json["i"]])
    expected_ids = set([i["m"]["id"] for i in expected_issue_json["i"]])
    logger.info(
        (
            f"[{actual_issue_json['id']}] Expected IDs: {len(expected_ids)}"
            f"; actual IDs: {len(actual_ids)}"
        )
    )
    assert expected_ids.difference(actual_ids) == set()

    # SECOND CHECK: identity of content items
    # the assumption here is that: 1) content item IDs are the same;
    # 2) two CIs are identical when their legacy information is
    # identical (e.g. ID of the XML elememnt in the Olive file)
    for actual_content_item in actual_issue_json["i"]:

        try:
            expected_content_item = [
                ci
                for ci in expected_issue_json["i"]
                if ci["m"]["id"] == actual_content_item["m"]["id"]
            ][0]
        except Exception:
            # usually these are images: they were not there in the
            # first content ingestion; nothing to worry about
            continue

        assert actual_content_item["l"] == expected_content_item["l"]

        logger.info(
            (
                f"Content item {actual_content_item['m']['id']}"
                "dit not change (legacy metadata are identical)"
            )
        )
