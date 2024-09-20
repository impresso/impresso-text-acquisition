"""This module contains generic helper functions for the text-importer module.
"""

import json
import logging
import os
import copy
import shutil
from typing import Any, Callable
from contextlib import ExitStack
from filelock import FileLock
import jsonlines

from smart_open import open as smart_open_function
import python_jsonschema_objects as pjs

from impresso_essentials.utils import get_pkg_resource

logger = logging.getLogger(__name__)


def get_page_schema(
    schema_folder: str = "impresso-schemas/json/newspaper/page.schema.json",
) -> pjs.util.Namespace:
    """Generate a list of python classes starting from a JSON schema.

    Args:
        schema_folder (str, optional): Path to the schema folder. Defaults to
            "impresso-schemas/json/newspaper/page.schema.json".

    Returns:
        pjs.util.Namespace: Newspaper page schema based on canonical format.
    """
    file_manager = ExitStack()
    schema_path = get_pkg_resource(file_manager, schema_folder, "text_preparation")
    with open(os.path.join(schema_path), "r", encoding="utf-8") as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().NewspaperPage
    file_manager.close()
    return ns


def get_issue_schema(
    schema_folder: str = "impresso-schemas/json/newspaper/issue.schema.json",
) -> pjs.util.Namespace:
    """Generate a list of python classes starting from a JSON schema.

    Args:
        schema_folder (str, optional): Path to the schema folder. Defaults to
            "impresso-schemas/json/newspaper/issue.schema.json".

    Returns:
        pjs.util.Namespace: Newspaper issue schema based on canonical format.
    """
    file_manager = ExitStack()
    schema_path = get_pkg_resource(file_manager, schema_folder, "text_preparation")
    with open(os.path.join(schema_path), "r", encoding="utf-8") as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().NewspaperIssue
    file_manager.close()
    return ns


def verify_imported_issues(
    actual_issue_json: dict[str, Any], expected_issue_json: dict[str, Any]
) -> None:
    """Verify that the imported issues fit expectations.

    Two verifications are done: the number of content items, and their IDs.

    Args:
        actual_issue_json (dict[str, Any]): Created issue json,
        expected_issue_json (dict[str, Any]): Expected issue json.
    """
    # FIRST CHECK: number of content items
    actual_ids = set([i["m"]["id"] for i in actual_issue_json["i"]])
    expected_ids = set([i["m"]["id"] for i in expected_issue_json["i"]])
    logger.info(
        "[%s] Expected IDs: %s; actual IDs: %s",
        actual_issue_json["id"],
        len(expected_ids),
        len(actual_ids),
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
            "Content item %s did not change (legacy metadata are identical)",
            actual_content_item["m"]["id"],
        )


def get_reading_order(items: list[dict[str, Any]]) -> dict[str, int]:
    """Generate a reading order for items based on their id and the pages they span.

    This reading order can be used to display the content items properly in a table
    of contents without skipping form page to page.

    Args:
        items (list[dict[str, Any]]): List of items to reorder for the ToC.

    Returns:
        dict[str, int]: A dictionary mapping item IDs to their reading order.
    """
    items_copy = copy.deepcopy(items)
    ids_and_pages = [(i["m"]["id"], i["m"]["pp"]) for i in items_copy]
    sorted_ids = sorted(
        sorted(ids_and_pages, key=lambda x: int(x[0].split("-i")[-1])),
        key=lambda x: x[1],
    )

    return {t[0]: index + 1 for index, t in enumerate(sorted_ids)}


def add_property(
    object_dict: dict[str, Any],
    prop_name: str,
    prop_function: Callable[[str], str],
    function_input: str,
) -> dict[str, Any]:
    """Add a property and value to a given object dict computed with a given function.

    Args:
        object_dict (dict[str, Any]): Object to which the property is added.
        prop_name (str): Name of the property to add.
        prop_function (Callable[[str], str]): Function computing the property value.
        function_input (str): Input to `prop_function` for this object.

    Returns:
        dict[str, Any]: Updated object.
    """
    object_dict[prop_name] = prop_function(function_input)
    logger.debug(
        "%s -> Added property %s: %s",
        object_dict["id"],
        prop_name,
        object_dict[prop_name],
    )
    return object_dict


def empty_folder(dir_path: str) -> None:
    """Empty a directoy given its path if it exists.

    Args:
        dir_path (str): Path to the directory to empty.
    """
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        logger.info("Emptied directory at %s", dir_path)
    os.mkdir(dir_path)


def write_error(
    thing_id: str, origin_function: str, error: Exception, failed_log: str
) -> None:
    """Write the given error of a failed import to the `failed_log` file.

    Adapted from `impresso-text-acquisition/text_preparation/importers/core.py` to allow
    using a issue or page id, and provide the function in which the error took place.

    Args:
        thing_id (str): Canonical ID of the object/file for which the error occurred.
        origin_function (str): Function in which the exception occured.
        error (Exception): Error that occurred and should be logged.
        failed_log (str): Path to log file for failed imports.
    """
    note = f"Error in {origin_function} for {thing_id}: {error}"
    logger.exception(note)
    with open(failed_log, "a+", encoding="utf-8") as f:
        f.write(note + "\n")


def write_jsonlines_file(
    filepath: str,
    contents: str | list[str],
    content_type: str,
    failed_log: str | None = None,
) -> None:
    """Write the given contents to a JSONL file given its path.

    Filelocks are used here to prevent concurrent writing to the files.

    Args:
        filepath (str): Path to the JSONL file to write to.
        contents (str | list[str]): Dump contents to write to the file.
        content_type (str): Type of content that is being written to the file.
        failed_log (str | None, optional): Path to a log to keep track of failed
            operations. Defaults to None.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # put a file lock to avoid the overwriting of files due to parallelization
    lock = FileLock(filepath + ".lock", timeout=13)

    try:
        with lock:
            with smart_open_function(filepath, "ab") as fout:
                writer = jsonlines.Writer(fout)

                writer.write_all(contents)

                logger.info(
                    "Written %s %s to %s", len(contents), content_type, filepath
                )
                writer.close()
    except Exception as e:
        logger.error("Error for %s", filepath)
        logger.exception(e)
        if failed_log is not None:
            write_error(
                os.path.basename(filepath), "write_jsonlines_file()", e, failed_log
            )
