"""This module contains generic helper functions for the text-importer module."""

import json
import logging
import os
import copy
import shutil
from typing import Any, Callable
from contextlib import ExitStack
from filelock import FileLock
import jsonlines
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw
from smart_open import open as smart_open_function
import python_jsonschema_objects as pjs

from impresso_essentials.utils import get_pkg_resource, validate_against_schema

logger = logging.getLogger(__name__)

# path to the canonical schemas (in essentials, in text_prep, it's `impresso-schemas``)
CANONICAL_PAGE_SCHEMA = "schemas/json/canonical/page.schema.json"
CANONICAL_ISSUE_SCHEMA = "schemas/json/canonical/issue.schema.json"
CANONICAL_RECORD_SCHEMA = "schemas/json/canonical/audio_record.schema.json"


def get_page_schema(
    schema_folder: str = f"impresso-{CANONICAL_PAGE_SCHEMA}",
) -> pjs.util.Namespace:
    """Generate a list of python classes starting from a JSON schema.

    Args:
        schema_folder (str, optional): Path to the schema folder. Defaults to
            "impresso-schemas/json/canonical/page.schema.json".

    Returns:
        pjs.util.Namespace: Printed page schema based on canonical format.
    """
    file_manager = ExitStack()
    schema_path = get_pkg_resource(file_manager, schema_folder, "text_preparation")
    with open(os.path.join(schema_path), "r", encoding="utf-8") as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().Page
    file_manager.close()
    return ns


def get_issue_schema(
    schema_folder: str = f"impresso-{CANONICAL_ISSUE_SCHEMA}",
) -> pjs.util.Namespace:
    """Generate a list of python classes starting from a JSON schema.

    Args:
        schema_folder (str, optional): Path to the schema folder. Defaults to
            "impresso-schemas/json/canonical/issue.schema.json".

    Returns:
        pjs.util.Namespace: Issue schema based on canonical format.
    """
    file_manager = ExitStack()
    schema_path = get_pkg_resource(file_manager, schema_folder, "text_preparation")
    with open(os.path.join(schema_path), "r", encoding="utf-8") as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().Issue
    file_manager.close()
    return ns


def validate_page_schema(page_json: dict, page_schema: str = CANONICAL_PAGE_SCHEMA) -> None:
    # msg = f"{page_json['id']} - Validating against page schema"
    # print(msg)
    # logger.info(msg)
    return validate_against_schema(page_json, page_schema)


def validate_audio_schema(audio_json: dict, audio_schema: str = CANONICAL_RECORD_SCHEMA) -> None:
    # msg = f"{page_json['id']} - Validating against page schema"
    # print(msg)
    # logger.info(msg)
    return validate_against_schema(audio_json, audio_schema)


def validate_issue_schema(issue_json: dict, issue_schema: str = CANONICAL_ISSUE_SCHEMA) -> None:
    # msg = f"{issue_json['id']} - Validating against issue schema"
    # print(msg)
    # logger.info(msg)
    return validate_against_schema(issue_json, issue_schema)


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


def write_error(thing_id: str, origin_function: str, error: Exception, failed_log: str) -> None:
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

                logger.info("Written %s %s to %s", len(contents), content_type, filepath)
                writer.close()
    except Exception as e:
        logger.error("Error for %s", filepath)
        logger.exception(e)
        if failed_log is not None:
            write_error(os.path.basename(filepath), "write_jsonlines_file()", e, failed_log)


def coords_to_xy(coords: list[int | Any], as_int: bool = False) -> list[int | Any]:
    """Convert coordinates from xywh format to x1y1x2y2 format.

    Args:
        coords (list): Coords in xywh format to convert.
        as_int (bool, optional): Whether to cast elements to int before conversion.
            Defaults to False.

    Returns:
        list[int | Any]: Resulting converted coordinates, now in x1y1x2y2 format.
    """
    if as_int:
        coords = [int(c) for c in coords]
    return [coords[0], coords[1], coords[0] + coords[2], coords[1] + coords[3]]


def coords_to_xywh(coords: list[int | Any], as_int: bool = True) -> list[int | Any]:
    """Convert coordinates from x1y1x2y2 format to xywh format.

    Args:
        coords (list): Coords in x1y1x2y2 format to convert.
        as_int (bool, optional): Whether to cast elements to int before conversion.
            Defaults to False.

    Returns:
        list[int | Any]: Resulting converted coordinates, now in xywh format.
    """
    if as_int:
        coords = [int(c) for c in coords]
    return [coords[0], coords[1], coords[2] - coords[0], coords[3] - coords[1]]


def draw_box_on_img(
    base_img_path: str, coords_xy: list, img: Image = None, width: int = 10
) -> Image:
    """Draw a bounding box on an image given coordinates in x1y1x2y2 format.

    The image can either be provided through its path, or as a PIL.Image object (specifically
    if other bboxes have already been drawn on it.)

    Args:
        base_img_path (str): Path to the image to open as a PIL Image.
        coords_xy (list): Coordinates of the bbox to draw.
        img (Image, optional): PIL image if already loaded. Defaults to None.
        width (int, optional): Stroke width for the bbox. Defaults to 10.

    Returns:
        Image: Resulting PIL Image with the bbox drawn on it.
    """
    if not img:
        img = Image.open(base_img_path)
    ImageDraw.Draw(img).rectangle(coords_xy, outline="red", width=width)
    return img


def read_xml(file_path: str) -> BeautifulSoup:
    """Read the content of an XML file to a BeautifulSoup object.

    Args:
        file_path (str): Path to the XML object.

    Returns:
        BeautifulSoup: Resulting BeautifulSoup object.
    """
    with open(file_path, "rb") as f:
        raw_xml = f.read()

    return BeautifulSoup(raw_xml, "xml")


def rescale_coords(
    coords: list[float],
    curr_size: tuple[float, float] = None,
    dest_size: tuple[float, float] = None,
    curr_res: float = None,
    dest_res: float = None,
    xy_format: bool = True,
    int_sc_factor: bool = False,
) -> list[float]:
    """Scales image or bounding box coordinates based on image size or resolution.

    This function rescales a set of coordinates (`coords`) based on either:
    - The current and target image sizes (`curr_size` and `dest_size`).
    - The current and target resolutions (`curr_res` and `dest_res`).

    If `xy_format` is False and `curr_res`/`dest_res` are not provided, the function
    estimates a resolution-based scaling factor using image sizes (`width * height`).

    When `xy_format` is True, the function assumes coordinates are in "x1, y1, x2, y2" format.
    Otherwise, it assumes "x, y, width, height" format.

    Args:
        coords (list[float]): List of coordinates to be scaled.
        curr_size (tuple[float, float], optional): Current image size (width, height).
            Required if `xy_format=True`. Defaults to None.
        dest_size (tuple[float, float], optional): Target image size (width, height).
            Required if `xy_format=True`. Defaults to None.
        curr_res (float, optional): Current image resolution (optional for `xy_format=False`).
        dest_res (float, optional): Target image resolution (optional for `xy_format=False`).
        xy_format (bool, optional): If True, treats coordinates as "x1, y1, x2, y2".
            If False, treats coordinates as "x, y, width, height". Defaults to True.
        int_sc_factor (bool, optional): If True, scales using integer division for factor calculation.
            Defaults to False.

    Returns:
        list[float]: Scaled coordinates.

    Raises:
        ValueError: If required parameters (size or resolution) are missing.
        ValueError: If `curr_size` or `curr_res` contain zero.

    Example:
        >>> scale_coords([10, 20, 30, 40], (100, 200), (200, 400))
        [20.0, 40.0, 60.0, 80.0]
    """
    # Validate input parameters
    if xy_format:
        if curr_size is None or dest_size is None:
            raise ValueError(
                "When `xy_format` is True, `curr_size` and `dest_size` must be provided."
            )
        if 0 in curr_size:
            raise ValueError("Current image size must be non-zero values.")
    else:
        if curr_res is None or dest_res is None:
            if curr_size is None or dest_size is None:
                raise ValueError(
                    "When `xy_format` is False, either (`curr_res` and `dest_res`) or (`curr_size` and `dest_size`) must be provided."
                )
            # Estimate resolution from image size
            curr_res = curr_size[0] * curr_size[1]
            dest_res = dest_size[0] * dest_size[1]
        if curr_res == 0:
            raise ValueError("Current image resolution or size must be non-zero values.")

    # Compute scaling factors
    if xy_format:
        x_scale = (
            int(dest_size[0]) / int(curr_size[0]) if int_sc_factor else dest_size[0] / curr_size[0]
        )
        y_scale = (
            int(dest_size[1]) / int(curr_size[1]) if int_sc_factor else dest_size[1] / curr_size[1]
        )

        return [c * (x_scale if i % 2 == 0 else y_scale) for i, c in enumerate(coords)]
    else:
        scale_factor = int(dest_res) / int(curr_res) if int_sc_factor else dest_res / curr_res

        return [c * scale_factor for c in coords]
