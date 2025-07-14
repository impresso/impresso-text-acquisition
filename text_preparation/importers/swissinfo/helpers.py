"""Helper functions to parse SWISSINFO OCR files."""

import logging
import os
from datetime import datetime, date
import json
from typing import Any

from bs4 import Tag
from text_preparation.utils import coords_to_xywh

logger = logging.getLogger(__name__)


def parse_lines(
    blocks_with_lines: dict, pg_id: str, pg_notes: list[str]
) -> tuple[list[list[int]], list[dict]]:
    """Parse the blocks from the OCR to extract the lines of text.

    Args:
        blocks_with_lines (dict): All blcoks with text lines extracted from the PDF OCR.
        pg_id (str): Canonical ID of the page the text is on.
        pg_notes (list[str]): Notes of the page, to store potential issues found.

    Returns:
        tuple[list[list[int]], list[dict]]: Parsed text line corresponding to canonical format.
    """
    all_blocks_xy_coords = []
    paragraphs = []
    hyphen_at_last = False
    # par_sizes = []
    for block_id, block in enumerate(blocks_with_lines):
        all_blocks_xy_coords.append(block["rescaled_bbox"])
        # there is usually only one paragraph per paragraph
        block_lines = []
        for line_id, line in enumerate(block["lines"]):
            tokens = []
            # tokens are in this "spans" object
            for t_id, token in enumerate(line["spans"]):

                # Skip tokens which are only spaces
                if token["text"] == " ":
                    continue

                curr_token = {
                    "c": coords_to_xywh(token["rescaled_bbox"]),
                    "tx": token["text"],
                    "gn": False,
                }

                # second half of a hyphen should be at the start of a line/block (which is not the first)
                if (block_id != 0 or line_id != 0) and t_id == 0 and hyphen_at_last:
                    if (
                        line_id != 0
                        and len(paragraphs) == 0
                        and not ("hy" in block_lines[-1]["t"][-1])
                    ):
                        msg = f"{pg_id} - Warning! problem 1 with hyphen_at_last!: curr_token: {curr_token}, block_lines[-1]['t'][-1]: {block_lines[-1]['t'][-1]}"
                        # logger.info(msg)
                        print(msg)
                        # saving in the notes
                        pg_notes.append(
                            (
                                f"block {block_id} ('number' {block['number']}), line {line_id}, "
                                f"token {t_id} - problem with hyphenation: "
                                "hyphen_at_last is true but no 'hy' in previous token."
                            )
                        )
                    elif (
                        block_id != 0
                        and line_id == 0
                        and not ("hy" in paragraphs[-1]["l"][-1]["t"][-1])
                    ):
                        msg = (
                            f"{pg_id} - Warning! problem 2 with hyphen_at_last!: "
                            f"curr_token: {curr_token}, "
                            f"all_lines[-1]['l'][-1]['t'][-1]: {paragraphs[-1]['l'][-1]['t'][-1]}"
                        )
                        print(msg)
                        # saving in the notes
                        pg_notes.append(
                            (
                                f"block {block_id} ('number' {block['number']}), line {line_id}, "
                                f"token {t_id} - problem with hyphenation: "
                                "hyphen_at_last is true but no 'hy' in previous token."
                            )
                        )

                    # if the first token of the line is a the second part of a hyphen,
                    # we need to merge it with the last token (after removing the hyphen)
                    if len(paragraphs) == 0:
                        full_word = block_lines[-1]["t"][-1]["tx"].split("-")[0] + token["text"]
                    else:
                        full_word = (
                            paragraphs[-1]["l"][-1]["t"][-1]["tx"].split("-")[0] + token["text"]
                        )
                    curr_token["nf"] = full_word

                # reset the hyphenation flag
                hyphen_at_last = False

                tokens.append(curr_token)

            # handle hyphenation
            if len(tokens) > 1 and tokens[-1]["tx"].endswith("-"):
                tokens[-1]["hy"] = True
                hyphen_at_last = True
            else:
                hyphen_at_last = False

            block_lines.append({"c": coords_to_xywh(line["rescaled_bbox"]), "t": tokens})

        paragraphs.append({"c": coords_to_xywh(block["rescaled_bbox"]), "l": block_lines})

    return all_blocks_xy_coords, paragraphs


def compute_agg_coords(all_coords: list[list[int]]) -> list[int]:
    """Compute the coordinates of a paragraph from the coordinates of its lines.

    Args:
        all_coords (list[list[int]]): All line coordinates to merge into one block.

    Returns:
        list[int]: Line coordinates merged into one region block.
    """
    x1 = min([l[0] for l in all_coords])
    y1 = min([l[1] for l in all_coords])
    x2 = max([l[2] for l in all_coords])
    y2 = max([l[3] for l in all_coords])
    return [x1, y1, x2, y2]
    # return coords_to_xywh([x1, y1, x2, y2])
