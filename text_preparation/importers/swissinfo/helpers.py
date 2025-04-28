"""Helper functions to parse SWISSINFO OCR files."""

import logging
import os
from datetime import datetime, date
import json
from typing import Any

from bs4 import Tag
from text_preparation.utils import coords_to_xywh

logger = logging.getLogger(__name__)


def parse_lines(blocks_with_lines, pg_id, pg_notes):

    all_line_xy_coords = []
    all_lines = []
    hyphen_at_last = False
    par_sizes = []
    for block_id, block in enumerate(blocks_with_lines):
        all_line_xy_coords.append(block["rescaled_bbox"])
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
                        and len(all_lines) == 0
                        and not ("hy" in block_lines[-1]["t"][-1])
                    ) or (len(all_lines) != 0 and not ("hy" in all_lines[-1]["t"][-1])):
                        msg = f"{pg_id} - Warning! problem with hyphen_at_last!: curr_token: {curr_token}, all_lines[-1]['t'][-1]: {all_lines[-1]['t'][-1]}"
                        logger.info(msg)
                        print(msg)
                        # saving in the notes
                        pg_notes.append(
                            f"block {block_id} ('number' {block['number']}), line {line_id}, token {t_id} - problem with hyphenation: hyphen_at_last is true but no 'hy' in previous token."
                        )

                    # if the first token of the line is a the second part of a hyphen,
                    # we need to merge it with the last token (after removing the hyphen)
                    if len(all_lines) == 0:
                        full_word = (
                            block_lines[-1]["t"][-1]["tx"].split("-")[0] + token["text"]
                        )
                    else:
                        full_word = all_lines[-1]["t"][-1]["tx"].split("-")[0] + token["text"]
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

        par_sizes.append(len(block_lines))
        # there is usually only one line per block
        if len(block_lines) == 1:
            all_lines.append(block_lines[0])
        else:
            # cases where there were more than one line seemed to be errors - to be checked.
            # msg = f"{pg_id} - Warning! {len(block_lines)} lines in this paragraph, adding them separately!! block coords: {[b['c'] for b in block_lines]}"
            pg_notes.append(
                f"block {block_id} ('number' {block['number']}), lines {len(all_lines)}-{len(all_lines)+len(block_lines)} were in the same block initially."
            )
            # print(msg)
            # logger.info(msg)
            all_lines.extend(block_lines)

    return all_line_xy_coords, all_lines, par_sizes


def compute_paragraph_coords(all_line_coords):
    """
    Compute the coordinates of a paragraph from the coordinates of its lines.
    """
    x1 = min([l[0] for l in all_line_coords])
    y1 = min([l[1] for l in all_line_coords])
    x2 = max([l[2] for l in all_line_coords])
    y2 = max([l[3] for l in all_line_coords])
    return coords_to_xywh([x1, y1, x2, y2])
