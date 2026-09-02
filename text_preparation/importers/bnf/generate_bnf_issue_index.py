#!/usr/bin/env python3
"""Generate (or update) the BNF issue index JSON file.

Walks each title's directory tree under ``base_data_path/BNF/{alias}/{yyyy}/{mm}/{dd}/{edition}``,
reads the ``manifest.json`` written by the BnF API downloader for each issue, and writes a
``{alias: {year: {month: [issue, ...]}}}`` index to ``output_file``.

This is a straight port of the "BNF case" cells in ``notebooks/index_generator.ipynb`` into a
script that can run unattended in a ``screen`` session (a full walk over all titles can take a
long time). The index is resumable: aliases already present in ``output_file`` are skipped unless
listed in ``aliases_to_override``, and the file is rewritten to disk after every alias so an
interrupted run loses at most the alias in progress.

Example (screen)::

    screen -S bnf_index
    conda activate cpu
    python generate_bnf_issue_index.py --log_file=/path/to/bnf_index.log
    # Ctrl-A D to detach, `screen -r bnf_index` to reattach

To (re)compute only specific titles without touching the rest of the index::

    python generate_bnf_issue_index.py --aliases_to_override=humanite,letemps
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import fire
import ijson
from tqdm import tqdm

from impresso_essentials.utils import init_logger

logger = logging.getLogger(__name__)

# Titles that were already shared with us for Impresso 1 and live under a different directory
# name than their alias.
IMP1_DIRNAME_TO_ALIAS = {
    "Excelsior": "excelsior",
    "La-Fronde": "lafronde",
    "Marie-Claire": "marieclaire",
    "Oeuvre": "oeuvre",  # oeuvre contains both old and new data - both need to be listed in issue_index
}

# Aliases whose entry in `output_file` should be recomputed even though they are already present
# (e.g. titles reprocessed after a fix, or added since the last full run).
DEFAULT_ALIASES_TO_OVERRIDE = [
    # "bsgecm",
    "bcaf",
    "actionfrancaise1899",
    "actionfrancaise1908",
    "echoalger",
    "echoran",
    "humanite",
    # "ikdam",
    "intransigeant",
    "univers",
    "lajustice",
    "lapresse",
    "canardenchaine",
    "leconstitutionnel",
    "figaro1826",
    "figaro1854",
    # "figarosupl",
    "lepetitjournal",
    "letemps",
    "revuecol",
]

DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def find_bnf_issues_w_walk(
    prov_base_dir: str, alias: str, base_dir: str, skipped_issues: Optional[list] = None
) -> Tuple[Dict, Optional[list]]:
    """Discover BNF issues for one title using the directory structure:
    [base]/[alias]/[yyyy]/[mm]/[dd]/[edition_letter]

    Issues are identified by the presence of a manifest.json file.
    """
    alias_issues = {}
    alias_dir = os.path.join(prov_base_dir, alias)
    for issue_dir_path, dirs, files in tqdm(os.walk(alias_dir), desc=alias):
        if alias == "oeuvre" and (
            "toc" in issue_dir_path or "ocr" in issue_dir_path or (dirs and files)
        ):
            # oeuvre contains issue dirs in the "old" format too, which should be skipped: they can
            # be identified because the old format has a dir per issue containing other directories
            skipped_issues.append(issue_dir_path)
            continue
        elif files:
            manifest_file = [f for f in files if f == "manifest.json"]
            if len(manifest_file) == 0:
                logger.warning(
                    "Manifest JSON file is missing in dir %s, skipping this issue (dirs=%s, files=%s)",
                    issue_dir_path,
                    dirs,
                    files,
                )
                continue
            elif len(manifest_file) > 1:
                logger.warning(
                    "More than 1 Manifest JSON file in dir %s, using the first one. (dirs=%s, files=%s)",
                    issue_dir_path,
                    dirs,
                    files,
                )

            # the issue edition disambiguation was already done in the download
            year, month, day, edition = issue_dir_path.replace(alias_dir, "")[1:].split("/")

            manifest_path = os.path.join(issue_dir_path, manifest_file[0])
            with open(manifest_path, "rb") as f:
                id_url = next(ijson.items(f, "id"))

            issue_dict = {
                "day": day,
                "edition": edition,
                "local_path": [issue_dir_path.replace(base_dir, "")[1:]],
                "ark_id": id_url.split("/")[-2],
                "batch": "BNF_API_NEW",
            }

            if year not in alias_issues:
                alias_issues[year] = {month: [issue_dict]}
            elif month not in alias_issues[year]:
                alias_issues[year][month] = [issue_dict]
            else:
                alias_issues[year][month].append(issue_dict)

    return alias_issues, skipped_issues


def detect_issues_bnf(
    out_path: str,
    base_dir: str,
    prov: str = "BNF",
    write_file: bool = True,
    resume: bool = True,
    aliases_to_override: Optional[List[str]] = None,
) -> Tuple[Dict, Optional[list]]:
    """Discover all BNF newspaper issues using the directory structure:
    [base]/[alias]/[yyyy]/[mm]/[dd]/[edition_name]

    Aliases already present in `out_path` are skipped unless resume=False or they are listed in
    aliases_to_override. The index is rewritten to disk after every alias.
    """
    prov_base_dir = os.path.join(base_dir, prov)

    try:
        title_dirs = [
            d
            for d in os.listdir(prov_base_dir)
            if os.path.isdir(os.path.join(prov_base_dir, d))
            and "2020" not in d
            and not d.startswith(".")
        ]
    except OSError as e:
        logger.error("Failed to list base directory %s: %s", prov_base_dir, e)
        return {}, None

    logger.info("Found %d title directories: %s", len(title_dirs), title_dirs)

    if resume and os.path.exists(out_path):
        with open(out_path, "r") as fin:
            all_issues = json.load(fin)
        logger.info(
            "The following aliases were already processed and will be skipped: %s",
            list(all_issues.keys()),
        )
    else:
        all_issues = {}

    oeuvre_skipped_issues = None
    if aliases_to_override is None:
        aliases_to_override = []

    for idx, alias in enumerate(title_dirs):
        if alias in IMP1_DIRNAME_TO_ALIAS:
            alias = IMP1_DIRNAME_TO_ALIAS[alias]

        if alias in all_issues and alias not in aliases_to_override:
            logger.info(
                "%d/%d - Alias %s is already present in the index and will be skipped.",
                idx + 1,
                len(title_dirs),
                alias,
            )
            continue
        else:
            logger.info(
                "%d/%d - Processing to find the issues of alias %s...",
                idx + 1,
                len(title_dirs),
                alias,
            )

        if alias not in ("excelsior", "lafronde", "marieclaire"):
            # skip the impresso 1 data for now, will add later
            if alias == "oeuvre":
                alias_issues, oeuvre_skipped_issues = find_bnf_issues_w_walk(
                    prov_base_dir, alias, base_dir, []
                )
            else:
                alias_issues, _ = find_bnf_issues_w_walk(prov_base_dir, alias, base_dir, None)
        else:
            continue

        n_issues = sum(len(d) for m in alias_issues.values() for d in m.values())
        logger.info("Adding %d issues for alias %s to the total list of aliases", n_issues, alias)
        all_issues[alias] = alias_issues

        if write_file:
            with open(out_path, "w") as fout:
                json.dump(all_issues, fout, indent=4)

    return all_issues, oeuvre_skipped_issues


def main(
    base_data_path: str = "/mnt/project_impresso/original",
    provider_name: str = "BNF",
    output_file: str = "",
    aliases_to_override: str = ",".join(DEFAULT_ALIASES_TO_OVERRIDE),
    resume: bool = True,
    log_level: str = "INFO",
    log_file: str = "",
) -> None:
    """Generate/update the BNF issue index.

    Args:
        base_data_path: Root directory containing the provider directory (`{base_data_path}/{provider_name}`).
        provider_name: Provider directory name under base_data_path. Defaults to "BNF".
        output_file: Path to the issue index JSON to read/write. Defaults to
            `text_preparation/data/issue_indices/issue_index.{provider_name.lower()}_new.json`.
        aliases_to_override: Comma-separated list of aliases to recompute even if already present
            in output_file. Empty string = only compute aliases missing from the index.
        resume: If True (default), reuse and add to an existing output_file. If False, start fresh.
        log_level: One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
        log_file: Log destination. Empty = stdout.
    """
    if log_level.upper() not in VALID_LOG_LEVELS:
        print(
            f"Error: log_level must be one of {VALID_LOG_LEVELS}, got '{log_level}'",
            file=sys.stderr,
        )
        sys.exit(1)

    if not output_file:
        output_file = str(
            DEFAULT_DATA_DIR / "issue_indices" / f"issue_index.{provider_name.lower()}_new.json"
        )

    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    init_logger(logger, getattr(logging, log_level.upper()), log_file or None)

    override_list = [a.strip() for a in aliases_to_override.split(",") if a.strip()]

    logger.info("Provider path: %s", os.path.join(base_data_path, provider_name))
    logger.info("Output file: %s", output_file)
    logger.info("Resume: %s", resume)
    logger.info("Aliases to override: %s", override_list)

    all_issues, skipped_issues = detect_issues_bnf(
        output_file,
        base_dir=base_data_path,
        prov=provider_name,
        resume=resume,
        aliases_to_override=override_list,
    )

    logger.info("Done. Index now has %d aliases, written to %s", len(all_issues), output_file)
    if skipped_issues:
        logger.info(
            "Skipped %d 'oeuvre' issue directories (old format): %s",
            len(skipped_issues),
            skipped_issues,
        )


if __name__ == "__main__":
    fire.Fire(main)
