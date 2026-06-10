"""This module contains helper functions to detect Chronicling America issues on disk."""

import os
import logging
from datetime import date
from impresso_essentials.utils import IssueDir
from text_preparation.importers.detect import _apply_datefilter
from text_preparation.utils import edition_num_to_code

logger = logging.getLogger(__name__)

def detect_issues(
    base_dir: str,
    alias_filter: list[str] | None = None,
    exclude_list: list[str] | None = None,
) -> list[IssueDir]:
    """Dynamically scan directory structure to detect Chronicling America issues.

    Expected directory structure:
    [base_dir]/[alias]/[year]/[month]/[day]/[edition]/
    """
    issues = []
    
    # 1. Walk base_dir to find newspaper directories
    if not os.path.exists(base_dir):
        logger.warning(f"Base directory {base_dir} does not exist.")
        return []

    aliases = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) and not d.startswith(".")]

    # Register LOC aliases to satisfy impresso_essentials utilities
    from impresso_essentials.utils import (
        PARTNER_TO_MEDIA,
        PARTNERS_TO_SRC_MEDIUM_TO_MEDIA,
        PARTNERS_TO_SRC_TYPE_TO_MEDIA,
        PARTNER_TO_COUNTRY,
        MEDIA_TO_COUNTRY,
        ALL_MEDIA,
        SourceMedium,
        SourceType,
    )
    if "LOC" not in PARTNER_TO_MEDIA:
        PARTNER_TO_MEDIA["LOC"] = []
    if "LOC" not in PARTNERS_TO_SRC_MEDIUM_TO_MEDIA:
        PARTNERS_TO_SRC_MEDIUM_TO_MEDIA["LOC"] = {SourceMedium.PT: "all"}
    if "LOC" not in PARTNERS_TO_SRC_TYPE_TO_MEDIA:
        PARTNERS_TO_SRC_TYPE_TO_MEDIA["LOC"] = {SourceType.NP: "all"}
    if "LOC" not in PARTNER_TO_COUNTRY:
        PARTNER_TO_COUNTRY["LOC"] = "US"

    for alias in aliases:
        if alias not in PARTNER_TO_MEDIA["LOC"]:
            PARTNER_TO_MEDIA["LOC"].append(alias)
        if alias not in MEDIA_TO_COUNTRY:
            MEDIA_TO_COUNTRY[alias] = "US"
        if alias not in ALL_MEDIA:
            ALL_MEDIA.append(alias)
    ALL_MEDIA.sort()

    # Apply alias filter
    if alias_filter:
        aliases = [a for a in aliases if a in alias_filter]
    if exclude_list:
        aliases = [a for a in aliases if a not in exclude_list]

    for alias in aliases:
        alias_path = os.path.join(base_dir, alias)
        
        # Walk years
        years = [d for d in os.listdir(alias_path) if os.path.isdir(os.path.join(alias_path, d)) and d.isdigit()]
        for year in years:
            year_path = os.path.join(alias_path, year)
            
            # Walk months
            months = [d for d in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, d)) and d.isdigit()]
            for month in months:
                month_path = os.path.join(year_path, month)
                
                # Walk days
                days = [d for d in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, d)) and d.isdigit()]
                for day in days:
                    day_path = os.path.join(month_path, day)
                    
                    # Walk editions (e.g. ed-1, ed-2)
                    editions = [d for d in os.listdir(day_path) if os.path.isdir(os.path.join(day_path, d)) and not d.startswith(".")]
                    for edition in editions:
                        edition_path = os.path.join(day_path, edition)
                        
                        try:
                            # Verify that the directory contains a METS file
                            xml_files = [f for f in os.listdir(edition_path) if f.endswith(".xml") and not f.startswith(".")]
                            if not xml_files:
                                continue
                            
                            # Translate edition folder name (e.g. "ed-1") to letter code (e.g. "a")
                            edition_code = "a"
                            if edition.startswith("ed-"):
                                try:
                                    edition_code = edition_num_to_code(int(edition[3:]))
                                except (ValueError, TypeError):
                                    pass
                            elif edition.isdigit():
                                try:
                                    edition_code = edition_num_to_code(int(edition))
                                except (ValueError, TypeError):
                                    pass
                            else:
                                edition_code = edition
                                
                            issue = IssueDir(
                                provider="LOC",  # Standard provider name for Library of Congress
                                alias=alias,
                                date=date(int(year), int(month), int(day)),
                                edition=edition_code,
                                path=edition_path,
                            )
                            issues.append(issue)
                        except ValueError as e:
                            logger.warning(f"Skipping invalid date folder {year}/{month}/{day}: {e}")
                            
    logger.info(f"Detected {len(issues)} issues in {base_dir}")
    return issues

def select_issues(base_dir: str, config: dict) -> list[IssueDir] | None:
    """Detect selectively newspaper issues to import using a config dict."""
    try:
        filter_dict = config.get("titles", {})
        exclude_list = config.get("exclude_titles", [])
        year_flag = config.get("year_only", False)
    except KeyError as e:
        logger.critical(f"Missing required key in config file: {e}")
        return None

    alias_filter = list(filter_dict.keys()) if filter_dict else None
    selected_issues = detect_issues(base_dir, alias_filter, exclude_list)

    # Apply date filtering
    if filter_dict and not exclude_list:
        filtered_issues = _apply_datefilter(filter_dict, selected_issues, year_only=year_flag)
    else:
        filtered_issues = selected_issues

    logger.info(f"{len(filtered_issues)} newspaper issues remained after applying filter")
    return filtered_issues
