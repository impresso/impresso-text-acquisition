"""This module contains helper functions to find KBR OCR data to import."""

import logging
import os
import re
from collections import namedtuple
from datetime import date, datetime

from text_preparation.importers.detect import _apply_datefilter

logger = logging.getLogger(__name__)

KbrIssueDir = namedtuple(
    "IssueDirectory", ["provider", "alias", "date", "edition", "path"]
)
"""A light-weight data structure to represent a KBR newspaper issue.

This named tuple contains basic metadata about a newspaper issue. They
can then be used to locate the relevant data in the filesystem or to create
canonical identifiers for the issue and its pages.

Note:
    In case of newspaper published multiple times per day, a lowercase letter
    is used to indicate the edition number: 'a' for the first, 'b' for the
    second, etc.

Args:
    provider (str): Provider for this alias, here always "KBR"
    alias (str): Newspaper alias (e.g., "Bruxellois", "Lynx").
    date (datetime.date): Publication date of issue.
    edition (str): Edition of the newspaper issue ('a', 'b', 'c', etc.).
    path (str): Path to the directory containing the issue's OCR data.

>>> from datetime import date
>>> i = KbrIssueDir(
    provider='KBR',
    alias='Bruxellois', 
    date=date(1917, 8, 8), 
    edition='a', 
    path='./KBR/Bruxellois/16777935_19170808_136456'
)
"""


def dir2issue(path: str, alias: str) -> KbrIssueDir | None:
    """Convert a directory path into a KbrIssueDir object.

    Expected directory structure:
    [base]/[alias]/[newspaper_id]_[yyyymmdd]_[unique_id]

    The directory name follows the pattern: {newspaper_id}_{date}_{unique_id}
    Example: 16777935_19170808_136456

    Args:
        path (str): The issue directory path
        alias (str): The newspaper alias (directory name in base path)

    Returns:
        KbrIssueDir | None: The corresponding Issue, or None if path is invalid
    """
    try:
        # Get the directory name (issue folder)
        dir_name = os.path.basename(path.rstrip("/"))
        
        # Parse the directory name: {newspaper_id}_{date}_{unique_id}
        parts = dir_name.split("_")
        if len(parts) < 3:
            logger.warning(f"Directory name does not match expected pattern: {dir_name}")
            return None
        
        # The date is the second part: YYYYMMDD
        date_str = parts[1]
        if len(date_str) != 8:
            logger.warning(f"Invalid date format in directory name: {date_str}")
            return None
        
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        
        # Default edition to 'a' - this will be updated later when grouping by date
        edition = "a"

        return KbrIssueDir(
            provider="KBR",
            alias=alias,
            date=date(year, month, day),
            edition=edition,
            path=path,
        )
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse issue directory {path}: {e}")
        return None


def get_issue_quality_score(path: str) -> tuple[int, datetime | None]:
    """Get a quality score for an issue based on file metadata.

    This function analyzes the ALTO files in an issue directory to determine
    the quality of the OCR. It can be used to choose between duplicate issues.

    Scoring criteria:
    1. More recent processing date (from ALTO processingDateTime)
    2. Higher unique ID in the directory name (newer digitization)

    Args:
        path (str): Path to the issue directory

    Returns:
        tuple[int, datetime | None]: (unique_id score, processing date if found)
    """
    # Get the unique ID from the directory name (last part)
    dir_name = os.path.basename(path.rstrip("/"))
    parts = dir_name.split("_")
    unique_id = int(parts[-1]) if parts else 0
    
    # Try to get processing date from ALTO files
    processing_date = None
    try:
        for filename in os.listdir(path):
            if filename.endswith(".xml"):
                alto_path = os.path.join(path, filename)
                with open(alto_path, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Look for processingDateTime in the ALTO file
                # Example: <processingDateTime>2019-05-10</processingDateTime>
                match = re.search(r"<processingDateTime>([^<]+)</processingDateTime>", content)
                if match:
                    date_str = match.group(1)
                    try:
                        processing_date = datetime.strptime(date_str, "%Y-%m-%d")
                    except ValueError:
                        pass
                break  # Only need to check one file
    except Exception as e:
        logger.debug(f"Could not get processing date for {path}: {e}")
    
    return (unique_id, processing_date)


def detect_issues(
    base_dir: str, alias_filter: list[str] | None = None, exclude_list: list[str] | None = None
) -> list[KbrIssueDir]:
    """Detect KBR issues to import within the filesystem.

    Traverses the directory structure looking for issue directories.
    Handles multiple issues per day by assigning editions 'a', 'b', 'c', etc.
    based on quality scores (newer processing/higher unique ID = better quality).

    Expected structure:
    base_dir/
        alias1/
            newspaper_id_YYYYMMDD_uniqueid/
                ALTO_files.xml
        alias2/
            ...

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to newspaper aliases.
        alias_filter (list[str] | None, optional): Aliases to consider. Defaults to None.
        exclude_list (list[str] | None, optional): Aliases to exclude. Defaults to None.

    Returns:
        list[KbrIssueDir]: List of `KbrIssueDir` instances to import.
    """
    # Dictionary to track multiple issues per day: {(alias, date): [issues]}
    issues_per_day = {}

    # First, list alias directories in base_dir
    try:
        alias_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    except OSError as e:
        logger.error(f"Failed to list base directory {base_dir}: {e}")
        return []

    # Apply alias filters early
    if alias_filter:
        alias_dirs = [a for a in alias_dirs if a in alias_filter]
    if exclude_list:
        alias_dirs = [a for a in alias_dirs if a not in exclude_list]

    # Walk through each alias directory
    for alias in alias_dirs:
        alias_path = os.path.join(base_dir, alias)
        
        # List issue directories (direct children)
        try:
            issue_dirs = [
                d for d in os.listdir(alias_path) 
                if os.path.isdir(os.path.join(alias_path, d))
            ]
        except OSError as e:
            logger.error(f"Failed to list alias directory {alias_path}: {e}")
            continue

        for issue_dir in issue_dirs:
            issue_path = os.path.join(alias_path, issue_dir)
            
            # Check if the directory contains ALTO XML files
            try:
                has_alto = any(f.endswith(".xml") for f in os.listdir(issue_path))
            except OSError:
                continue
            
            if not has_alto:
                logger.debug(f"Skipping {issue_path} - no ALTO files found")
                continue
            
            issue = dir2issue(issue_path, alias)
            
            if issue is None:
                continue
            
            # Group issues by alias and date to handle multiple editions per day
            day_key = (issue.alias, issue.date)
            if day_key not in issues_per_day:
                issues_per_day[day_key] = []
            issues_per_day[day_key].append(issue)

    # Process and finalize issues
    issues = []
    for day_key, day_issues in issues_per_day.items():
        if len(day_issues) == 1:
            # Only one issue for this day
            issues.append(day_issues[0])
        else:
            # Multiple issues for the same day - sort by quality and assign editions
            # Get quality scores for each issue
            scored_issues = []
            for issue in day_issues:
                unique_id, proc_date = get_issue_quality_score(issue.path)
                scored_issues.append((issue, unique_id, proc_date))
            
            # Sort by: processing date (newer first), then unique_id (higher first)
            scored_issues.sort(
                key=lambda x: (x[2] if x[2] else datetime.min, x[1]),
                reverse=True
            )
            
            # Log duplicate detection
            logger.info(
                f"Found {len(scored_issues)} duplicate issues for "
                f"{day_key[0]} on {day_key[1]}:"
            )
            for issue, uid, pdate in scored_issues:
                logger.info(f"  - {os.path.basename(issue.path)}: unique_id={uid}, "
                           f"processing_date={pdate}")
            
            # Assign editions: 'a' for best quality, 'b' for second best, etc.
            for idx, (issue, _, _) in enumerate(scored_issues):
                edition_letter = chr(97 + idx)  # 97 is ASCII for 'a'
                corrected_issue = KbrIssueDir(
                    provider=issue.provider,
                    alias=issue.alias,
                    date=issue.date,
                    edition=edition_letter,
                    path=issue.path,
                )
                issues.append(corrected_issue)
                logger.debug(
                    f"Issue {corrected_issue.alias} - {corrected_issue.date} "
                    f"assigned edition '{corrected_issue.edition}'"
                )

    # Sort final list by alias, date, and edition for consistent output
    issues.sort(key=lambda x: (x.alias, x.date, x.edition))

    logger.info(f"Found {len(issues)} issues in {base_dir}")
    return issues


def select_issues(base_dir: str, config: dict) -> list[KbrIssueDir] | None:
    """Detect selectively newspaper issues to import.

    The behavior is very similar to :func:`detect_issues` with the only
    difference that ``config`` specifies some rules to filter the data to
    import. See the configuration documentation for details on filtering.

    Args:
        base_dir (str): Path to the base directory of newspaper data,
            this directory should contain directories corresponding to newspaper aliases.
        config (dict): Configuration dictionary containing 'titles', 'exclude_titles',
            and 'year_only' keys for filtering.

    Returns:
        list[KbrIssueDir] | None: List of `KbrIssueDir` instances to import.
    """
    try:
        filter_dict = config.get("titles", {})
        exclude_list = config.get("exclude_titles", [])
        year_flag = config.get("year_only", False)
    except KeyError as e:
        logger.critical(f"Missing required key in config file: {e}")
        return None

    alias_filter = list(filter_dict.keys()) if filter_dict else None

    selected_issues = detect_issues(base_dir, alias_filter, exclude_list)

    # Apply date filtering if we have a filter dict
    if filter_dict and not exclude_list:
        filtered_issues = _apply_datefilter(filter_dict, selected_issues, year_only=year_flag)
    else:
        filtered_issues = selected_issues

    logger.info(f"{len(filtered_issues)} newspaper issues remained after applying filter")

    return filtered_issues


def analyze_duplicates(base_dir: str, alias: str | None = None) -> dict:
    """Analyze duplicate issues in the KBR data.

    This utility function helps identify which issues have duplicates
    and provides recommendations on which copy to use based on quality.

    Args:
        base_dir (str): Path to the base directory of newspaper data.
        alias (str | None): Specific alias to analyze. If None, analyze all.

    Returns:
        dict: Dictionary with analysis results per alias/date combination
    """
    alias_filter = [alias] if alias else None
    
    # First pass: collect all issues
    issues_per_day = {}
    
    try:
        alias_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    except OSError as e:
        logger.error(f"Failed to list base directory {base_dir}: {e}")
        return {}
    
    if alias_filter:
        alias_dirs = [a for a in alias_dirs if a in alias_filter]
    
    for alias_name in alias_dirs:
        alias_path = os.path.join(base_dir, alias_name)
        
        try:
            issue_dirs = [
                d for d in os.listdir(alias_path) 
                if os.path.isdir(os.path.join(alias_path, d))
            ]
        except OSError:
            continue
        
        for issue_dir in issue_dirs:
            issue_path = os.path.join(alias_path, issue_dir)
            issue = dir2issue(issue_path, alias_name)
            
            if issue is None:
                continue
            
            day_key = (issue.alias, issue.date)
            if day_key not in issues_per_day:
                issues_per_day[day_key] = []
            
            # Get quality info
            unique_id, proc_date = get_issue_quality_score(issue_path)
            issues_per_day[day_key].append({
                "path": issue_path,
                "dir_name": issue_dir,
                "unique_id": unique_id,
                "processing_date": proc_date,
            })
    
    # Build analysis results
    results = {}
    for day_key, issues_list in issues_per_day.items():
        if len(issues_list) > 1:
            # Sort by quality
            issues_list.sort(
                key=lambda x: (x["processing_date"] if x["processing_date"] else datetime.min, x["unique_id"]),
                reverse=True
            )
            
            results[f"{day_key[0]}_{day_key[1]}"] = {
                "count": len(issues_list),
                "recommended": issues_list[0]["dir_name"],
                "all_copies": issues_list,
            }
    
    return results
