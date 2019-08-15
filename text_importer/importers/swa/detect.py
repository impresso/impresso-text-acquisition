import json
import logging
import os
from collections import namedtuple
from datetime import datetime
from typing import List, Optional
from datetime import date
import pandas as pd
from text_importer.utils import get_access_right

logger = logging.getLogger(__name__)

SwaIssueDir = namedtuple(
        "IssueDirectory", [
                'journal',
                'date',
                'edition',
                'path',
                'rights',
                'pages',
                ]
        )


def _apply(part):
    res = []
    archives = set()
    for i, x in part.iterrows():
        res.append((x.identifier_impresso, x.full_xml_path))
        archives.add(x.goobi_name + '.zip')
    return pd.Series({'pages': res, 'archives': archives})


def get_issuedir(row, archives_full_dir):
    if len(row.archives) > 1:
        logger.debug(f"Issue {row.manifest_id} has more than one archive {row.archives}")
    
    archive = os.path.join(archives_full_dir, list(row.archives)[0])
    if os.path.isfile(archive):
        split = row.manifest_id.split('-')[:-1]
        if len(split) == 5:
            try:
                journal, year, mo, day, edition = split
                d = date(int(year), int(mo), int(day))
                return SwaIssueDir(journal, date=d, edition=edition, path=archive, rights='open_public',  # TODO: ask about rights
                                   pages=row.pages)
            except ValueError as e:
                logger.debug(f"Issue {row.manifest_id} does not have a regular name")
        else:
            logger.debug(f"Issue {row.manifest_id} does not have regular name")
    else:
        logger.debug(f"Issue {row.manifest_id} does not have archive")
    return None


def detect_issues(base_dir: str, access_rights: str, csv_file: str = 'impresso_ids.csv', archives_dir: str = 'impresso_ocr'):
    archives_full_dir = os.path.join(base_dir, archives_dir)
    csv_file = os.path.join(base_dir, csv_file)
    
    df = pd.read_csv(csv_file).groupby('manifest_id').apply(_apply).reset_index()
    result = df.apply(lambda r: get_issuedir(r, archives_full_dir), axis=1)
    result = result[~result.isna()]
    
    return result.values


def select_issues(base_dir: str, config: dict, access_rights: str):  # TODO: continue select issues
    try:
        filter_dict = config.get("newspapers")
        exclude_list = config["exclude_newspapers"]
        year_flag = config["year_only"]
    except KeyError:
        logger.critical(f"The key [newspapers|exclude_newspapers|year_only] is missing in the config file.")
        return
    exclude_flag = False if not exclude_list else True
    logger.debug(f"got filter_dict: {filter_dict}, "
                 f"\nexclude_list: {exclude_list}, "
                 f"\nyear_flag: {year_flag}"
                 f"\nexclude_flag: {exclude_flag}")
    
    if not filter_dict and not exclude_list:  # todo: remove this case? should be detect issue
        logger.debug("No positive nor negative filter definition, all issues in {inp_dir} will be considered.")
        issues = detect_issues(base_dir, access_rights)
        return issues
    else:
        filter_newspapers = set(filter_dict.keys()) if not exclude_list else set(exclude_list)
        logger.debug(f"got filter_newspapers: {filter_newspapers}, with exclude flag: {exclude_flag}")
        issues = detect_issues(base_dir, access_rights)
        
        # apply date filter if not exclusion mode
        return issues
