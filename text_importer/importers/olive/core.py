from typing import List, Type
from impresso_commons.path.path_fs import IssueDir, canonical_path
from text_importer.importers.olive.classes import OliveNewspaperIssue
import logging

logger = logging.getLogger(__name__)


def dir2issue(issue_dir):
    issue = None
    try:
        issue = OliveNewspaperIssue(issue_dir)
    except Exception as e:
        logger.error(f'Error when processing issue {issue_dir}')
        logger.exception(e)
    return issue


def import_issues(issues: List[IssueDir], out_dir: str, s3_bucket: str, image_dir: str, temp_dir: str):
    msg = f'Issues to import: {len(issues)}'
    logger.info(msg)
