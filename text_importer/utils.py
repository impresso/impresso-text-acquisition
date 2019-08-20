import json
import logging
import os

import pkg_resources
import python_jsonschema_objects as pjs

logger = logging.getLogger(__name__)


def init_logger(_logger, log_level, log_file):
    # Initialise the logger
    _logger.setLevel(log_level)

    if log_file is not None:
        handler = logging.FileHandler(filename=log_file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
            )
    handler.setFormatter(formatter)
    _logger.addHandler(handler)

    _logger.info("Logger successfully initialised")


def get_page_schema(schema_folder: str = 'impresso-schemas/json/newspaper/page.schema.json'):
    """Generate a list of python classes starting from a JSON schema.

    :param schema_folder: path to the schema folder (default="./schemas/")
    :rtype: `python_jsonschema_objects.util.Namespace`
    """
    schema_path = pkg_resources.resource_filename(
            'text_importer',
            schema_folder
            )
    with open(os.path.join(schema_path), 'r') as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().NewspaperPage
    return ns


def get_issue_schema(schema_folder: str = 'impresso-schemas/json/newspaper/issue.schema.json'):
    """Generate a list of python classes starting from a JSON schema.

    :param schema_folder: path to the schema folder (default="./schemas/")
    :type schema_folder: string
    :rtype: `python_jsonschema_objects.util.Namespace`
    """
    schema_path = pkg_resources.resource_filename(
            'text_importer',
            schema_folder
            )
    with open(os.path.join(schema_path), 'r') as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    ns = builder.build_classes().NewspaperIssue
    return ns


def get_access_right(journal: str, date, access_rights: dict) -> str:
    rights = access_rights[journal]
    if rights['time'] == 'all':
        return rights['access-right'].replace('-', '_')
    else:
        # TODO: this should rather be a custom exception
        logger.warning(f"Access right not defined for {journal}-{date}")


def verify_imported_issues(actual_issue_json, expected_issue_json):
    # FIRST CHECK: number of content items
    actual_ids = set([i['m']['id'] for i in actual_issue_json['i']])
    expected_ids = set([i['m']['id'] for i in expected_issue_json['i']])
    logger.info((
        f"[{actual_issue_json['id']}] Expected IDs: {len(expected_ids)}"
        f"; actual IDs: {len(actual_ids)}"
    ))
    assert expected_ids.difference(actual_ids) == set()

    # SECOND CHECK: identity of content items
    # the assumption here is that: 1) content item IDs are the same;
    # 2) two CIs are identical when their legacy information is
    # identical (e.g. ID of the XML elememnt in the Olive file)
    for actual_content_item in actual_issue_json['i']:

        try:
            expected_content_item = [
                ci
                for ci in expected_issue_json['i']
                if ci['m']['id'] == actual_content_item['m']['id']
            ][0]
        except Exception:
            # usually these are images: they were not there in the
            # first content ingestion; nothing to worry about
            continue

        assert actual_content_item['l'] == expected_content_item['l']

        logger.info((
            f"Content item {actual_content_item['m']['id']}"
            "dit not change (legacy metadata are identical)"
        ))
