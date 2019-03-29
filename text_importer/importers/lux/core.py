"""Importer for the newspapers data of the Luxembourg National Library"""

import os
# from text_importer.helpers import get_issue_schema, serialize_issue


def compress_issues(group_id, issues, output_dir):
    pass


def convert_coordinates(hpos, vpos, height, width, x_res, y_res):
    """
    x =   (coordinate['xResolution']/254.0) * coordinate['hpos']

    y =   (coordinate['yResolution']/254.0) * coordinate['vpos']

    w =  (coordinate['xResolution']/254.0) * coordinate['width']

    h =  (coordinate['yResolution']/254.0) * coordinate['height']
    """
    x = (x_res / 254) * hpos
    y = (y_res / 254) * vpos
    w = (x_res / 254) * width
    h = (y_res / 254) * height
    return int(x), int(y), int(w), int(h)


def encode_ark(ark):
    return ark.replace('/', '%2f')


def import_issues(issues, out_dir, serialize=False):
    """
    # TODO: copy Implementation from notebook
    - start from detected issues (a bag ?)
    - for each issue, read the mets file and process it
    - group the issues by newspaper/year
    - serialize each group into a separate zipped archive
    """
    imported_issues = []
    for issue_dir in issues:
        issue_json = mets2issue(issue_dir)
        issue_out_dir = os.path.join(out_dir, issue_dir.journal)

        if serialize:
                serialize_issue(issue_json, issue_dir, issue_out_dir)

        imported_issues.append(issue_json)
    return imported_issues
