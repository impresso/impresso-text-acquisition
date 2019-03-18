"""Importer for the newspapers data of the Luxembourg National Library"""

import codecs
import os
from collections import namedtuple
from datetime import date
from time import strftime

from bs4 import BeautifulSoup

from text_importer.helpers import serialize_issue
from text_importer.helpers import get_issue_schema

LuxIssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path',
        'rights'
    ]
)


def mets2issue(issue_dir, encoding='utf-8'):
    # find the mets file
    # read important infos

    issue_id = "{}-{}-{}".format(
        issue_dir.journal,
        "{}-{}-{}".format(
            issue_dir.date.year,
            issue_dir.date.month,
            issue_dir.date.day
        ),
        issue_dir.edition
    )

    # get the canonical names for pages in the newspaper issue by
    # visiting the `text` sub-folder with the alto XML files
    text_path = os.path.join(issue_dir.path, 'text')
    """
    page_file_names = [
        file
        for file in os.listdir(text_path)
        if '.xml' in file
    ]
    page_numbers = [
        int(fname.split('-')[-1].replace('.xml', ''))
        for fname in page_file_names
    ]
    page_canonical_names = [
        "{}-p{}".format(issue_id, str(page_n).zfill(4))
        for page_n in page_numbers
    ]
    """
    page_canonical_names = []

    #######################
    # parse the METS file #
    #######################

    mets_file = [
        os.path.join(issue_dir.path, f)
        for f in os.listdir(issue_dir.path)
        if 'mets.xml' in f
    ][0]

    with codecs.open(mets_file, 'r', encoding) as f:
        raw_xml = f.read()

    local_id = None
    title = None
    content_items = []
    item_counter = 0

    mets_doc = BeautifulSoup(raw_xml, 'xml')

    for section in mets_doc.findAll('dmdSec'):

        section_id = section.get('ID')

        if 'COLLECTION' in section_id:
            local_id = section.findAll('identifier')[0].getText()
            try:
                title = section.find_all('title')[0].getText()
            except IndexError:
                title = None
        elif 'ARTICLE' in section_id:
            item_counter += 1
            lang = section.find_all('languageTerm')[0].getText()
            title_elements = section.find_all('titleInfo')
            item_title = title_elements[0].getText().replace('\n', ' ').strip() if len(title_elements) > 0 else None
            metadata = {
                'id': str(item_counter),
                't': item_title,
                'l': lang,
                'tp': 'ar',
                'pp': [] # TODO: find the pages belonging to the article
            }
            item = {
                "m": metadata,
                "l": {
                    "id": section_id
                }
            }
            content_items.append(item)

    ###############################
    # instantiate the IssueSchema #
    ###############################
    issue_data = {
        "cdt": strftime("%Y-%m-%d %H:%M:%S"),
        "t": title,
        "i": content_items,
        "id": issue_id,
        "pp": page_canonical_names
    }
    #IssueSchema = get_issue_schema()
    #issue = IssueSchema(**issue_data)
    return issue_data


def import_issues(issues, out_dir, serialize=False):
    imported_issues = []
    for issue_dir in issues:
        issue_json = mets2issue(issue_dir)
        issue_out_dir = os.path.join(out_dir, issue_dir.journal)

        if serialize:
                serialize_issue(issue_json, issue_dir, issue_out_dir)

        imported_issues.append(issue_json)
    return imported_issues


def dir2issue(path):
    """Create a LuxIssueDir object from a directory."""
    issue_dir = os.path.basename(path)
    local_id = issue_dir.split('_')[2]
    issue_date = issue_dir.split('_')[3]
    year, month, day = issue_date.split('-')
    rights = 'o' if 'public_domain' in path else 'c' # to be discussed
    editions_mappings = {
        1: 'a',
        2: 'b',
        3: 'c',
        4: 'd',
        5: 'e'
    }

    if len(issue_dir.split('_')) == 4:
        edition = 'a'
    elif len(issue_dir.split('_')) == 5:
        edition = issue_dir.split('_')[4]
        edition = editions_mappings[int(edition)]

    return LuxIssueDir(
        local_id,
        date(int(year), int(month), int(day)),
        edition,
        path,
        rights
    )


def detect_issues(base_dir):
    """Parse a directory structure and detect newspaper issues to be imported.

    :param base_dir: the root of the directory structure
    :type base_dir: LuxIssueDir
    :return: list of `LuxIssueDir` instances
    :rtype: list
    """
    dir_path, dirs, files = next(os.walk(base_dir))
    batches_dirs = [os.path.join(dir_path, dir) for dir in dirs]
    issue_dirs = [
        os.path.join(batch_dir, dir)
        for batch_dir in batches_dirs
        for dir in os.listdir(batch_dir)
        if 'newspaper' in dir
    ]
    return [
        dir2issue(dir)
        for dir in issue_dirs
    ]
