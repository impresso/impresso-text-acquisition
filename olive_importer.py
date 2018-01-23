"""
Functions and CLI script to convert Olive OCR data into Impresso's format.

Usage:
    olive_importer.py --input-dir=<id> --output-dir==<od> [--log-file=<f> --log-level=<l> --temp-dir==<td>]
"""

import os
import codecs
import json
import pdb
import sys
import string
import zipfile
from collections import deque, namedtuple
from datetime import date

from bs4 import BeautifulSoup
from docopt import docopt

# TODO: use `dask` for parallel execution

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"

# a simple data structure to represent input directories
# a `Document.zip` file is expected to be found in `IssueDir.path`
IssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path'
    ]
)

punctuation_nows_before = [".", ",", ")", "]", "}", "°", "..."]
punctuation_nows_after = ["(", "[", "{"]
punctuation_nows_beforeafter = ["'", "-"]
punctuation_ciffre = [".", ","]

html_escape_table = {
    "&amp;": "&",
    "&quot;": '"',
    "&apos;": "'",
    "&gt;": ">",
    "&lt;": "<",
}

# perhaps we don't need it
def to_canonical_filename(arg):
    pass


def olive_parser(text):
    """
    Parse a Le Temps xml file to an xml file reorganized for our internal use.

    @param text String: textual xml file to parse

    @return textual xml file to save as .xml, encoded in utf-8
        (so just write in wb mode the result into an xml file)
    """
    soup = BeautifulSoup(text)
    root = soup.find("xmd-entity")
    page_no = root['page_no']
    identifier = root['id']
    language = root['language']
    title = soup.meta['name']
    publication = soup.meta['publication']
    entity_type = root['entity_type']
    issue_date = soup.meta['issue_date']

    out = {
        "meta": {
            "id": identifier,
            "title": title,
            "page_no": page_no,
            "language": {"raw": language, "mapped": ""},
            "publication": publication,
            "type": entity_type,
            "issue_date": issue_date,
            "continuation_from": None,
            "continuation_to": None
        },
        "fulltext": "",
        # vars to store coordinates information
        "coordinates": {
            'words': [],
            'boxes': []
        },
        "stats": {}
    }

    # Prepare text
    word_count = 0
    char_count = 0
    fulltext = ''

    for primitive in soup.find_all("primitive"):

        # store coordinate of text areas (boxes) by page
        # 1) page number, 2) coordinate list
        coords = [int(i) for i in primitive.get('box').split(" ")]
        out["coordinates"]["boxes"].append((int(page_no), coords))

        for tag in primitive.find_all(recursive=False):
            if tag.name == "l":
                if len(fulltext) > 0:
                    if fulltext[-1] == "-":
                        fulltext = fulltext[-1]
                        word_count -= 1
            if tag.name in ["q", "w"]:
                # store coordinates of each token
                # 1) token, 2) page number, 3) coordinate list
                coords = [int(i) for i in tag.get('box').split(" ")]
                out["coordinates"]["words"].append(
                    (
                        tag.string,
                        int(page_no),
                        coords
                    )
                )
            if tag.name in ["w", "qw"]:
                if len(fulltext) > 0:
                    if (tag.string in punctuation_nows_before or tag.string in punctuation_nows_beforeafter) and fulltext[-1] == " ":
                        fulltext = fulltext[:-1]
                        word_count -= 1
                    # fix digits
                    if tag.string.isdigit():
                        before = fulltext.split()[-1]
                        if before[:-1].isdigit() and before[-1] in punctuation_ciffre:
                            fulltext = fulltext[:-1]
                            word_count -= 1
                fulltext += tag.string + " "
                if tag.string not in string.punctuation:
                    word_count += 1
                char_count += len(list(tag.string))
                if len(fulltext) > 0:
                    if (tag.string in punctuation_nows_after or tag.string in punctuation_nows_beforeafter) and fulltext[-1] == " ":
                        fulltext = fulltext[:-1]
                        word_count -= 1

    out["fulltext"] = fulltext.strip()
    out["stats"]["word_count"] = soup.meta['wordcnt']
    out["stats"]["total_chars_count"] = soup.meta['total_chars_count']
    suspicious_chars_count = soup.meta['suspicious_chars_count']
    out["stats"]["suspicious_chars_count"] = suspicious_chars_count
    out["stats"]["updated_word_count"] = word_count
    out["stats"]["updated_chars_count"] = char_count

    """
    # Prepare links elements
    source = out.new_tag("source")
    source.string = soup.link['source']
    links.append(source)

    first_id = out.new_tag("first_id")
    first_id.string = soup.link['first_id']
    links.append(first_id)

    last_id = out.new_tag("last_id")
    last_id.string = soup.link['last_id']
    links.append(last_id)

    next_id = out.new_tag("next_id")
    next_id.string = soup.link['next_id']
    links.append(next_id)

    prev_id = out.new_tag("prev_id")
    prev_id.string = soup.link['prev_id']
    links.append(prev_id)
    """

    if root.has_attr('continuation_from'):
        out["meta"]["continuation_from"] = root['continuation_from']

    if root.has_attr('continuation_to'):
        out["meta"]["continuation_to"] = root['continuation_to']

    return out


def import_issue(issue_dir, out_dir, temp_dir=None):
    """TODO.

    Program logic:
        - unzip the zip file in a temp directory (to be removed at the end)
        - parse each XML file and put in a data structure
        - serialize data structure to JSON, written in `out_dir` with canonical
            filename (but all files in one dir?, or divided by journal)
        - remove temporary directory

    """
    out_dir = os.path.join(
        out_dir,
        issue_dir.journal,
        str(issue_dir.date.year),
        str(issue_dir.date.month).zfill(2),
        str(issue_dir.date.day).zfill(2),
        issue_dir.edition
    )

    if temp_dir is not None:
        temp_dir = os.path.join(
            temp_dir,
            "{}-{}-{}-{}-{}".format(
                issue_dir.journal,
                str(issue_dir.date.year),
                str(issue_dir.date.month).zfill(2),
                str(issue_dir.date.day).zfill(2),
                issue_dir.edition
            )
        )
    working_archive = os.path.join(issue_dir.path, "Document.zip")

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if temp_dir is not None and not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    if os.path.isfile(working_archive):
        try:
            archive = zipfile.ZipFile(working_archive)
        except:
            print("Bad zip file.")
            print("Unexpected error:", sys.exc_info()[0])
            print(out_dir + ";bad zip file;\n")
            return  # TODO: add correct return object

        counter = 0
        article = []
        items = sorted(
            [
                item
                for item in archive.namelist()
                if ".xml" in item
                and not item.startswith("._")
                and "/Ar" in item
            ]
        )

        # if a `temp_dir` is passed as a parameter, do store the intermediate
        # Olive .xml files, otherwise just read without storing them
        if temp_dir is not None:
            for item in items:
                with codecs.open(
                    os.path.join(temp_dir, item.replace('/', '-')),
                    'wb'
                ) as out_file:
                    xml_data = archive.read(item)
                    out_file.write(xml_data)

        while len(items) > 0:
            counter += 1
            out_file = os.path.join(
                out_dir,
                "{}-{}-{}-{}-{}-{}{}".format(
                    issue_dir.journal,
                    str(issue_dir.date.year),
                    str(issue_dir.date.month).zfill(2),
                    str(issue_dir.date.day).zfill(2),
                    issue_dir.edition,
                    str(counter).zfill(4),
                    ".json"
                )
            )

            # if out file already exists skip the data it contains
            # TODO: change this to work with the JSON output
            if os.path.exists(out_file):
                exclude_data = BeautifulSoup(open(out_file).read())
                exclude_data = [
                    x.meta.id.string
                    for x in exclude_data.find_all("entity")
                ]
                for y in exclude_data:
                    for z in items:
                        if y in z:
                            items.remove(z)
                continue

            internal_deque = deque([items[0]])
            items = items[1:]
            while len(internal_deque) > 0:
                item = internal_deque.popleft()
                # print(item)
                # legacy code had: `archive.read(item, 'r')` which won't work
                xml_data = archive.read(item)
                new_data = olive_parser(xml_data)

                # check if it needs to be parsed later on
                if new_data["meta"]['continuation_from'] is not None:
                    target = new_data["meta"]["continuation_from"]
                    target = [x for x in items if target in x]
                    if len(target) > 0:
                        items.append(item)
                        continue

                article.append(new_data)
                # .prettify(formatter="minimal").encode('utf-8')
                # print(article)
                if new_data["meta"]['continuation_to'] is not None:
                    next_id = new_data["meta"]["continuation_to"]
                    next_id = [x for x in items if next_id in x][0]
                    internal_deque.append(next_id)
                    items.remove(next_id)

            with codecs.open(out_file, 'w', 'utf-8') as out_file:
                json.dump(article, out_file, indent=3)
            article = []


    # or return (directory, False, error)
    return (issue_dir, True, None)


# by decoupling the directory parsing and the unzipping etc.
# this code can run in parallel
def detect_journal_issues(base_dir):
    """Parse a directory structure and detect newspaper issues to be imported.

    :param base_dir: the root of the directory structure
    """
    detected_issues = []
    known_journals = ["GDL", "EVT", "JDG", "LNQ"]  # TODO: anything to add?
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [d for d in dirs if d in known_journals]

    for journal in journal_dirs:
        journal_path = os.path.join(base_dir, journal)
        dir_path, year_dirs, files = next(os.walk(journal_path))
        # year_dirs = [d for d in dirs if len(d) == 4]

        for year in year_dirs:
            year_path = os.path.join(journal_path, year)
            dir_path, month_dirs, files = next(os.walk(year_path))

            for month in month_dirs:
                month_path = os.path.join(year_path, month)
                dir_path, day_dirs, files = next(os.walk(month_path))

                for day in day_dirs:
                    day_path = os.path.join(month_path, day)
                    # concerning `edition="a"`: for now, no cases of newspapers
                    # published more than once a day in Olive format (but it
                    # may come later on)

                    # TODO: add some debug logging
                    detected_issues.append(
                        IssueDir(
                            journal,
                            date(int(year), int(month), int(day)),
                            'a',
                            day_path
                        )
                    )

    return detected_issues


def main(args):
    """Execute the main with CLI parameters."""
    print(args)
    inp_dir = args["--input-dir"]
    outp_dir = args["--output-dir"]
    temp_dir = args["--temp-dir"]
    journal_issues = detect_journal_issues(inp_dir)
    result = [import_issue(i, outp_dir, temp_dir) for i in journal_issues]  # parallelize
    print(result)
    pdb.set_trace()


if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
