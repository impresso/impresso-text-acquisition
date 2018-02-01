"""
Functions and CLI script to convert Olive OCR data into Impresso's format.

Usage:
    olive_importer.py --input-dir=<id> --output-dir==<od> [--log-file=<f> --temp-dir==<td> --verbose --parallelize]

Options:
    --input-dir=<id>    Base directory containing one sub-directory for each journal.
    --output-dir==<od>  Base directory where to write the output files.
    --log-file=<f>      Log file; when missing print log to stdout
    --verbose           Verbose log messages (good for debugging).
    --parallelize       Parallelize the import.
"""

import codecs
import json
import logging
import os
import shutil
import string
import sys
import zipfile
from collections import deque, namedtuple
from datetime import date
from functools import reduce

import dask
# import ipdb as pdb
import pandas as pd
import python_jsonschema_objects as pjs
from bs4 import BeautifulSoup
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get as mp_get
from docopt import docopt
from numpy import nan

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"
__version__ = "0.1.0"

logger = logging.getLogger(__name__)

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


def canonical_path(dir, name=None, extension=None, path_type="file"):
    """Create a canonical dir/file path from an `IssueDir` object.

    :param dir: an object representing a newspaper issue
    :type dir: `IssueDir`
    :param name: the file name (used only if path_type=='file')
    :type name: string
    :param extension: the file extension (used only if path_type=='file')
    :type extension: string
    :param path_type: type of path to build ('dir' | 'file')
    :type path_type: string
    :rtype: string
    """
    sep = "-" if path_type == "file" else "/"
    extension = extension if extension is not None else ""
    if path_type == "file":
        return "{}{}".format(
            sep.join(
                [
                    dir.journal,
                    str(dir.date.year),
                    str(dir.date.month).zfill(2),
                    str(dir.date.day).zfill(2),
                    dir.edition,
                    name
                ]
            ),
            extension)
    else:
        return sep.join(
            [
                dir.journal,
                str(dir.date.year),
                str(dir.date.month).zfill(2),
                str(dir.date.day).zfill(2),
                dir.edition
            ]
        )


def schemas_to_classes(schema_folder="./schemas/"):
    """Generate a list of python classes starting from JSON schemas.

    :param schema_folder: path to the schema folder (default="./schemas/")
    :type schema_folder: string
    :rtype: `python_jsonschema_objects.util.Namespace`
    """
    with open(os.path.join(schema_folder, "article.schema"), 'r') as f:
        json_schema = json.load(f)
    builder = pjs.ObjectBuilder(json_schema)
    return builder.build_classes()


def write_coordinates(word_coordinates, region_coordinates, out_file):
    """Write word and region coordinates to a CSV file.

    :param word_coordinates: each tuple in the list contains [0] token (str);
        [1] page number (int); [2] coordinates (list of int).
    :type word_coordinates: list of tuples

    :param region_coordinates: each tuple in the list contains: [0] page number
        (int); [1] coordinates (list of int).
    :type region_coordinates: list of tuples

    :param out_file: path of the ouput CSV file
    :type out_file: string
    """
    word_coords_df = pd.DataFrame(
        word_coordinates,
        columns=['token', 'page_no', 'coords']
    )
    region_coords_df = pd.DataFrame(
        region_coordinates,
        columns=['page_no', 'coords']
    )
    word_coords_df["type"] = "word"
    region_coords_df["type"] = "region"
    region_coords_df["token"] = nan
    coords_df = pd.concat([word_coords_df, region_coords_df])
    coords_df.to_csv(out_file, encoding="utf-8")


def olive_parser(text):
    """Parse an Olive XML file (e.g. from Le Temps corpus).

    The main logic implemented here was derived from
    <https://github.com/dhlab-epfl/LeTemps-preprocessing/>.

    :param text: a string with the textual xml file to parse
    :type text: string
    :rtype: a dictionary, with keys "meta", "fulltext", "stats", "legacy"
    """
    soup = BeautifulSoup(text, "lxml")
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
            "language": {},
            "type": {}
        },
        "fulltext": "",
        "stats": {},
        "legacy": {"continuation_from": None, "continuation_to": None},
        "coordinates": {"words": [], "regions": []}
    }
    out["meta"]["title"] = title
    out["meta"]["page_no"] = [int(page_no)]
    out["meta"]["language"]["raw"] = language
    out["meta"]["publication"] = publication
    out["meta"]["type"]["raw"] = entity_type
    out["meta"]["issue_date"] = issue_date

    # Prepare text
    word_count = 0
    char_count = 0
    fulltext = ''

    for primitive in soup.find_all("primitive"):

        # store coordinate of text areas (boxes) by page
        # 1) page number, 2) coordinate list
        coords = [int(i) for i in primitive.get('box').split(" ")]
        out["coordinates"]["regions"].append((int(page_no), coords))

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
                    if (
                        tag.string in punctuation_nows_before or
                        tag.string in punctuation_nows_beforeafter
                    ) and fulltext[-1] == " ":
                        fulltext = fulltext[:-1]
                        word_count -= 1

                    # fix digits
                    if tag.string.isdigit():
                        before = fulltext.split()[-1]
                        if (
                            before[:-1].isdigit() and
                            before[-1] in punctuation_ciffre
                        ):
                            fulltext = fulltext[:-1]
                            word_count -= 1
                fulltext += tag.string + " "
                if tag.string not in string.punctuation:
                    word_count += 1
                char_count += len(list(tag.string))
                if len(fulltext) > 0:
                    if (
                        tag.string in punctuation_nows_after or
                        tag.string in punctuation_nows_beforeafter
                    ) and fulltext[-1] == " ":
                        fulltext = fulltext[:-1]
                        word_count -= 1

    out["fulltext"] = fulltext.strip()

    out["stats"]["word_count"] = int(word_count)
    out["stats"]["chars_count"] = int(char_count)

    out["legacy"]["id"] = identifier
    out["legacy"]["source"] = soup.link['source']
    out["legacy"]["word_count"] = int(soup.meta['wordcnt'])
    out["legacy"]["chars_count"] = int(soup.meta['total_chars_count'])
    suspicious_chars_count = int(soup.meta['suspicious_chars_count'])
    out["legacy"]["suspicious_chars_count"] = int(suspicious_chars_count)
    out["legacy"]["first_id"] = soup.link['first_id']
    out["legacy"]["last_id"] = soup.link['last_id']
    out["legacy"]["next_id"] = soup.link['next_id']
    out["legacy"]["prev_id"] = soup.link['prev_id']

    if root.has_attr('continuation_from'):
        out["legacy"]["continuation_from"] = root['continuation_from']

    if root.has_attr('continuation_to'):
        out["legacy"]["continuation_to"] = root['continuation_to']

    return out


def combine_article_parts(article_parts):
    """TODO.

    :param article_parts: one or more article parts
    :type article_parts: list of dict
    :rtype: a dictionary, with keys "meta", "fulltext", "stats", "legacy"
    """
    if len(article_parts) > 1:
        # if an article has >1 part, retain the metadata
        # from the first item in the list
        article_dict = {
            "meta": {},
            "fulltext": "",
            "stats": {},
            "legacy": {}
        }
        article_dict["legacy"]["id"] = [
            "{}.xml".format(ar["legacy"]["id"])
            for ar in article_parts
        ]
        article_dict["meta"]["type"] = {}
        article_dict["meta"]["type"]["raw"] =\
            article_parts[0]["meta"]["type"]["raw"]

        article_dict["meta"]["title"] = article_parts[0]["meta"]["title"]
        article_dict["meta"]["page_no"] = [
            int(n)
            for ar in article_parts
            for n in ar["meta"]["page_no"]
        ]

        article_dict["meta"]["language"] = {}
        article_dict["meta"]["language"]["raw"] =\
            article_parts[0]["meta"]["language"]["raw"]
        article_dict["meta"]["publication"] =\
            article_parts[0]["meta"]["publication"]
        article_dict["meta"]["issue_date"] =\
            article_parts[0]["meta"]["issue_date"]

        #  using '\f' for now but open to suggestions
        article_dict["fulltext"] =\
            u"\u000C".join(ar["fulltext"] for ar in article_parts)

        # if an article has >1 part, combine the stats (e.g. by
        # summing individual counts)
        article_dict["stats"]["word_count"] = sum(
            [
                ar["stats"]["word_count"]
                for ar in article_parts]
        )

        article_dict["stats"]["chars_count"] = sum(
            [
                ar["stats"]["chars_count"]
                for ar in article_parts]
        )

        article_dict["legacy"]["word_count"] = sum(
            [
                ar["legacy"]["word_count"]
                for ar in article_parts]
        )

        article_dict["legacy"]["chars_count"] = sum(
            [
                ar["legacy"]["chars_count"]
                for ar in article_parts]
        )

        article_dict["legacy"]["suspicious_chars_count"] = sum(
            [
                ar["legacy"]["suspicious_chars_count"]
                for ar in article_parts]
        )
    else:
        article_dict = next(iter(article_parts))
    return article_dict


def import_issue(issue_dir, out_dir, temp_dir=None):
    """Import newspaper issues from a directory structure.

    This function imports a set of newspaper issues from a directory
    structure containing a `Document.zip` file for each issue (in the case of
    Le Temps one issue per day).

    Each ZIP archive is read, and the XML files it contains are transformed
    into the Impresso canonical format, described by a JSON schema. Each
    resulting JSON article is validate against this schema and written to
    file, following an agreed-upon directory structure.

    The intermediate Olive XML files can be kept on request (useful for
    debugging) by passing a `temp_dir` parameter.

    :param issue_dir: an object representing a newspaper issue
    :type issue_dir: `IssueDir`

    :param out_dir: the output directory, where the resulting JSON files
        will be written.
    :type out_dir: string

    :param temp_dir: a directory where to write the intermediate Olive XML
        files (default is `None`); if  `None` these files are not kept.
    :type temp_dir: string

    :rtype: a tuple where [0] the `issue_dir`; [1] a boolean indicating whether
        the import was successful; [2] the exception message if [1] is `False`,
        otherwise `None`.
    """
    out_dir = os.path.join(out_dir, canonical_path(issue_dir, path_type="dir"))
    logger.debug("Importing '{}'...'".format(issue_dir.path))

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if temp_dir is not None:
        temp_dir = os.path.join(
            temp_dir,
            canonical_path(issue_dir, path_type="dir")
        )

    if temp_dir is not None and not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    ns = schemas_to_classes()

    working_archive = os.path.join(issue_dir.path, "Document.zip")
    if os.path.isfile(working_archive):
        try:
            archive = zipfile.ZipFile(working_archive)
        except Exception as e:
            print("Bad zip file.")
            print("Unexpected error:", sys.exc_info()[0])
            print(out_dir + ";bad zip file;\n")
            return (issue_dir, False, e)

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

        logger.debug("XML files contained in {}: {}".format(
            working_archive,
            items
        ))
        while len(items) > 0:
            counter += 1
            out_file = os.path.join(
                out_dir,
                canonical_path(issue_dir, str(counter).zfill(4), ".json")
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
                # legacy code had: `archive.read(item, 'r')` which won't work
                xml_data = archive.read(item)
                new_data = olive_parser(xml_data)

                # check if it needs to be parsed later on
                if new_data["legacy"]['continuation_from'] is not None:
                    target = new_data["legacy"]["continuation_from"]
                    target = [x for x in items if target in x]
                    if len(target) > 0:
                        items.append(item)
                        continue

                article.append(new_data)

                if new_data["legacy"]['continuation_to'] is not None:
                    next_id = new_data["legacy"]["continuation_to"]
                    next_id = [x for x in items if next_id in x][0]
                    internal_deque.append(next_id)
                    items.remove(next_id)

            # aggregate word coordinates
            word_coordinates = reduce(
                lambda x, y: x + y,
                [ar["coordinates"]["words"] for ar in article]
            )

            # aggregate region coordinates
            region_coordinates = reduce(
                lambda x, y: x + y,
                [ar["coordinates"]["regions"] for ar in article]
            )

            # build path of CSV file
            coords_out_file = os.path.join(
                out_dir,
                canonical_path(issue_dir, str(counter).zfill(4), ".coords.csv")
            )

            write_coordinates(
                word_coordinates,
                region_coordinates,
                coords_out_file
            )

            # dispose of the `coordinates` field
            for ar in article:
                del ar["coordinates"]

            article_dict = combine_article_parts(article)

            doctypes_mappings = {
                "article": "ar",
                "page": "p"
            }

            doctype_label = article_dict["meta"]["type"]["raw"].lower()
            doctype_code = doctypes_mappings[doctype_label]

            lang_mappings = {
                "french": "fr",
                "english": "en"
            }

            lang_label = article_dict["meta"]["language"]["raw"].lower()
            lang_code = lang_mappings[lang_label]

            journal_mappings = {
                "GDL": "Gazette de Lausanne",
                "JDG": "Journal de Geneve"
            }

            journal_label = article_dict["meta"]["publication"]
            journal_code = journal_mappings[journal_label]

            article_id = canonical_path(issue_dir, str(counter).zfill(4))
            metadata = ns.Metadata(
                id=article_id,
                title=article_dict["meta"]["title"],
                page_no=article_dict["meta"]["page_no"],
                language=ns.ControlledField(
                    code=lang_code,
                    label=lang_label
                ),
                publication=ns.ControlledField(
                    code=journal_code,
                    label=journal_label
                ),
                type=ns.ControlledField(
                    code=doctype_code,
                    label=doctype_label
                )
            )

            ar = ns.Article(
                meta=metadata,
                fulltext=article_dict["fulltext"],
                stats=article_dict["stats"],
                legacy=article_dict["legacy"]
            )
            ar.validate()

            # serialize to JSON
            with codecs.open(out_file, 'w', 'utf-8') as f:
                json.dump(ar.as_dict(), f, indent=3)
                logger.info(
                    "Written article \'{}\' to {}".format(
                        article_id,
                        out_file
                    )
                )
            article = []

    logger.debug("Done importing '{}'".format(out_dir))
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
                    detected_issue = IssueDir(
                        journal,
                        date(int(year), int(month), int(day)),
                        'a',
                        day_path
                    )
                    logger.debug("Found an issue: {}".format(
                        str(detected_issue))
                    )
                    detected_issues.append(detected_issue)

    return detected_issues


def main(args):
    """Execute the main with CLI parameters."""
    # store CLI parameters
    inp_dir = args["--input-dir"]
    outp_dir = args["--output-dir"]
    temp_dir = args["--temp-dir"]
    log_file = args["--log-file"]
    parallel_execution = args["--parallelize"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO

    # Initialise the logger
    global logger
    logger.setLevel(log_level)

    if(log_file is not None):
        handler = logging.FileHandler(filename=log_file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Logger successfully initialised")

    logger.debug("CLI arguments received: {}".format(args))

    # clean output directory if existing
    if os.path.exists(outp_dir):
        shutil.rmtree(outp_dir)

    # detect issues to be imported
    journal_issues = detect_journal_issues(inp_dir)
    logger.info(
        "Found {} newspaper issues to import".format(
            len(journal_issues)
        )
    )
    logger.debug("Following issues will be imported:{}".format(journal_issues))

    # prepare the execution of the import function
    tasks = [
        delayed(import_issue)(i, outp_dir, temp_dir)
        for i in journal_issues
    ]

    print("\nImporting {} newspaper issues...".format(len(journal_issues)))
    with ProgressBar():
        if parallel_execution:
            result = compute(*tasks, get=mp_get)
        else:
            result = compute(*tasks, get=dask.get)
    print("Done.\n")

    logger.debug(result)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
