"""The Olive XML OCR importer."""

import codecs
import ipdb as pdb
import copy
import json
import logging
import os
import re
import sys
import zipfile
from collections import deque
from operator import itemgetter

from bs4 import BeautifulSoup
from impresso_commons.path import canonical_path
from text_importer.helpers import get_page_schema, serialize_page

logger = logging.getLogger(__name__)


def olive_toc_parser(toc_path, issue_dir, encoding="windows-1252"):
    """TODO."""
    with codecs.open(toc_path, 'r', encoding) as f:
        text = f.read()

    return {
        int(page.get('page_no')): {
            entity.get("id"): {
                "id": entity.get("id"),
                "canonical_id": canonical_path(
                    issue_dir,
                    name="i" + entity.get("index_in_doc").zfill(4),
                    extension=""
                ),
                "type": entity.get("entity_type"),
                "seq": int(entity.get("index_in_doc"))
            }
            for entity in page.find_all("entity")
        }
        for page in BeautifulSoup(text, 'lxml').find_all('page')
    }


def normalize_hyphenation(line):
    """Normalize end-of-line hyphenated words.

    :param line: a line of OCR in JSON format
    :type line: dict (keys: coords, tokens)
    :rtype: dict (keys: coords, tokens)
    """
    for i, token in enumerate(line["t"]):
        if i == (len(line["t"]) - 1):
            if token["tx"] == "-" and "norm_form" in token:
                prev_token = line["t"][i - 1]
                line["t"] = line["t"][:-2]
                merged_token = {
                    "tx": "".join([prev_token["tx"], token["tx"]]),
                    "c": prev_token["c"][:2] + token["c"][2:],
                    "s": token["s"]
                }
                logger.debug(
                    "Merged {} and {} => {}".format(
                        prev_token,
                        token,
                        merged_token
                    )
                )
                line["t"].append(merged_token)
    return line


def merge_tokens(tokens, line):
    merged_token = {
        "tx": "".join(
            [
                token["tx"]
                for token in tokens
            ]
        ),
        "c": tokens[0]["c"][:2] + tokens[-1]["c"][2:],
        "norm_form": tokens[0]["norm_form"],
        "s": tokens[0]["s"]
    }
    logger.warning(
        "(In-line pseudo tokens) Merged {} => {} in line \"{}\"".format(
            "".join([t["tx"] for t in tokens]),
            merged_token["tx"],
            line
        )
    )
    return merged_token


def merge_pseudo_tokens(line):
    """Remove pseudo tokens from a line.

    :param line: a line of OCR in JSON format
    :type line: dict (keys: coords, tokens)
    :rtype: dict (keys: coords, tokens)
    """
    original_line = " ".join([t["tx"] for t in line["t"]])
    qids = set([
        token["qid"]
        for token in line["t"]
        if "qid" in token
    ])

    inline_qids = []

    for qid in qids:
        tokens = [
            (i, token)
            for i, token in enumerate(line["t"])
            if "qid" in token and token["qid"] == qid
        ]
        last_token_idx, last_token = tokens[-1]

        # FIXME: the problem with this is that it misses those cases
        # where the pseudo tokens occur at the end of the line.
        # TODO: check if the first token of the next line has the same `qid`
        if last_token_idx < (len(line["t"]) - 1):
            inline_qids.append(qid)

    if len(inline_qids) == 0:
        return line

    for qid in inline_qids:
        # identify tokens to merge
        tokens = [
            (i, token)
            for i, token in enumerate(line["t"])
            if "qid" in token and token["qid"] == qid
        ]

        # remove tokens to merge from the line
        tokens_to_merge = [
            line["t"].pop(
                line["t"].index(token)
            )
            for i, token in tokens
        ]

        if len(tokens_to_merge) < 2:
            # check if this occurs
            # pdb.set_trace()
            pass
        else:
            insertion_point = tokens[0][0]
            merged_token = merge_tokens(tokens_to_merge, original_line)
            line["t"].insert(insertion_point, merged_token)
    return line


def normalize_line(line):
    """Apply normalization to a line of OCR.

    :param line: a line of OCR text
    :type line: dict (keys: coords, tokens)
    :rtype: dict (keys: coords, tokens)
    """
    mw_tokens = [
        token
        for token in line["t"]
        if "qid" in token
    ]
    # apply normalization only to those lines that contain at least one
    # multi-word token (denoted by presence of `qid` field)
    if len(mw_tokens) > 0:
        line = merge_pseudo_tokens(line)
        line = normalize_hyphenation(line)

    return line


def olive_parser(text):
    u"""Parse an Olive XML file (e.g. from Le Temps corpus).

    The main logic implemented here was derived from
    <https://github.com/dhlab-epfl/LeTemps-preprocessing/>. Each XML file
    corresponds to one article, as detected by Olive.

    :param text: a string with the textual xml file to parse
    :type text: string
    :rtype: a dictionary, with keys "meta", °stats", "legacy"
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
        "r": [],
        "stats": {},
        "legacy": {"continuation_from": None, "continuation_to": None},
    }
    out["meta"]["title"] = title
    out["meta"]["page_no"] = [int(page_no)]
    out["meta"]["language"]["raw"] = language
    out["meta"]["publication"] = publication
    out["meta"]["type"]["raw"] = entity_type
    out["meta"]["issue_date"] = issue_date

    new_region = {
        "c": [],
        "p": []
    }

    new_paragraph = {
        "l": []
    }

    new_line = {
        "c": [],
        "t": []
    }

    new_token = {
        "c": [],
        "tx": ""
    }

    for primitive in soup.find_all("primitive"):

        # store coordinate of text areas (boxes) by page
        # 1) page number, 2) coordinate list
        region = copy.deepcopy(new_region)
        region["c"] = [int(i) for i in primitive.get('box').split(" ")]
        region["n"] = page_no

        para = None
        line = None
        line_counter = 0

        for tag in primitive.find_all(recursive=False):

            if tag.name == "l":

                if para is None and line is None:
                    para = copy.deepcopy(new_paragraph)
                    line = copy.deepcopy(new_line)

                if line_counter > 0 and line is not None:
                    line = normalize_line(line)
                    para["l"].append(line)

                if tag.get("p") in ["S", "SA"] and line_counter > 0:
                    region["p"].append(para)
                    para = copy.deepcopy(new_paragraph)

                line = copy.deepcopy(new_line)
                line["c"] = [
                    int(i)
                    for i in tag.get('box').split(" ")
                ]
                line_counter += 1

            if tag.name in ["w", "q"]:

                # store coordinates of each token
                # 1) token, 2) page number, 3) coordinate list
                t = copy.deepcopy(new_token)
                t["c"] = [int(i) for i in tag.get('box').split(" ")]
                t["tx"] = tag.string
                t["s"] = int(tag.get('style_ref'))

                if tag.name == "q" and tag.get('qid') is not None:
                    qid = tag.get('qid')
                    normalized_form = soup.find('qw', qid=qid).text
                    t["norm_form"] = normalized_form
                    t["qid"] = qid

                # append the token to the line
                line["t"].append(t)

        # append orphan lines
        if line is not None:
            line = normalize_line(line)
            para["l"].append(line)

        region["p"].append(para)

        if para is not None:
            out["r"].append(region)

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
    else:
        article_dict = next(iter(article_parts))
    return article_dict


def parse_styles(text):
    """Turn Olive style file into a dictionary.

    :param text: textual content of file `styleGallery.txt`
    :type text: str
    :rtype: list of dicts
    """
    styles = []
    regex = r'(\d{3})=("\D+?),(\d+\.?\d+),(\(.*?\))'

    for line in text.split("\r\n"):

        if line == "":
            continue

        n, font, font_size, color = re.match(regex, line).groups()
        styles.append(
            {
                "id": int(n),
                "font": font.replace('"', ""),
                "font_size": float(font_size),
                "rgb_color": [
                    int(i)
                    for i in color.replace("(", "").replace(")", "").split(",")
                ]
            }
        )

    return styles


def recompose_page(page_number, info_from_toc, page_elements):
    """Create a page document starting from a list of page documents.

    :param page_number: page number
    :type page_number: int
    :param info_from_toc: a dictionary with page element IDs (articles, ads.)
        as keys, and dictionaries as values
    :type info_from_toc:
    :param page_elements: articles or advertisements
    :type page_elements: list of dict

    It's here that `n` attributes are assigned to each region/para/line/token.
    """
    page = {
        "r": []
    }
    ordered_elements = sorted(
        list(info_from_toc.values()), key=itemgetter('seq')
    )

    # put together the regions while keeping the order in the page
    for el in ordered_elements:

        # filter out the ids keeping only Ads or Articles
        # but escluding various other files in the archive
        if ("Ar" not in el["id"] or "Ar" not in el["id"]):
            continue

        element = page_elements[el["id"]]

        for i, region in enumerate(element["r"]):
            region["pOf"] = el["canonical_id"]

        page["r"] += element["r"]

    return page


def olive_import_issue(issue_dir, out_dir, temp_dir=None):
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
    logger.info("Started importing '{}'...'".format(issue_dir.path))

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if temp_dir is not None:
        temp_dir = os.path.join(
            temp_dir,
            canonical_path(issue_dir, path_type="dir")
        )

    if temp_dir is not None and not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    PageSchema = get_page_schema()

    issue_contents = []

    working_archive = os.path.join(issue_dir.path, "Document.zip")
    if os.path.isfile(working_archive):
        try:
            archive = zipfile.ZipFile(working_archive)
        except Exception as e:
            print("Bad zip file.")
            print("Unexpected error:", sys.exc_info()[0])
            print(out_dir + ";bad zip file;\n")
            return (issue_dir, False, e)

        # parse the XML files in the .zip archive
        counter = 0
        article = []
        items = sorted(
            [
                item
                for item in archive.namelist()
                if ".xml" in item
                and not item.startswith("._")
                and ("/Ar" in item or "/Ad" in item)
            ]
        )

        logger.debug("Contents: {}".format(archive.namelist()))

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

        # parse the `styleGallery.txt` file
        if 'styleGallery.txt' in archive.namelist():
            try:
                styles = parse_styles(
                    archive.read('styleGallery.txt').decode()
                )
            except Exception as e:
                logger.warning(
                    "Parsing style file in {} failed with error {}".format(
                        working_archive,
                        e
                    )
                )
                styles = []

        # parse the TOC
        toc_path = os.path.join(issue_dir.path, "TOC.xml")
        toc_data = olive_toc_parser(toc_path, issue_dir)
        logger.debug(toc_data)

        logger.debug("XML files contained in {}: {}".format(
            working_archive,
            items
        ))
        articles = []
        while len(items) > 0:
            counter += 1

            # if out file already exists skip the data it contains
            # TODO: change this to work with the JSON output
            """
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
            """

            internal_deque = deque([items[0]])
            items = items[1:]

            while len(internal_deque) > 0:
                item = internal_deque.popleft()
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

            articles += article

            article = []

        # at this point the articles have been recomposed
        # but we still need to recompose pages
        for page_no in toc_data:

            info_from_toc = toc_data[page_no]
            element_ids = toc_data[page_no].keys()
            elements = {
                el["legacy"]["id"]: el
                for el in articles
                if (el["legacy"]["id"] in element_ids)
            }
            page_dict = recompose_page(page_no, info_from_toc, elements)
            issue_contents += list(elements.values())
            page = PageSchema(**page_dict)
            serialize_page(
                page_no, page,
                issue_dir,
                s3_bucket="canonical-json"
            )

        # TODO: combine info in `articles`
        content_items = [
            toc_data[pn][elid]
            for pn in toc_data.keys() for elid in toc_data[pn].keys()
        ]

        contents = {
            "articles": [i for i in content_items if i["type"] == "Article"],
            "ads": [i for i in content_items if i["type"] == "Ad"],
            "styles": styles
        }

        contents_filename = os.path.join(
            out_dir,
            canonical_path(issue_dir, "issue", extension=".json")
        )

        with codecs.open(contents_filename, 'w', 'utf-8') as f:
            json.dump(contents, f, indent=3)

    logger.debug("Done importing '{}'".format(out_dir))
    return (issue_dir, True, None)
