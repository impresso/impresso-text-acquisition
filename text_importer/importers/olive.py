"""The Olive XML OCR importer."""

import codecs
import copy
import logging
import os
import re
import sys
import zipfile
from collections import deque
from operator import itemgetter
from time import strftime

from bs4 import BeautifulSoup

from impresso_commons.path.path_fs import canonical_path
from text_importer.helpers import (convert_page_coordinates, get_image_info,
                                   get_issue_schema, get_page_schema,
                                   keep_title, normalize_language,
                                   serialize_issue, serialize_page,
                                   convert_image_coordinates)
from text_importer.tokenization import insert_whitespace

logger = logging.getLogger(__name__)


def olive_toc_parser(toc_path, issue_dir, encoding="windows-1252"):
    """Parse the TOC.xml file (Olive format)."""
    with codecs.open(toc_path, 'r', encoding) as f:
        text = f.read()

    toc_data = {}

    global_counter = 0

    for page in BeautifulSoup(text, 'lxml').find_all('page'):
        page_data = {}

        for n, entity in enumerate(page.find_all("entity")):

            global_counter += 1
            item_legacy_id = entity.get("id")

            item = {
                "legacy_id": item_legacy_id,
                "id": canonical_path(
                    issue_dir,
                    name=f"i{str(global_counter).zfill(4)}",
                    extension=""
                ),
                "type": entity.get("entity_type"),
                "seq": n + 1
            }

            # if it's a picture we want to get also the article into which
            # the image is embedded
            if item['type'].lower() == "picture":
                if entity.get("embedded_into") is not None:
                    item['embedded_into'] = entity.get("embedded_into")

            page_data[item_legacy_id] = item

        toc_data[int(page.get('page_no'))] = page_data

    # gather the IDs of all content items int the issue
    ids = [
        toc_data[page][item]["id"]
        for page in toc_data
        for item in toc_data[page]
    ]

    # check that these IDs are unique within the issue
    assert len(ids) == len(list(set(ids)))

    return toc_data


def normalize_hyphenation(line):
    """Normalize end-of-line hyphenated words.

    :param line: a line of OCR in JSON format
    :type line: dict (keys: coords, tokens)
    :rtype: dict (keys: coords, tokens)
    """
    for i, token in enumerate(line["t"]):
        if i == (len(line["t"]) - 1):
            if token["tx"][-1] == "-":
                token["hy"] = True
            if token["tx"] == "-" and "nf" in token:
                prev_token = line["t"][i - 1]
                line["t"] = line["t"][:-2]
                merged_token = {
                    "tx": "".join([prev_token["tx"], token["tx"]]),
                    "c": prev_token["c"][:2] + token["c"][2:],
                    "s": token["s"],
                    "hy": token["hy"]
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
        "s": tokens[0]["s"]
    }
    logger.debug(
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
        if len(tokens) > 1:
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

        if len(tokens_to_merge) >= 2:
            insertion_point = tokens[0][0]
            merged_token = merge_tokens(tokens_to_merge, original_line)
            line["t"].insert(insertion_point, merged_token)

    return line


def normalize_line(line, lang):
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

    for i, token in enumerate(line["t"]):
        if "qid" not in token and "nf" in token:
            del token["nf"]

        if "qid" in token:
            del token["qid"]

        if i == 0 and i != len(line["t"]) - 1:
            insert_ws = insert_whitespace(
                token["tx"],
                line["t"][i+1]["tx"],
                None,
                lang
            )

        elif i == 0 and i == len(line["t"]) - 1:
            insert_ws = insert_whitespace(
                token["tx"],
                None,
                None,
                lang
            )

        elif i == len(line["t"]) - 1:
            insert_ws = insert_whitespace(
                token["tx"],
                None,
                line["t"][i-1]["tx"],
                lang
            )

        else:
            insert_ws = insert_whitespace(
                token["tx"],
                line["t"][i+1]["tx"],
                line["t"][i-1]["tx"],
                lang
            )
        if not insert_ws:
            token["gn"] = True

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
    entity_type = root['entity_type']
    issue_date = soup.meta['issue_date']

    out = {
        "meta": {
            "language": None,
            "type": {}
        },
        "r": [],
        "stats": {},
        "legacy": {"continuation_from": None, "continuation_to": None},
    }
    out["meta"]["title"] = title
    out["meta"]["page_no"] = [int(page_no)]
    out["meta"]["language"] = normalize_language(language)
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

        para = None
        line = None
        line_counter = 0

        for tag in primitive.find_all(recursive=False):

            if tag.name == "l":

                if para is None and line is None:
                    para = copy.deepcopy(new_paragraph)
                    line = copy.deepcopy(new_line)

                if line_counter > 0 and line is not None:
                    line = normalize_line(line, out["meta"]["language"])
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
                    t["nf"] = normalized_form
                    t["qid"] = qid

                # append the token to the line
                line["t"].append(t)

        # append orphan lines
        if line is not None:
            line = normalize_line(line, out["meta"]["language"])
            para["l"].append(line)

        region["p"].append(para)

        if para is not None:
            out["r"].append(region)

    out["legacy"]["id"] = identifier
    out["legacy"]["source"] = soup.link['source']
    """
    # I suspect this could be deleted
    out["legacy"]["word_count"] = int(soup.meta['wordcnt'])
    out["legacy"]["chars_count"] = int(soup.meta['total_chars_count'])
    suspicious_chars_count = int(soup.meta['suspicious_chars_count'])
    out["legacy"]["suspicious_chars_count"] = int(suspicious_chars_count)
    """
    out["legacy"]["first_id"] = soup.link['first_id']
    out["legacy"]["last_id"] = soup.link['last_id']
    out["legacy"]["next_id"] = soup.link['next_id']
    out["legacy"]["prev_id"] = soup.link['prev_id']

    if root.has_attr('continuation_from'):
        out["legacy"]["continuation_from"] = root['continuation_from']

    if root.has_attr('continuation_to'):
        out["legacy"]["continuation_to"] = root['continuation_to']

    return out


def olive_image_parser(text):
    soup = BeautifulSoup(text, "lxml")
    root = soup.find("xmd-entity")

    img = {
        'id': root.get('id'),
        'coords': root.img.get('box').split(),
        'name': root.meta.get('name'),
        'resolution': root.meta.get('images_resolution'),
        'filepath': root.img.get('href')
    }
    return img


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
            ar["legacy"]["id"]
            for ar in article_parts
        ]
        article_dict["legacy"]["source"] = [
            ar["legacy"]["source"]
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

        # TODO: remove from production
        if len(article_dict["meta"]["page_no"]) > 1:
            # pdb.set_trace()
            pass

        article_dict["meta"]["language"] = {}
        article_dict["meta"]["language"] =\
            article_parts[0]["meta"]["language"]
        article_dict["meta"]["issue_date"] =\
            article_parts[0]["meta"]["issue_date"]
    elif len(article_parts) == 1:
        article_dict = next(iter(article_parts))
    else:
        article_dict = None
    return article_dict


def parse_styles(text):
    """Turn Olive style file into a dictionary.

    :param text: textual content of file `styleGallery.txt`
    :type text: str
    :rtype: list of dicts
    """
    styles = []
    regex = r'(\d{3})=(".*?"),(\d+\.?\d+),(\(.*?\))'

    for line in text.split("\r\n"):

        if line == "":
            continue

        n, font, font_size, color = re.match(regex, line).groups()
        styles.append(
            {
                "id": int(n),
                "f": font.replace('"', ""),
                "fs": float(font_size),
                "rgb": [
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
        "r": [],
        "cdt": strftime("%Y-%m-%d %H:%M:%S")
    }
    ordered_elements = sorted(
        list(info_from_toc.values()), key=itemgetter('seq')
    )

    # put together the regions while keeping the order in the page
    for el in ordered_elements:

        # keep only IDS of content items that are Ads or Articles
        # but escluding various other files in the archive
        if ("Ar" not in el["legacy_id"] and "Ad" not in el["legacy_id"]):
            continue

        element = page_elements[el["legacy_id"]]

        for i, region in enumerate(element["r"]):
            region["pOf"] = el["id"]

        page["r"] += element["r"]

    return page


def recompose_ToC(toc_data, articles, images):
    """TODO."""
    # concate content items from all pages into a single flat list
    content_items = [
        toc_data[pn][elid]
        for pn in toc_data.keys() for elid in toc_data[pn].keys()
    ]

    # filter out those items that are part of a multipart article
    contents = []
    sorted_content_items = sorted(content_items, key=itemgetter('seq'))
    for item in sorted_content_items:

        item['m'] = {}
        item["l"] = {}

        if(item["type"] == "Article" or item["type"] == "Ad"):

            # find the corresponding item in `articles`
            # by using `legacy_id` as the search key
            # if not found (raises exception) means that it's one of the
            # multipart articles, and it's ok to skip it
            legacy_id = item['legacy_id']
            article = None
            for ar in articles:
                if isinstance(ar["legacy"]["id"], list):
                    if ar["legacy"]["id"][0] == legacy_id:
                        article = ar
                else:
                    if ar["legacy"]["id"] == legacy_id:
                        article = ar

            try:
                assert article is not None
            except Exception:
                continue


            item['m']["id"] = item["id"]
            item['m']['pp'] = article["meta"]["page_no"]
            item['m']['l'] = article["meta"]["language"]
            item['m']['tp'] = article["meta"]["type"]["raw"].lower()

            if keep_title(article["meta"]["title"]):
                item['m']['t'] = article["meta"]["title"]

            item["l"]["id"] = article["legacy"]["id"]
            item["l"]["source"] = article["legacy"]["source"]

        elif(item["type"] == "Picture"):

            # find in which page the image is
            page_no = [
                page_no
                for page_no in toc_data
                if item['legacy_id'] in toc_data[page_no]
            ]

            # get the new canonical id via the legacy id
            item['m']['id'] = item['id']
            item['m']['tp'] = item['type'].lower()
            item['m']['pp'] = page_no

            image = [
                image
                for image in images
                if image['id'] == item['legacy_id']
            ][0]

            if keep_title(image["name"]):
                item['m']['t'] = image["name"]

            item['l']['id'] = item['legacy_id']
            item['l']['res'] = image['resolution']
            item['l']['path'] = image['filepath']

            item['c'] = image['coords']
            toc_item = toc_data[page_no[0]][item['legacy_id']]

            if "embedded_into" in item:
                containing_article = toc_item['embedded_into']

                # content item entries exists in different shapes within the
                # `toc_data` dict, depending on whether they have already been
                # processed in this `for` loop or not
                if "m" in toc_data[page_no[0]][containing_article]:
                    item['pOf'] = toc_data[page_no[0]][containing_article]['m']['id']
                else:
                    item['pOf'] = toc_data[page_no[0]][containing_article]['id']

        # delete redundant fields
        if "embedded_into" in item:
            del item['embedded_into']
        del item['seq']
        del item['legacy_id']
        del item['type']
        del item['id']

        contents.append(item)
    return contents


def check_consistency(issue_obj, element_ids):
    """Perform a consistency check of Issue against the Olive files.

    Ensure that all article ids from the Olive package are found in the Issue
    object. Separate article components may have been merged into a single
    ToC item, so it's necessary to compare the set of legacy IDs with the
    set of all element IDs found within the Olive files.
    """
    # check that the IDs from the ToC.XML are found also in the issue.json
    all_item_ids = []
    for item in issue_obj.i:
        if isinstance(item["l"]["id"], list):
            all_item_ids += item["l"]["id"]
        else:
            all_item_ids.append(item["l"]["id"])

    difference_btw_sets = set(element_ids).difference(set(all_item_ids))

    try:
        assert len(difference_btw_sets) == 0
        logger.info("{} consistency check ok".format(issue_obj.id))
    except AssertionError as e:
        logger.warning(
            "There are missing elements in the issue.json: {}".format(
                difference_btw_sets
            )
        )
        raise e


def olive_import_issue(
    issue_dir,
    image_dir,
    out_dir=None,
    s3_bucket=None,
    temp_dir=None
):
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
    logger.info("Started importing '{}'...'".format(issue_dir.path))

    if out_dir is not None:

        out_dir = os.path.join(
            out_dir,
            canonical_path(issue_dir, path_type="dir")
        )

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
    IssueSchema = get_issue_schema()
    pages = {}

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
        content_elements = []
        items = sorted(
            [
                item
                for item in archive.namelist()
                if ".xml" in item
                and not item.startswith("._")
                and ("/Ar" in item or "/Ad" in item)
            ]
        )

        page_xml_files = {
            int(item.split("/")[0]): item
            for item in archive.namelist()
            if ".xml" in item and not item.startswith("._") and "/Pg" in item
        }

        image_xml_files = [
            item
            for item in archive.namelist()
            if ".xml" in item and not item.startswith("._") and "/Pc" in item
        ]

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
        try:
            toc_data = olive_toc_parser(toc_path, issue_dir)
            logger.debug(toc_data)
        except FileNotFoundError as e:
            logger.error(f'Missing ToC.xml for {issue_dir.path}')
            logger.error(e)
            return (issue_dir, False, e)
        except Exception as e:
            logger.error(f'Corrupted ToC.xml for {issue_dir.path}')
            logger.error(e)
            return (issue_dir, False, e)

        # parse the XML files into JSON dicts
        images = [
            olive_image_parser(archive.read(image_file))
            for image_file in image_xml_files
        ]

        logger.debug("XML files contained in {}: {}".format(
            working_archive,
            items
        ))
        articles = []
        all_element_ids = []

        # recompose each article by following the continuation links
        article_parts = []
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
                try:
                    xml_data = archive.read(item).decode('windows-1252')
                    new_data = olive_parser(xml_data)
                except Exception as e:
                    logger.error(f'Corrupted zip archive for {issue_dir.path}')
                    logger.error(e)
                    return (issue_dir, False, e)

                # check if it needs to be parsed later on
                if new_data["legacy"]['continuation_from'] is not None:
                    target = new_data["legacy"]["continuation_from"]
                    target = [x for x in items if target in x]
                    if len(target) > 0:
                        items.append(item)
                        continue

                article_parts.append(new_data)

                if new_data["legacy"]['continuation_to'] is not None:
                    next_id = new_data["legacy"]["continuation_to"]
                    next_id = [x for x in items if next_id in x][0]
                    internal_deque.append(next_id)
                    items.remove(next_id)

            try:
                content_elements += article_parts
                combined_article = combine_article_parts(article_parts)

                if combined_article is not None:
                    articles.append(combined_article)

                article_parts = []
            except Exception as e:
                """
                logger.error("Import of issue {} failed with error {}".format(
                    issue_dir,
                    e
                ))
                return
                """
                raise e

        # at this point the articles have been recomposed
        # but we still need to recompose pages
        for page_no in toc_data:
            logger.info(f'Processing page {page_no} of {issue_dir.path}')
            info_from_toc = toc_data[page_no]
            element_ids = toc_data[page_no].keys()

            # create a list of all element IDs for the issue.json
            all_element_ids = [
                el_id
                for el_id in toc_data[page_no].keys()
                if "Ar" in el_id or "Ad" in el_id
            ]

            elements = {
                el["legacy"]["id"]: el
                for el in content_elements
                if (el["legacy"]["id"] in element_ids)
            }
            page_dict = recompose_page(page_no, info_from_toc, elements)
            page_dict['id'] = canonical_path(
                issue_dir,
                "p" + str(page_no).zfill(4)
            )
            page = PageSchema(**page_dict)
            pages[page_no] = page

            # flag those cases where the Olive XML does not contain any OCR
            if len(page.r) == 0:
                logger.warning(f"Page {page.id} has no OCR text")

        contents = recompose_ToC(toc_data, articles, images)
        issue_data = {
            "id": canonical_path(issue_dir, path_type="dir").replace("/", "-"),
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "s": styles,
            "i": contents,
            "pp": [pages[page_no].id for page_no in pages]
        }
        issue = IssueSchema(**issue_data)
        try:
            image_info = get_image_info(issue_dir, image_dir)
        except FileNotFoundError:
            logger.error(f"Missing image-info.json file for {issue_dir.path}")

        for page_no in pages:
            page = pages[page_no]

            # fail gracefully if no entry in image-info.json for this page
            try:
                image_info_record = [
                    page
                    for page in image_info
                    if int(page['pg']) == page_no
                ][0]
                box_strategy = image_info_record['strat']
                image_name = image_info_record['s']
                page_xml_file = page_xml_files[page_no]
            except Exception as e:
                logger.error("Page {} in {} raised error: {}".format(
                    page_no,
                    issue_data.id,
                    e
                ))
                logger.error(
                    "Couldn't get information about page img {} in {}".format(
                        page_no,
                        issue_data.id
                    )
                )

            # TODO move this to the helper function
            # convert the box coordinates
            try:
                convert_page_coordinates(
                    page,
                    archive.read(page_xml_file),
                    image_name,
                    archive,
                    box_strategy,
                    issue_dir
                )
                pages[page_no].cc = True
            except Exception as e:
                logger.error("Page {} in {} raised error: {}".format(
                    page_no,
                    issue_data.id,
                    e
                ))
                logger.error(
                    "Couldn't convert coordinates in p. {} {}".format(
                        page_no,
                        issue_data.id
                    )
                )
                pages[page_no].cc = False

            """
            conversion of image coordinates:
            - fetch all images belonging to current page
            - for each image, convert and set .cc=True if ok
            """

            images_in_page = [
                content_item
                for content_item in issue.i
                if content_item.m.tp == "picture" and page_no in
                content_item.m.pp
            ]
            for image in images_in_page:
                image = convert_image_coordinates(
                    image,
                    archive.read(page_xml_file),
                    image_name,
                    archive,
                    box_strategy,
                    issue_dir
                )
                image.m.tp =  'image'

            # serialize the page object to JSON
            if out_dir is not None:
                serialize_page(
                    page_no,
                    page,
                    issue_dir,
                    out_dir=out_dir
                )
            elif s3_bucket is not None:
                serialize_page(
                    page_no,
                    page,
                    issue_dir,
                    s3_bucket=s3_bucket
                )

        if out_dir is not None:
            serialize_issue(issue, issue_dir, out_dir=out_dir)
        elif s3_bucket is not None:
            serialize_issue(issue, issue_dir, s3_bucket=s3_bucket)

        check_consistency(issue, all_element_ids)

        # 2. (TODO) check that each item in the issue.json
        # has at least one text region in the corresponding page file

    logger.info("Done importing '{}'".format(out_dir))
    return (issue_dir, True, None)
