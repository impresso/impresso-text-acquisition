"""Functions to parse Olive XML data."""

import codecs
import copy
import re
from typing import Any

from bs4 import BeautifulSoup
from impresso_essentials.io.fs_utils import canonical_path
from impresso_essentials.utils import IssueDir

from text_preparation.importers.olive.helpers import normalize_language, normalize_line


def parse_styles(text: str) -> list[dict[str, Any]]:
    """Turn Olive `styleGallery.txt` file into a dictionary.

    Style IDs may be referred to within the ``s`` property of token elements
    as defined in the impresso JSON schema for newspaper pages (see
    `documentation <https://github.com/impresso/impresso-schemas/blob/master/docs/page.schema.md>`__).
    Each style has ID, font, font size, color (rgb).

    Args:
        text (str): textual content of file `styleGallery.txt`.

    Returns:
        list[dict[str, Any]]: List of styles according to the impresso schema.
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
                    int(i) for i in color.replace("(", "").replace(")", "").split(",")
                ],
            }
        )

    return styles


def olive_image_parser(text: bytes) -> dict[str, str | list] | None:
    """Parse the Olive XML file containing image metadata.

    Args:
        text (bytes): Content of the XML file to parse.

    Returns:
        dict[str, str | list] | None: Dictionary of image metadata.
    """
    soup = BeautifulSoup(text, "lxml")
    root = soup.find("xmd-entity")

    try:
        assert root is not None
        img = {
            "id": root.get("id"),
            "coords": root.img.get("box").split(),
            "name": root.meta.get("name"),
            "resolution": root.meta.get("images_resolution"),
            "filepath": root.img.get("href"),
        }
        return img
    except AssertionError:
        return None


def olive_toc_parser(
    toc_path: str, issue_dir: IssueDir, encoding: str = "windows-1252"
) -> dict[int, dict[str, dict]]:
    """Parse the TOC.xml file (Olive format).

    For each page, the a dict containing page data is created; mapping content
    item legacy IDs to their metadata.

    Args:
        toc_path (str): Path to the ToC XML file.
        issue_dir (IssueDir): Corresponding ``IssueDir`` object.
        encoding (str, optional): File's encoding. Defaults to "windows-1252".

    Returns:
        dict[int, dict[str, dict]]: Dictionary where keys are page numbers and
            values the corresponding page data dictionary.
    """
    with codecs.open(toc_path, "r", encoding) as f:
        text = f.read()

    toc_data = {}

    global_counter = 0

    for page in BeautifulSoup(text, "lxml").find_all("page"):
        page_data = {}

        for n, entity in enumerate(page.find_all("entity")):

            global_counter += 1
            item_legacy_id = entity.get("id")

            item = {
                "legacy_id": item_legacy_id,
                "id": canonical_path(
                    issue_dir, suffix=f"i{str(global_counter).zfill(4)}"
                ),
                "type": entity.get("entity_type"),
                "seq": n + 1,
            }

            # if it's a picture we want to get also the article into which
            # the image is embedded
            if item["type"].lower() == "picture":
                if entity.get("embedded_into") is not None:
                    item["embedded_into"] = entity.get("embedded_into")

            page_data[item_legacy_id] = item

        toc_data[int(page.get("page_no"))] = page_data

    # gather the IDs of all content items int the issue
    ids = [toc_data[page][item]["id"] for page in toc_data for item in toc_data[page]]

    # check that these IDs are unique within the issue
    assert len(ids) == len(list(set(ids)))

    return toc_data


def olive_parser(text: str) -> dict[str, dict | list]:
    """Parse an Olive XML file (e.g. from Le Temps corpus).

    The main logic implemented here was derived from
    <https://github.com/dhlab-epfl/LeTemps-preprocessing/>. Each XML file
    corresponds to one article, as detected by Olive.
    The final dictionary has keys ``meta``, ``r``, ``stats`` and ``legacy``,
    each mapping to dictionaries or lists with the file's parsed contents.

    Args:
        text (str): Contents of the xml file to parse.

    Returns:
        dict[str, dict | list]: Dictionary with parsed contents.
    """
    soup = BeautifulSoup(text, "lxml")
    root = soup.find("xmd-entity")
    page_no = root["page_no"]
    identifier = root["id"]
    language = root["language"]
    title = soup.meta["name"]
    entity_type = root["entity_type"]
    issue_date = soup.meta["issue_date"]

    out = {
        "meta": {"language": None, "type": {}},
        "r": [],
        "stats": {},
        "legacy": {"continuation_from": None, "continuation_to": None},
    }
    out["meta"]["title"] = title
    out["meta"]["page_no"] = [int(page_no)]
    out["meta"]["language"] = normalize_language(language)
    out["meta"]["type"]["raw"] = entity_type
    out["meta"]["issue_date"] = issue_date

    new_region = {"c": [], "p": []}

    new_paragraph = {"l": []}

    new_line = {"c": [], "t": []}

    new_token = {"c": [], "tx": ""}

    for primitive in soup.find_all("primitive"):

        # store coordinate of text areas (boxes) by page
        # 1) page number, 2) coordinate list
        region = copy.deepcopy(new_region)
        region["c"] = [int(i) for i in primitive.get("box").split(" ")]

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
                line["c"] = [int(i) for i in tag.get("box").split(" ")]
                line_counter += 1

            if tag.name in ["w", "q"]:

                # store coordinates of each token
                # 1) token, 2) page number, 3) coordinate list
                t = copy.deepcopy(new_token)
                t["c"] = [int(i) for i in tag.get("box").split(" ")]
                t["tx"] = tag.string
                t["s"] = int(tag.get("style_ref"))

                if tag.name == "q" and tag.get("qid") is not None:
                    qid = tag.get("qid")
                    normalized_form = soup.find("qw", qid=qid).text
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
    out["legacy"]["source"] = soup.link["source"]
    out["legacy"]["first_id"] = soup.link["first_id"]
    out["legacy"]["last_id"] = soup.link["last_id"]
    out["legacy"]["next_id"] = soup.link["next_id"]
    out["legacy"]["prev_id"] = soup.link["prev_id"]

    if root.has_attr("continuation_from"):
        out["legacy"]["continuation_from"] = root["continuation_from"]

    if root.has_attr("continuation_to"):
        out["legacy"]["continuation_to"] = root["continuation_to"]

    return out
