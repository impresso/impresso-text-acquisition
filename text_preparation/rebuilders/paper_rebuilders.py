"""Rebuild helpers for the paper (print, typescript) text data."""

import logging
from typing import Optional, Any
from impresso_essentials.text_utils import insert_whitespace

logger = logging.getLogger(__name__)


def rebuild_paper_text(
    page: list[dict], language: Optional[str], string: Optional[str] = None
) -> tuple[str, dict[list], dict[list]]:
    """Rebuild the text of an article for Solr ingestion.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, dict[list], dict[list]]: [0] CI fulltext, [1] offsets and
            [2] coordinates of token regions.
    """

    coordinates = {"regions": [], "tokens": []}

    offsets = {"line": [], "para": [], "region": []}

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)
    for reg in page:

        if len(string) > 0:
            offsets["region"].append(len(string))

        coordinates["regions"].append(reg["c"])

        for para in reg["p"]:

            if len(string) > 0:
                offsets["para"].append(len(string))

            for line in para["l"]:

                for n, token in enumerate(line["t"]):
                    region = {}
                    if "c" not in token:
                        print(f"'c' was not present in token: {token}, line: {line}")
                        continue
                    region["c"] = token["c"]
                    region["s"] = len(string)

                    token_text = None
                    if "hy" in token:
                        region["l"] = len(token["tx"][:-1]) - 1
                        region["hy1"] = True
                    elif "nf" in token:
                        region["l"] = len(token["nf"])
                        region["hy2"] = True

                        token_text = token["nf"] if token["nf"] is not None else ""
                    else:
                        if token["tx"]:
                            region["l"] = len(token["tx"])
                        else:
                            region["l"] = 0

                        token_text = token["tx"] if token["tx"] is not None else ""

                    # don't add the tokens corresponding to the first part of a hyphenated word
                    if "hy" not in token:
                        next_token = line["t"][n + 1]["tx"] if n != len(line["t"]) - 1 else None
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=next_token,
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        string += f"{token_text} " if ws else f"{token_text}"

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        if "hy" in token:
                            offsets["line"].append(region["s"])
                        else:
                            # TODO check if possible to add a space after a coma or period
                            token_length = len(token["tx"]) if token["tx"] else 0
                            offsets["line"].append(region["s"] + token_length)

                    coordinates["tokens"].append(region)

    return (string, coordinates, offsets)


def rebuild_paper_text_passim(
    page: list[dict], language: Optional[str], string: Optional[str] = None
) -> tuple[str, list[dict]]:
    """The text rebuilding function from pages for passim.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, list[dict]]: [0] article fulltext, [1] coordinates of token regions.
    """

    regions = []

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)

    for region in page:

        for para in region["p"]:

            for line in para["l"]:

                for n, token in enumerate(line["t"]):

                    region_string = ""

                    if "c" not in token:
                        # if the coordniates are missing, they should be skipped
                        logger.debug("Missing 'c' in token %s", token)
                        print(f"Missing 'c' in token {token}")
                        continue

                    # each page region is a token
                    output_region = {
                        "start": None,
                        "length": None,
                        "coords": {
                            "x": token["c"][0],
                            "y": token["c"][1],
                            "w": token["c"][2],
                            "h": token["c"][3],
                        },
                    }

                    if len(string) == 0:
                        output_region["start"] = 0
                    else:
                        output_region["start"] = len(string)

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        tmp = f"{token['tx']}\n"
                        region_string += tmp
                    else:
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=line["t"][n + 1]["tx"],
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        region_string += f"{token['tx']} " if ws else f"{token['tx']}"

                    string += region_string
                    output_region["length"] = len(region_string)
                    regions.append(output_region)

    return (string, regions)


def recompose_ci_from_page_solr(
    solr_ci: dict[str, Any], content_item: dict[str, Any]
) -> dict[str, Any]:
    """Given a partly constructed solr rebuilt CI, reconstruct the page elements.

    The parts added are the components of the `rebuilt pages`, `ppreb` composed of regions,
    paragraphs, lines and tokens.
    Then the fulltext and the offsets of the breaks separating each element are also added
    to the Solr representation.

    Args:
        solr_ci (dict[str, Any]): Solr/rebuilt representation of the CI to complete.
        content_item (dict[str, Any]): Temporary version of the canonical CI used to reconstruct.

    Returns:
        dict[str, Any]: Rebuilt CI with the recomposed audio elements added to it.
    """
    issue_id = "-".join(solr_ci["id"].split("-")[:-1])
    page_file_names = {p: f"{issue_id}-p{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]}

    fulltext = ""
    linebreaks = []
    parabreaks = []
    regionbreaks = []
    solr_ci["ppreb"] = []

    for n, page_no in enumerate(solr_ci["pp"]):

        page = content_item["pprr"][n]

        if fulltext == "":
            fulltext, coords, offsets = rebuild_paper_text(page, solr_ci["lg"])
        else:
            fulltext, coords, offsets = rebuild_paper_text(page, solr_ci["lg"], fulltext)

        linebreaks += offsets["line"]
        parabreaks += offsets["para"]
        regionbreaks += offsets["region"]

        page_doc = {
            "id": page_file_names[page_no].replace(".json", ""),
            "n": page_no,
            "t": coords["tokens"],
            # TODO add paragraphs?
            # "p": coords["paragraphs"],
            "r": coords["regions"],
        }
        solr_ci["ppreb"].append(page_doc)
    solr_ci["lb"] = linebreaks
    solr_ci["pb"] = parabreaks
    solr_ci["rb"] = regionbreaks
    solr_ci["ft"] = fulltext

    return solr_ci


def recompose_ci_from_page_passim(
    content_item: dict[str, Any], passim_doc: dict[str, Any]
) -> dict[str, Any]:
    """_summary_
    TODO documentation and ensuring the code works for our needs.

    Args:
        content_item (dict[str, Any]): _description_
        passim_doc (dict[str, Any]): _description_

    Returns:
        dict[str, Any]: _description_
    """
    issue_id = "-".join(passim_doc["id"].split("-")[:-1])

    page_file_names = {p: f"{issue_id}-p{str(p).zfill(4)}.json" for p in content_item["m"]["pp"]}

    fulltext = ""
    for n, page_no in enumerate(content_item["m"]["pp"]):

        page = content_item["pprr"][n]

        if fulltext == "":
            fulltext, regions = rebuild_paper_text_passim(page, passim_doc["lg"])
        else:
            fulltext, regions = rebuild_paper_text_passim(page, passim_doc["lg"], fulltext)

        page_doc = {
            "id": page_file_names[page_no].replace(".json", ""),
            "seq": page_no,
            "regions": regions,
        }
        passim_doc["pages"].append(page_doc)

    passim_doc["text"] = fulltext

    return passim_doc


def reconstruct_pages(
    issue_json: dict[str, Any], ci: dict[str, Any], cis: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Reconstruct the pages of a given issue to prepare for the rebuilt CI composition.

    Args:
        issue_json (dict[str, Any]): Issue for which to rebuild the page regions.
        ci (dict[str, Any]): Current CI for which to rebuild the regions.
        cis (list[dict[str, Any]]): Current list of previously processed CIs from this issue.

    Returns:
        list[dict[str, Any]]: List of processed CIs with this new CI added to it.
    """
    pages = []
    page_ids = [page["id"] for page in issue_json["pp"]]
    for page_no in ci["m"]["pp"]:
        # given a page  number (from issue.json) and its canonical ID
        # find the position of that page in the array of pages (with text regions)
        page_no_string = f"p{str(page_no).zfill(4)}"
        try:
            page_idx = [
                n for n, page in enumerate(issue_json["pp"]) if page_no_string in page["id"]
            ][0]
            pages.append(issue_json["pp"][page_idx])
        except IndexError:
            ci["has_problem"] = True
            cis.append(ci)
            logger.error(
                "Page %s not found for item %s. Issue %s has pages %s",
                page_no_string,
                ci["m"]["id"],
                issue_json["id"],
                page_ids,
            )
            continue

    regions_by_page = []
    for page in pages:
        regions_by_page.append(
            [region for region in page["r"] if "pOf" in region and region["pOf"] == ci["m"]["id"]]
        )
    ci["pprr"] = regions_by_page
    try:
        convert_coords = [p["cc"] for p in pages]
        ci["m"]["cc"] = sum(convert_coords) / len(convert_coords) == 1.0
    except Exception:
        # it just means there was no CC field in the pages
        ci["m"]["cc"] = None

    cis.append(ci)

    return cis
