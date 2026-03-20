"""This module contains helper functions to find BNL OCR data to import.
"""
import os
from typing import Any
from bs4.element import Tag
from text_preparation.importers import CONTENTITEM_TYPE_IMAGE
import requests
from ast import literal_eval


NON_ARTICLE = ["advertisement", "death_notice"]


def convert_coordinates(
    hpos: int, vpos: int, width: int, height: int, x_res: float, y_res: float
) -> list[int]:
    """Convert the coordinates to iiif-compliant ones using the resolution.

    - x = (coordinate['xResolution']/254.0) * coordinate['hpos']
    - y = (coordinate['yResolution']/254.0) * coordinate['vpos']
    - w = (coordinate['xResolution']/254.0) * coordinate['width']
    - h = (coordinate['yResolution']/254.0) * coordinate['height']

    Args:
        hpos (int): Horizontal position coordinate of element.
        vpos (int): Vertical position coordinate of element..
        width (int): Width of element.
        height (int): Height of element.
        x_res (float): X-axis resolution of image.
        y_res (float): Y-axis resolution of image.

    Returns:
        list[int]: Converted coordinates.
    """
    x = (x_res / 254) * hpos
    y = (y_res / 254) * vpos
    w = (x_res / 254) * width
    h = (y_res / 254) * height
    return [int(x), int(y), int(w), int(h)]


def encode_ark(ark: str) -> str:
    """Replaces (encodes) backslashes in the Ark identifier.

    Args:
        ark (str): original ark identifier.

    Returns:
        str: New ark identifier with encoded backslashes.
    """
    return ark.replace("/", "%2f")


def div_has_body_or_article(div: Tag, body_type="body", article_type="article") -> bool:
    """Checks if the given `div` has a body in it's direct children.

    Args:
        div (Tag): `div` element to check.
        body_type (str, optional): Content type of a body. Defaults to 'body'.

    Returns:
        bool: True if one or more of `div`'s direct children have a body.
    """
    children_types = set()
    for i in div.findChildren("div", recursive=False):
        child_type = i.get("TYPE")
        if child_type is not None:
            children_types.add(child_type.lower())
    return body_type in children_types, article_type in children_types


def section_is_article(section_div: Tag) -> bool:
    """Check if the given section `div` is an article.

    It's the case when none of `div`'s children are of non-article types
    (except for "BODY" and "BODY_CONTENT"), which are ads or obituaries.

    Args:
        section_div (Tag): section `div` to check.

    Returns:
        bool: True if given `div` is an article section.
    """
    types = []
    for c in section_div.findChildren("div"):
        _type = c.get("TYPE").lower()
        if not (_type == "body" or _type == "body_content"):
            types.append(_type)
    return not all(t in NON_ARTICLE for t in types)


def find_section_articles(
    section_div: Tag, content_items: list[dict[str, Any]]
) -> list[str]:
    """Parse the articles inside the section div and get their content item ID.

    Recover the content item canonical ID corresponding to each article using
    the legacy ID (from the OCR) of the articles found in `div`'s children.

    Args:
        section_div (Tag): `div` with the articles for which to get CI IDs.
        content_items (list[dict[str, Any]]): Content items already identified.

    Returns:
        list[str]: List of content item IDs for `div`'s children articles.
    """
    articles_lid = []
    for d in section_div.findChildren("div", {"TYPE": "ARTICLE"}):
        article_id = d.get("DMDID")
        if article_id is not None:
            articles_lid.append(article_id)

    children_art = []
    # Then search for corresponding content item
    for i in articles_lid:
        for ci in content_items:
            if i == ci["l"]["id"]:
                children_art.append(ci["m"]["id"])
    return children_art


def remove_section_cis(
    content_items: list[dict[str, Any]], sections: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Remove undesired content items based on the formed sections.

    Some content items are contained within a section and should not be in the
    content items. Given the recovered section content items, they can be
    removed.

    Args:
        content_items (list[dict[str, Any]]): Content items, to be filtered.
        sections (list[dict[str, Any]]): Formed section content items.

    Returns:
        tuple[list[dict[str, Any]], list[dict[str, Any]]]: Filtered
            content items and ones that were removed.
    """
    to_remove = [j for i in sections for j in i["l"]["canonical_parts"]]
    if len(to_remove) == 0:
        return content_items, []

    to_remove = set(to_remove)
    new_cis = []
    not_removed = []

    for ci in content_items:
        if ci["m"]["id"] not in to_remove or ci["m"]["tp"] == CONTENTITEM_TYPE_IMAGE:
            new_cis.append(ci)
            not_removed.append(ci["m"]["id"])

    print(f"inside remove_section_cis CIs to remove AFTER: {to_remove}")
    #print(f"inside remove_section_cis last CI: {new_cis[-1]}")
    print(f"inside remove_section_cis not_removed: {not_removed}")

    return new_cis, list(to_remove)

def fetch_fwh_from_iiif(iiif_uri: str) -> tuple[int, int]:
    if 'info.json' not in iiif_uri:
        iiif_uri = os.path.join(iiif_uri, 'info.json')

    r = requests.get(iiif_uri, timeout=60)

    #soup.findAll("width"), soup.findAll("height")
    dict_output = literal_eval(r.content.decode('utf-8'))

    return dict_output['width'], dict_output['height']