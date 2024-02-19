"""Helper functions to parse BCUL OCR files."""
import os
from datetime import datetime, date
import json
from bs4 import Tag
from typing import Any


def replace_alias(current_alias: str, journal_name: str) -> str:
    # TODO replace by a csv file
    # some title aliases repeat or don't match our format restrictions
    if 'GAV' in current_alias:
        return 'GAVi'
    elif 'feuille' in current_alias :
        return 'feuilleP' if journal_name in "Feuille_d'avis_de_Payerne" else 'feuillePMA'
    elif 'TO-SU-IL' in current_alias:
        return 'TouSuIl'
    elif 'Le_Phare_de_Nyon' in journal_name:
        return 'PDN'
    elif 'FAN' in current_alias and 'Le_Phare_du_' in journal_name:
        return 'PDL'
    elif 'VVS' in current_alias and 'Le_Veveysan_1' in journal_name:
        return 'VVS1'
    else:
        return current_alias

def parse_info(mit_filename: str) -> tuple[date, str]:
    """Given the Mit filename, parse the Journal name and the date.

    Args:
        mit_filename (str): Filename of the 'mit' file.

    Returns:
        Tuple[date, str]: Publication date of the issue, and journal name.
    """
    split = mit_filename.split('/')
    # extract date of publication
    year, month, day = int(split[-5]), int(split[-4]), int(split[-3])
    #normalize the month & day values if they are out of range
    month = max(min(month, 12), 1)
    day = max(min(day, 31), 1)

    # extract journal name alias
    basename = os.path.splitext(split[-1])[0]
    journal_alias = replace_alias(basename.split('_')[0], split[-6])
    
    return datetime(year, month, day).date(), journal_alias

def find_mit_file(_dir: str) -> str:
    """Given a directory, search for a file with a name ending with `mit`.

    Args:
        _dir (str): Directory to look into.

    Returns:
        str: Path to the mit file once found.
    """
    mit_file = None
    for f in os.listdir(_dir):
        if os.path.splitext(os.path.basename(f))[0].endswith("mit"):
            mit_file = os.path.join(_dir, f)
    
    return mit_file


def get_page_number(exif_file: str) -> int:
    """Given an `exif` file, look for the page number inside.

    This is for the JSON 'flavour' of BCUL, in which metadata about the pages
    are in JSON files which contain the substring `exif`.

    Args:
        exif_file (str): Path to the `exif` file.

    Raises:
        ValueError: The page number could not be extracted from the file.

    Returns:
        int: Page number extracted from the file.
    """
    try:
        with open(exif_file) as f:
            exif = json.load(f)[0]
            source = exif['SourceFile'].split('/')[-1]
            page_no = int(os.path.splitext(source)[0].split('_')[-1])
            return page_no
    except Exception as e:
        raise ValueError("Could not get page number from {}".format(exif_file))

def find_page_file_in_dir(base_path: str, file_id: str) -> str | None:
    page_path = os.path.join(base_path, "{}.xml".format(file_id))
    if not os.path.isfile(page_path):
        # some xml files are compressed
        page_path = f"{page_path}.bz2"
        if not os.path.isfile(page_path):
            # logger.critical instead? if yes, add continue in classes line 258
            raise Exception(f"The page file {page_path} couldn't be found."
                            "Skipping this page, please verify input data.")

    return page_path

def verify_issue_has_ocr_files(path: str) -> None:
    if not any([".xml" in f for f in os.listdir(path)]):
        raise Exception(f"The issue's folder {path} does not contain any xml "
                        "OCR files. Issue cannot be processed as a result.")

def get_div_coords(div: Tag) -> list[int]:
    """Extract the coordinates from the given element and format them for iiif.

    In Abbyy format, the coordinates are denoted by the bottom, top (y-axis), 
    left and right (x-axis) values.
    But iiif coordinates should be formatted as `[x, y, width, height]`, where 
    (x,y) denotes the box's top left corner: (l, t). Thus they need conversion.

    Args:
        div (Tag): Element to extract the coordinates from

    Returns:
        list[int]: Coordinates converted to the iiif format.
    """
    if div is None:
        return None
    b, l = int(div.get('b')), int(div.get('l'))
    r, t = int(div.get('r')), int(div.get('t'))
    return [l, t, r - l, b - t]


def parse_token(t: Tag) -> dict[str, list[int] | str]:
    """Parse the given div Tag to extract the token and coordinates.

    Args:
        t (Tag): div tag corresponding to a token to parse.

    Returns:
        dict[str, list[int] | str]: dict with the coordinates ans token.
    """
    coords = get_div_coords(t)
    tx = t.getText()
    return {"c": coords, "tx": tx}


def parse_textline(line: Tag) -> dict[str, list[Any]]:
    """Parse the div element corresponding to a textline.

    Args:
        line (Tag): Textline div element Tag.

    Returns:
        dict[str, list]: Parsed line of text.
    """
    line_ci = {"c": get_div_coords(line)}
    tokens = [parse_token(t) for t in line.findAll("charParams")]
    
    line_ci['t'] = tokens
    return line_ci


def parse_textblock(block: Tag, page_ci_id: str) -> dict[str, Any]:
    """Parse the given textblock element into a canonical region element.

    Args:
        block (Tag): Text block div element to parse.
        page_ci_id (str): Canonical ID of the CI corresponding to this page.

    Returns:
        dict[str, Any]: Parsed region object in canonical format.
    """
    coordinates = get_div_coords(block)
    
    lines = [parse_textline(line) for line in block.findAll("line")]
    paragraph = {
        "c": coordinates,
        "l": lines,
    }
    region = {
        "c": coordinates,
        "p": [paragraph],
        "pOf": page_ci_id
    }

    return region
