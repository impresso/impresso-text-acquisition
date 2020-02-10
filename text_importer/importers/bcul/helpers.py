import os
from datetime import datetime, date
import json
from typing import Tuple


def parse_info(mit_filename: str) -> Tuple[date, bytes]:
    """ Given the Mit filename, parses the Journal name and the date
    
    :param str mit_filename:
    :return:
    """
    basename = os.path.splitext(os.path.basename(mit_filename.split('/')[-1]))[0]
    split = basename.split('_')
    year, month, day = int(split[-4]), int(split[-3]), int(split[-2])
    return datetime(year, month, day).date(), split[0]


def find_mit_file(_dir: str) -> str:
    """ Given a directory, searches for a file ending with `mit`
    
    :param str _dir: Directory to look into
    :return:
    """
    mit_file = None
    for f in os.listdir(_dir):
        if os.path.splitext(os.path.basename(f))[0].endswith("mit"):
            mit_file = os.path.join(_dir, f)
    
    return mit_file


def get_page_number(exif_file: str) -> int:
    """ Given an exif file (for the JSON flavour of BCUL) looks for the page number inside the EXIF file
    
    :param str exif_file:
    :return:
    """
    try:
        with open(exif_file) as f:
            exif = json.load(f)[0]
            source = exif['SourceFile'].split('/')[-1]
            page_no = int(os.path.splitext(source)[0].split('_')[-1])
            return page_no
    except Exception as e:
        raise ValueError("Could not get page number from {}".format(exif_file))


def get_image_coords(image_div):
    rect_div = image_div.find("rect")
    if rect_div is None:
        return None
    b, l, r, t = rect_div.get('b'), rect_div.get('l'), rect_div.get('r'), rect_div.get('t')
    return [b, l, r, t]
