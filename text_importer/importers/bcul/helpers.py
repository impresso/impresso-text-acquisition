import os
from datetime import datetime
import json


def parse_info(mit_filename):
    basename, _ = os.path.splitext(os.path.basename(mit_filename.split('/')[-1]))
    split = basename.split('_')
    year, month, day = int(split[-4]), int(split[-3]), int(split[-2])
    return datetime(year, month, day).date(), split[0]


def find_mit_file(_dir):
    mit_file = None
    for f in os.listdir(_dir):
        if os.path.splitext(os.path.basename(f))[0].endswith("mit"):
            mit_file = os.path.join(_dir, f)
    
    return mit_file


def get_page_number(exif_file):
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
