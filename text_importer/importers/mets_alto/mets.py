"""Utility functions to parse Mets XML files."""

import logging
from bs4 import BeautifulSoup
from bs4.element import Tag

logger = logging.getLogger(__name__)


def parse_mets_filegroup(mets_doc: BeautifulSoup) -> dict[int, str]:
    """Parse ``<fileGrp>`` section to extract the page's OCR image ids.

    The ``<fileGrp>`` section contains the names and ids of the images and text
    files linked to the Mets XML file. Each page of the issue is associated to
    a scan image file and ids.

    Args:
        mets_doc (BeautifulSoup): BeautifulSoup object of Mets XML document.

    Returns:
        dict[int, str]: Mapping from page number to page image id.
    """
    image_filegroup = mets_doc.findAll(
        "fileGrp", {"USE": lambda x: x and x.lower() == "images"}
    )[0]

    return {
        int(child.get("SEQ")): child.get("ADMID")
        for child in image_filegroup.findAll("file")
    }


def parse_mets_amdsec(
    mets_doc: BeautifulSoup,
    x_res: str,
    y_res: str,
    x_res_default: int = 300,
    y_res_default: int = 300,
) -> dict:
    """Parse the ``<amdsec>`` section of Mets XML to extract image properties.

    The ``<amdsec>`` section contains administrative metadata about the OCR, in
    particular information about the image resolution allowing the coordinates
    conversion to iiif format.

    Args:
        mets_doc (BeautifulSoup): BeautifulSoup object of Mets XML document.
        x_res (str): Name of field representing the X resolution.
        y_res (str): Name of field representing the Y resolution.
        x_res_default (int, optional): Default X_res. Defaults to 300.
        y_res_default (int, optional): Default Y res. Defaults to 300.

    Returns:
        dict: Parsed image properties with default values if the field was not
            found in the document.
    """
    page_image_ids = parse_mets_filegroup(mets_doc)  # Returns {page: im_id}

    amd_sections = {
        # Returns {page_id: amdsec}
        image_id: mets_doc.findAll("amdSec", {"ID": image_id})[0]
        for image_id in page_image_ids.values()
    }

    image_properties_dict = {}
    for image_no, image_id in page_image_ids.items():
        amd_sect = amd_sections[image_id]
        try:
            x_res_val = (
                int(amd_sect.find(x_res).text)
                if amd_sect.find(x_res) is not None
                else x_res_default
            )
            y_res_val = (
                int(amd_sect.find(y_res).text)
                if amd_sect.find(y_res) is not None
                else x_res_default
            )
            image_properties_dict[image_no] = {
                "x_resolution": x_res_val,
                "y_resolution": y_res_val,
            }
        # if it fails it's because of value < 1
        except Exception as e:
            logger.debug("Error occured when parsing %s", e)
            image_properties_dict[image_no] = {
                "x_resolution": x_res_default,
                "y_resolution": y_res_default,
            }
    return image_properties_dict


def get_dmd_sec(mets_doc: BeautifulSoup, _filter: str) -> Tag:
    """Extract the contents of a specific ``<dmdsec>`` from the Mets document.

    The ``<dmdsec>`` section contains descriptive metadata. It's composed of
    several different subsections each identified with string IDs.

    Args:
        mets_doc (BeautifulSoup): BeautifulSoup object of Mets XML document.
        _filter (str): ID of the subsection of interest to filter the search.

    Returns:
        Tag: Contents of the desired ``<dmdsec>`` of the Mets XML document.
    """
    return mets_doc.find("dmdSec", {"ID": _filter})
