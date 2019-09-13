import logging
from typing import Dict

logger = logging.getLogger(__name__)


def parse_mets_filegroup(element) -> Dict:
    # return a list of page image ids
    
    return {
            int(child.get("SEQ")): child.get("ADMID")
            for child in element.findAll('file')
            }


def parse_mets_amdsec(mets_doc, x_res: str, y_res: str, x_res_default=300, y_res_default=300) -> Dict:
    """
    :param mets_doc: BeautifulSoup document of METS.xml
    :param x_res: Field representing the X resolution
    :param y_res: Field representing the Y resolution
    :param x_res_default: Default X_res (300)
    :param y_res_default: Default Y res (300)
    :return: dict, containing the resolution for each image
    """
    image_filegroup = mets_doc.findAll('fileGrp', {"USE": lambda x: x and x.lower() == 'images'})[0]
    page_image_ids = parse_mets_filegroup(image_filegroup)  # Returns {page: im_id}
    
    amd_sections = {
            image_id: mets_doc.findAll('amdSec', {'ID': image_id})[0]  # Returns {page_id: amdsec}
            for image_id in page_image_ids.values()
            }
    
    image_properties_dict = {}
    for image_no, image_id in page_image_ids.items():
        amd_sect = amd_sections[image_id]
        try:
            image_properties_dict[image_no] = {
                    'x_resolution': int(amd_sect.find(x_res).text),
                    'y_resolution': int(amd_sect.find(y_res).text)
                    }
        # if it fails it's because of value < 1
        except Exception as e:
            logger.debug(f'Error occured when parsing {e}')
            image_properties_dict[image_no] = {
                    'x_resolution': x_res_default,
                    'y_resolution': y_res_default
                    }
    return image_properties_dict
