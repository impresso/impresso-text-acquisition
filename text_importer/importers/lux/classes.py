import codecs
import logging
import os
import re
from time import strftime
from typing import List, Tuple

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

from text_importer.importers import (CONTENTITEM_TYPE_ADVERTISEMENT,
                                     CONTENTITEM_TYPE_ARTICLE,
                                     CONTENTITEM_TYPE_IMAGE,
                                     CONTENTITEM_TYPE_OBITUARY,
                                     CONTENTITEM_TYPE_TABLE,
                                     CONTENTITEM_TYPE_WEATHER)
from text_importer.importers.lux.helpers import convert_coordinates, encode_ark
from text_importer.importers.mets_alto import (MetsAltoNewspaperIssue,
                                               MetsAltoNewspaperPage,
                                               parse_mets_amdsec)
from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URL = "https://iiif.eluxemburgensia.lu/iiif/2"


class LuxNewspaperPage(MetsAltoNewspaperPage):
    """Class representing a page in BNL data."""
    
    def add_issue(self, issue: MetsAltoNewspaperIssue):
        self.issue = issue
        encoded_ark_id = encode_ark(self.issue.ark_id)
        iiif_base_link = f'{IIIF_ENDPOINT_URL}/{encoded_ark_id}'
        iiif_link = f'{iiif_base_link}%2fpages%2f{self.number}/info.json'
        self.page_data['iiif'] = iiif_link
    
    def _convert_coordinates(self, page_data: List[dict]) -> Tuple[bool, List[dict]]:
        success = False
        try:
            img_props = self.issue.image_properties[self.number]
            x_res = img_props['x_resolution']
            y_res = img_props['y_resolution']
            
            for region in page_data:
                
                x, y, w, h = region['c']
                region['c'] = convert_coordinates(x, y, w, h, x_res, y_res)
                
                logger.debug(f"Page {self.number}: {x},{y},{w},{h} => {region['c']}")
                
                for paragraph in region['p']:
                    
                    x, y, w, h = paragraph['c']
                    paragraph['c'] = convert_coordinates(x, y, w, h, x_res, y_res)
                    
                    logger.debug(f"(para) Page {self.number}: {x},{y},{w},{h} => {paragraph['c']}")
                    
                    for line in paragraph['l']:
                        
                        x, y, w, h = line['c']
                        line['c'] = convert_coordinates(x, y, w, h, x_res, y_res)
                        
                        logger.debug(f"(line) Page {self.number}: {x},{y},{w},{h} => {paragraph['c']}")
                        
                        for token in line['t']:
                            x, y, w, h = token['c']
                            token['c'] = convert_coordinates(x, y, w, h, x_res, y_res)
                            logger.debug(f"(token) Page {self.number}: {x},{y},{w},{h} => {token['c']}")
            success = True
        except Exception as e:
            logger.error(f"Error {e} occurred when converting coordinates for {self.id}")
        finally:
            return success, page_data


class LuxNewspaperIssue(MetsAltoNewspaperIssue):
    """Class representing an issue in BNL data.
    All functions defined in this child class are specific to parsing BNL Mets/Alto format
    """
    
    def _find_pages(self):
        """Detects the Alto XML page files for a newspaper issue."""
        
        # get the canonical names for pages in the newspaper issue by
        # visiting the `text` sub-folder with the alto XML files
        text_path = os.path.join(self.path, 'text')
        page_file_names = [
            file
            for file in os.listdir(text_path)
            if not file.startswith('.') and '.xml' in file
            ]
        
        page_numbers = []
        page_match_exp = r'(.*?)(\d{5})(.*)'
        
        for fname in page_file_names:
            g = re.match(page_match_exp, fname)
            page_no = g.group(2)
            page_numbers.append(int(page_no))
        
        page_canonical_names = ["{}-p{}".format(self.id, str(page_n).zfill(4)) for page_n in page_numbers]
        
        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, page_numbers, page_canonical_names):
            try:
                self.pages.append(LuxNewspaperPage(page_id, page_no, filename, text_path))
            except Exception as e:
                logger.error(
                        f'Adding page {page_no} {page_id} {filename}',
                        f'raised following exception: {e}'
                        )
                raise e
    
    def _parse_mets_div(self, element):
        # to each section_id corresponds a div
        # find first-level DIVs inside the element
        # and inside each div get to the <area>
        # return a dict with component_id, component_role, component_fileid
        
        parts = []
        
        for child in element.children:
            if isinstance(child, NavigableString):
                continue
            elif isinstance(child, Tag):
                type_attr = child.get('TYPE')
                comp_role = type_attr.lower() if type_attr else None
                areas = child.findAll('area')
                for area in areas:
                    comp_id = area.get('BEGIN')
                    comp_fileid = area.get('FILEID')
                    comp_page_no = int(comp_fileid.replace('ALTO', ''))
                    
                    parts.append(
                            {
                                'comp_role': comp_role,
                                'comp_id': comp_id,
                                'comp_fileid': comp_fileid,
                                'comp_page_no': comp_page_no
                                }
                            )
        return parts
    
    def _parse_dmdsec(self):
        """
        Parses the `dmdSec` tags of the Mets file. Those tags' `ID` attribute should contain `ARTICLE` or `PICT`
        :return:
        """
        content_items = []
        sections = self.xml.findAll('dmdSec')
        
        # enforce sorting based on the ID string to pinpoint the generated canonical IDs
        sections = sorted(
                sections,
                key=lambda elem: elem.get('ID').split("_")[1]
                )
        counter = 1
        for section in sections:
            section_id = section.get('ID')
            
            if "ARTICLE" in section_id or "PICT" in section_id:
                # Get title Info
                title_elements = section.find_all('titleInfo')
                item_title = title_elements[0].getText().replace('\n', ' ') \
                    .strip() if len(title_elements) > 0 else None
                
                # Prepare ci metadata
                metadata = {
                    'id': "{}-i{}".format(self.id, str(counter).zfill(4)),
                    'pp': [],
                    'tp': CONTENTITEM_TYPE_ARTICLE if "ARTICLE" in section_id else CONTENTITEM_TYPE_IMAGE
                    }
                
                # Find the parts
                try:
                    item_div = self.xml.findAll('div', {'DMDID': section_id})[0]
                    parts = self._parse_mets_div(item_div)
                except IndexError:
                    err_msg = f"<div [DMID]={section_id}> not found {self.path}"
                    self._notes.append(err_msg)
                    logger.error(err_msg)
                    parts = []
                
                if item_title:
                    metadata['t'] = item_title
                
                # Finalize the item
                item = {
                    "m": metadata,
                    "l": {
                        "id": section_id,
                        "parts": parts
                        }
                    }
                
                # TODO: keep language (there may be more than one)
                # TODO: how to get language information for these CIs ?
                if item['m']['tp'] == CONTENTITEM_TYPE_ARTICLE:
                    lang = section.find_all('languageTerm')[0].getText()
                    item['m']['l'] = lang
                
                content_items.append(item)
                counter += 1
        
        return content_items
    
    def _parse_structmap_divs(self, start_counter):
        """
        Parses content items that are only in the Logical structmap of the METS file
        :param start_counter:
        :return:
        """
        content_items = []
        counter = start_counter
        element = self.xml.find('structMap', {'TYPE': 'LOGICAL'})
        
        # TODO: not sure if death_notice is a type, maybe replace by `OBITUARY`
        allowed_types = ["ADVERTISEMENT", "DEATH_NOTICE", "WEATHER"]
        divs = []
        
        for div_type in allowed_types:
            divs += element.findAll('div', {'TYPE': div_type})
        
        sorted_divs = sorted(
                divs,
                key=lambda elem: elem.get('ID')
                )
        
        for div in sorted_divs:
            
            div_type = div.get('TYPE').lower()
            if div_type == 'advertisement':
                content_item_type = CONTENTITEM_TYPE_ADVERTISEMENT
            elif div_type == 'weather':
                content_item_type = CONTENTITEM_TYPE_WEATHER
            elif div_type == 'death_notice':
                content_item_type = CONTENTITEM_TYPE_OBITUARY
            else:
                continue
            
            # TODO: how to get language information for these CIs ?
            # The language of those CI should be in the DMDSEC of their parent section.
            metadata = {
                'id': "{}-i{}".format(self.id, str(counter).zfill(4)),
                'tp': content_item_type,
                'pp': [],
                't': div.get('LABEL')
                }
            
            item = {
                "m": metadata,
                "l": {
                    "parts": self._parse_mets_div(div),  # Parse the parts
                    "id": div.get('ID')
                    }
                }
            counter += 1
            content_items.append(item)
        
        return content_items
    
    def _process_image_ci(self, ci):
        
        item_div = self.xml.findAll('div', {'DMDID': ci['l']['id']})[0]
        
        legacy_id = item_div.get('ID')
        # Image is actually table
        
        if item_div.get('TYPE').lower() == "table":
            ci['m']['tp'] = CONTENTITEM_TYPE_TABLE
            for part in ci['l']['parts']:
                page_no = part["comp_page_no"]
                if page_no not in ci['m']['pp']:
                    ci['m']['pp'].append(page_no)
        
        elif item_div.get('TYPE').lower() == "illustration":
            
            # filter content item part that is the actual image
            # the other part is the caption
            try:
                part = [
                    part
                    for part in ci['l']['parts']
                    if part['comp_role'] == 'image'
                    ][0]
            except IndexError as e:
                err_msg = f'{legacy_id} without image subpart'
                err_msg += f"; {legacy_id} has {ci['l']['parts']}"
                logger.error(err_msg)
                self._notes.append(err_msg)
                logger.exception(e)
                return
            
            # for each "part" open the XML file of corresponding page
            # get the coordinates and convert them
            # some imgs are in fact tables (meaning they have text
            # recognized)
            
            # find the corresponding page where it's located
            curr_page = None
            for page in self.pages:
                if page.number == part['comp_page_no']:
                    curr_page = page
            
            # add the page number to the content item
            assert curr_page is not None
            if curr_page.number not in ci['m']['pp']:
                ci['m']['pp'].append(curr_page.number)
            
            try:
                # parse the Alto file to fetch the coordinates
                composed_block = curr_page.xml.find(
                        'ComposedBlock',
                        {"ID": part['comp_id']}
                        )
                
                if composed_block:
                    graphic_el = composed_block.find(
                            'GraphicalElement'
                            )
                    
                    if graphic_el is None:
                        graphic_el = curr_page.xml.find(
                                'Illustration'
                                )
                else:
                    graphic_el = curr_page.xml.find(
                            'Illustration',
                            {"ID": part['comp_id']}
                            )
                
                hpos = int(graphic_el.get('HPOS'))
                vpos = int(graphic_el.get('VPOS'))
                width = int(graphic_el.get('WIDTH'))
                height = int(graphic_el.get('HEIGHT'))
                img_props = self.image_properties[curr_page.number]
                x_resolution = img_props['x_resolution']
                y_resolution = img_props['y_resolution']
                coordinates = convert_coordinates(
                        hpos,
                        vpos,
                        height,
                        width,
                        x_resolution,
                        y_resolution
                        )
                encoded_ark_id = encode_ark(self.ark_id)
                iiif_base_link = f'{IIIF_ENDPOINT_URL}/{encoded_ark_id}'
                ci['m']['iiif_link'] = f'{iiif_base_link}%2fpages%2f{curr_page.number}/info.json'
                ci['c'] = list(coordinates)
                del ci['l']['parts']
            except Exception as e:
                err_msg = 'An error occurred with {}'.format(
                        os.path.join(
                                curr_page.basedir,
                                curr_page.filename
                                )
                        )
                err_msg += f"<ComposedBlock> @ID {part['comp_id']} \
                 not found"
                logger.error(err_msg)
                self._notes.append(err_msg)
                logger.exception(e)
    
    def _parse_mets(self):
        """Parses the Mets XML file of the newspaper issue."""
        
        mets_file = [
            os.path.join(self.path, f)
            for f in os.listdir(self.path)
            if 'mets.xml' in f
            ][0]
        
        with codecs.open(mets_file, 'r', "utf-8") as f:
            raw_xml = f.read()
        
        mets_doc = BeautifulSoup(raw_xml, 'xml')
        
        # explain
        self.image_properties = parse_mets_amdsec(mets_doc, x_res='xOpticalResolution', y_res='yOpticalResolution')
        
        # First find `ARTICLE` and `PICTURE`
        content_items = self._parse_dmdsec()
        # Then find other CIs
        content_items += self._parse_structmap_divs(
                start_counter=len(content_items) + 1
                )
        
        # Set ark_id
        ark_link = mets_doc.find('mets').get('OBJID')
        self.ark_id = ark_link.replace('https://persist.lu/', '')
        
        for ci in content_items:
            
            # ci['l']['parts'] = self._parse_mets_div(item_div)
            
            if ci['m']['tp'] == 'image':
                self._process_image_ci(ci)
            elif ci['m']['tp']:
                for part in ci['l']['parts']:
                    page_no = part["comp_page_no"]
                    if page_no not in ci['m']['pp']:
                        ci['m']['pp'].append(page_no)
        
        self.issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "i": content_items,
            "id": self.id,
            "ar": self.rights,
            "pp": [p.id for p in self.pages]
            }
        
        if self._notes:
            self.issue_data["n"] = "\n".join(self._notes)
