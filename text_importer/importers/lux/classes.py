import codecs
import logging
import os
import re
from time import strftime

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from impresso_commons.path.path_fs import IssueDir

from text_importer.helpers import get_issue_schema, get_page_schema
from text_importer.importers.lux import alto
from text_importer.importers.lux.helpers import convert_coordinates, encode_ark
from text_importer.importers import CONTENTITEM_TYPE_IMAGE
from text_importer.importers import CONTENTITEM_TYPE_TABLE
from text_importer.importers import CONTENTITEM_TYPE_ARTICLE
from text_importer.importers import CONTENTITEM_TYPE_WEATHER
from text_importer.importers import CONTENTITEM_TYPE_OBITUARY
from text_importer.importers import CONTENTITEM_TYPE_ADVERTISEMENT


IssueSchema = get_issue_schema()
Pageschema = get_page_schema()

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URL = "https://iiif.eluxemburgensia.lu/iiif/2"


class LuxNewspaperPage(object):
    """Class representing a page in BNL data."""

    def __init__(self, n, id, filename, basedir):
        self.number = n
        self.id = id
        self.filename = filename
        self.basedir = basedir
        self.issue = None
        self.data = {
            'id': id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'r': []  # here go the page regions
        }

    def add_issue(self, issue):
        self.issue = issue
        encoded_ark_id = encode_ark(self.issue.ark_id)
        iiif_base_link = f'{IIIF_ENDPOINT_URL}/{encoded_ark_id}'
        iiif_link = f'{iiif_base_link}%2fpages%2f{self.number}/info.json'
        self.data['iiif'] = iiif_link
        return

    def to_json(self):
        """Validates `page.data` against PageSchema & serializes to string.

        ..note::
            Validation adds a substantial overhead to computing time. For
            serialization of lots of pages it is recommendable to bypass
            schema validation.
        """
        page = Pageschema(**self.data)
        return page.serialize()

    def parse(self):

        doc = self.xml

        mappings = {}
        for ci in self.issue._issue_data['i']:
            ci_id = ci['m']['id']
            if 'parts' in ci['l']:
                for part in ci['l']['parts']:
                    mappings[part['comp_id']] = ci_id

        pselement = doc.find('PrintSpace')
        page_data = alto.parse_printspace(pselement, mappings)
        self.data['cc'], self.data["r"] = self._convert_coordinates(page_data)
        return

    def _convert_coordinates(self, page_data):
        # TODO: move this to a separate method ?
        success = False
        try:
            img_props = self.issue.image_properties[self.number]
            x_res = img_props['x_resolution']
            y_res = img_props['y_resolution']

            for region in page_data:

                x, y, w, h = region['c']
                region['c'] = convert_coordinates(x, y, w, h, x_res, y_res)

                for paragraph in region['p']:

                    x, y, w, h = region['c']
                    region['c'] = convert_coordinates(x, y, w, h, x_res, y_res)

                    for line in paragraph['l']:
                        for token in line:
                            x, y, w, h = region['c']
                            region['c'] = convert_coordinates(
                                x, y, w, h, x_res, y_res
                            )
            success = True
        except Exception as e:
            pass
        finally:
            return (success, page_data)

    @property
    def xml(self):
        """Returns a BeautifulSoup object with Alto XML of the page."""
        alto_xml_path = os.path.join(self.basedir, self.filename)

        with codecs.open(alto_xml_path, 'r', "utf-8") as f:
            raw_xml = f.read()

        alto_doc = BeautifulSoup(raw_xml, 'xml')
        return alto_doc


class LuxNewspaperIssue(object):
    """docstring for MetsNewspaperIssue."""

    def __init__(self, issue_dir):

        # create the canonical issue id
        self.id = "{}-{}-{}".format(
            issue_dir.journal,
            "{}-{}-{}".format(
                issue_dir.date.year,
                str(issue_dir.date.month).zfill(2),
                str(issue_dir.date.day).zfill(2)
            ),
            issue_dir.edition
        )
        self.edition = issue_dir.edition
        self.journal = issue_dir.journal
        self.path = issue_dir.path
        self.date = issue_dir.date
        self._issue_data = {}
        self._notes = []
        self.rights = issue_dir.rights
        self.image_properties = {}
        self.ark_id = None

        # TODO: copy the license/rights information from `issue_dir`

        self._find_pages()
        self._parse_mets()

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

        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4))
            for page_n in page_numbers
        ]

        self.pages = []
        for filename, page_no, page_id in zip(
            page_file_names, page_numbers, page_canonical_names
        ):
            try:
                self.pages.append(
                    LuxNewspaperPage(page_no, page_id, filename, text_path)
                )
            except Exception as e:
                logger.error(
                    f'Adding page {page_no} {page_id} {filename}',
                    f'raised following exception: {e}'
                )
                raise e

    def _parse_mets_sections(self, mets_doc):
        # returns a list of content items
        # enforce some sorting
        content_items = []
        sections = mets_doc.findAll('dmdSec')

        # enforce sorting based on the ID string to pinpoint the
        # generated canonical IDs
        sections = sorted(
            sections,
            key=lambda elem: elem.get('ID').split("_")[1]
        )

        for item_counter, section in enumerate(sections):

            section_id = section.get('ID')

            if 'ARTICLE' in section_id:
                item_counter += 1
                lang = section.find_all('languageTerm')[0].getText()
                title_elements = section.find_all('titleInfo')
                item_title = title_elements[0].getText().replace('\n', ' ')\
                    .strip() if len(title_elements) > 0 else None
                metadata = {
                    'id': "{}-i{}".format(self.id, str(item_counter).zfill(4)),
                    'l': lang,
                    'tp': CONTENTITEM_TYPE_ARTICLE,
                    'pp': []
                }
                # if there is not a title we omit the field
                if item_title:
                    metadata['t'] = item_title
                item = {
                    "m": metadata,
                    "l": {
                        # TODO: pass the article components
                        "id": section_id
                    }
                }
                content_items.append(item)
            elif 'PICT' in section_id:
                # TODO: keep language (there may be more than one)
                title_elements = section.find_all('titleInfo')
                item_title = title_elements[0].getText().replace('\n', ' ')\
                    .strip() if len(title_elements) > 0 else None
                metadata = {
                    'id': "{}-i{}".format(self.id, str(item_counter).zfill(4)),
                    # 'l': 'n/a',
                    'tp': CONTENTITEM_TYPE_IMAGE,
                    'pp': []
                }
                # if there is not a title we omit the field
                if item_title:
                    metadata['t'] = item_title
                item = {
                    "m": metadata,
                    "l": {
                        "id": section_id
                    }
                }
                content_items.append(item)
        return content_items

    def _parse_structmap_divs(self, mets_doc, start_counter):
        """TODO."""
        content_items = []
        counter = start_counter
        element = mets_doc.find('structMap', {'TYPE': 'LOGICAL'})
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

            metadata = {
                'id': "{}-i{}".format(self.id, str(counter).zfill(4)),
                'l': "n/a",
                'tp': content_item_type,
                'pp': [],
                't': div.get('LABEL')
            }
            item = {
                "m": metadata,
                "l": {
                    "id": div.get('ID')
                }
            }
            counter += 1
            content_items.append(item)

        return content_items

    def _parse_mets_div(self, element):
        # to each section_id corresponds a div
        # find first-level DIVs inside the element
        # and inside each div get to the <area>
        # return a dict with component_id, component_role, component_fileid

        parts = []

        for child in element.children:

            comp_id = None
            comp_role = None
            comp_fileid = None

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

    def _parse_mets_filegroup(self, element):
        # return a list of page image ids

        return {
            int(child.get("SEQ")): child.get("ADMID")
            for child in element.findAll('file')
        }

    def parse_mets_amdsec(self, mets_doc):
        image_filegroup = mets_doc.findAll('fileGrp', {'USE': 'Images'})[0]
        page_image_ids = self._parse_mets_filegroup(image_filegroup)
        amd_sections = {
            image_id:  mets_doc.findAll('amdSec', {'ID': image_id})[0]
            for image_id in page_image_ids.values()
        }
        image_properties_dict = {}
        for image_no, image_id in page_image_ids.items():
            amd_sect = amd_sections[image_id]
            image_properties_dict[image_no] = {
                'x_resolution': int(amd_sect.find('xOpticalResolution').text),
                'y_resolution': int(amd_sect.find('yOpticalResolution').text)
            }
        return image_properties_dict

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
        self.image_properties = self.parse_mets_amdsec(mets_doc)

        content_items = self._parse_mets_sections(mets_doc)
        content_items += self._parse_structmap_divs(
            mets_doc,
            start_counter=len(content_items) + 1
        )

        # NOTE: there are potentially other CIs that are not captured
        # by the method above. For example DEATH_NOTICE, WEATHER and
        # ADVERTISEMENT

        # TODO: implement a function that finds those other CIs
        # and assigns to them a canonical identifier. Search all DIVs,
        # filter by type, sort by ID et voila.

        ark_link = mets_doc.find('mets').get('OBJID')
        self.ark_id = ark_link.replace('https://persist.lu/', '')

        for ci in content_items:
            try:
                legacy_id = ci['l']['id']
                if (
                    ci['m']['tp'] == CONTENTITEM_TYPE_ARTICLE or
                    ci['m']['tp'] == CONTENTITEM_TYPE_IMAGE
                ):
                    item_div = mets_doc.findAll('div', {'DMDID': legacy_id})[0]
                else:
                    item_div = mets_doc.findAll('div', {'ID': legacy_id})[0]
            except IndexError:
                err_msg = f"<div [DMID|ID]={legacy_id}> not found {mets_file}"
                self._notes.append(err_msg)
                logger.error(err_msg)
                # the problem here is that a sort of ill-formed CI will
                # remain in the issue ToC (it has no pages)
                continue

            ci['l']['parts'] = self._parse_mets_div(item_div)

            # for each "part" open the XML file of corresponding page
            # get the coordinates and convert them
            # some imgs are in fact tables (meaning they have text
            # recognized)
            if ci['m']['tp'] == 'image':

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
                        continue

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
                        graphic_el = composed_block.find('GraphicalElement')
                        if graphic_el is None:
                            graphic_el = composed_block.find('Illustration')
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

            elif ci['m']['tp']:
                for part in ci['l']['parts']:
                    page_no = part["comp_page_no"]
                    if page_no not in ci['m']['pp']:
                        ci['m']['pp'].append(page_no)

        self._issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "i": content_items,
            "id": self.id,
            "ar": self.rights,
            "pp": [p.id for p in self.pages]
        }

        if self._notes:
            self._issue_data["n"] = "\n".join(self._notes)

    @property
    def xml(self):
        mets_file = [
            os.path.join(self.path, f)
            for f in os.listdir(self.path)
            if 'mets.xml' in f
        ][0]

        with codecs.open(mets_file, 'r', "utf-8") as f:
            raw_xml = f.read()

        mets_doc = BeautifulSoup(raw_xml, 'xml')
        return mets_doc

    @property
    def issuedir(self):
        return IssueDir(self.journal, self.date, self.edition, self.path)

    def to_json(self):
        issue = IssueSchema(**self._issue_data)
        return issue.serialize()
