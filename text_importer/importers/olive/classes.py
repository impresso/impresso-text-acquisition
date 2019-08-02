import logging
import os
from collections import deque
from time import strftime
from typing import List
from zipfile import ZipFile

from impresso_commons.path import IssueDir
from impresso_commons.path.path_fs import canonical_path
import json
from text_importer.importers.olive.helpers import combine_article_parts, recompose_ToC, recompose_page, \
    convert_page_coordinates, convert_image_coordinates
from text_importer.importers.olive.parsers import olive_image_parser, olive_parser, olive_toc_parser, parse_styles

logger = logging.getLogger(__name__)

IMPRESSO_IIIF_BASEURI = "https://impresso-project.ch/api/proxy/iiif/"


class OliveNewspaperPage(object):
    def __init__(self, can_id, toc_data, issue, image_info, page_xml):
        self.id = can_id
        self.toc_data = toc_data
        self.np_issue = issue
        self.page_dict = {}
        self.image_info = image_info
        self.page_xml = page_xml
    
    def parse(self):
        element_ids = self.toc_data.keys()
        # all_element_ids = [el_id for el_id in element_ids if "Ar" in el_id or "Ad" in el_id]
        elements = {el["legacy"]["id"]: el for el in self.np_issue.content_elements if (el["legacy"]["id"] in element_ids)}
        
        clusters = {}
        for ar in self.np_issue.articles:
            legacy_id = ar["legacy"]["id"]
            if isinstance(legacy_id, list):
                clusters[legacy_id[0]] = legacy_id
            else:
                clusters[legacy_id] = [legacy_id]
        
        self.page_dict = recompose_page(self.id, self.toc_data, elements, clusters)
        
        self.page_dict['id'] = self.id
        self.page_dict['iiif'] = os.path.join(IMPRESSO_IIIF_BASEURI, self.id)
        
        if len(self.page_dict['r']) == 0:
            logger.warning(f"Page {self.id} has not OCR text")
    
    def convert_page_coords(self):
        try:
            box_strategy = self.image_info['strat']
            image_name = self.image_info['s']
            convert_page_coordinates(self.page_dict, self.np_issue.archive.read(self.page_xml), image_name,
                                     self.np_issue.archive, box_strategy, self.np_issue)
            self.page_dict['cc'] = True
        except Exception as e:
            logger.error("Page {} raised error: {}".format(self.id, e))
            logger.error("Couldn't convert coordinates in p. {}".format(self.id))
            self.page_dict['cc'] = False


class OliveNewspaperIssue(object):
    
    def __init__(self, issue_dir, image_dir):  # TODO: add temp dir to save item_xml
        self.issue_dir = issue_dir
        self.id = canonical_path(issue_dir, path_type="dir").replace("/", "-")
        self.edition = issue_dir.edition
        self.journal = issue_dir.journal
        self.path = issue_dir.path
        self.date = issue_dir.date
        self._notes = []
        self.pages = []  # TODO : ask about access_rights
        self.image_dir = image_dir
        
        self.archive = self.parse_archive()
        self.styles = self.parse_styles_gallery()
        self.images = self.parse_image_xml_files()
        self.toc_data = self.parse_toc()
        self.image_info = self.get_image_info()
        
        self.page_xml = self.get_xml_files()
        
        self.articles, self.content_elements = self.parse_articles()
        self.content_items = recompose_ToC(self.toc_data, self.articles, self.images)
        
        self.find_pages()
        self._issue_data = {
                "id": self.id,
                "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                "s": self.styles,
                "i": self.content_items,
                "pp": [p.id for p in self.pages]
                }
    
    def parse_archive(self, file: str = "Document.zip") -> ZipFile:
        """
        Parses the archive for this issue. Fails if archive could not be parsed
        :param file: The archive file to parse
        :return:
        """
        archive_path = os.path.join(self.path, file)
        if os.path.isfile(archive_path):
            try:
                archive = ZipFile(archive_path)
                
                logger.debug(f"Contents of archive for {self.id}: {archive.namelist()}")
                return archive
            except Exception as e:
                msg = f"Bad Zipfile for {self.id}, failed with error : {e}"
                raise ValueError(msg)
        else:
            msg = f"Could not find {file} for {self.id}"
            raise ValueError(msg)
    
    def get_xml_files(self) -> dict:
        page_xml = None
        if self.archive is not None:
            page_xml = {int(item.split("/")[0]): item for item in self.archive.namelist() if
                        ".xml" in item and not item.startswith("._") and "/Pg" in item}
        
        return page_xml
    
    def parse_toc(self, file: str = "TOC.xml"):
        toc_path = os.path.join(self.path, file)
        try:
            toc_data = olive_toc_parser(toc_path, self.issue_dir)
            logger.debug(toc_data)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Missing ToC.xml for {self.id}')
        except Exception as e:
            logger.error(f'Corrupted ToC.xml for {self.id}')
            raise e
        return toc_data
    
    def parse_image_xml_files(self):
        image_xml_files = [item for item in self.archive.namelist() if
                           ".xml" in item and not item.startswith("._") and "/Pc" in item]
        
        images = []
        for image_file in image_xml_files:
            try:
                image_data = olive_image_parser(self.archive.read(image_file))
                # because of course there are empty files!
                if image_data is not None:
                    images.append(image_data)
            except Exception as e:
                # there are e.g. image file with empty coordinate attributes
                msg = f"Parsing img file {image_file} in {self.id} failed"
                logger.error(msg)
                logger.error(e)
        return images
    
    def parse_styles_gallery(self, file: str = 'styleGallery.txt') -> List[dict]:
        styles = []
        if file in self.archive.namelist():
            try:
                styles = parse_styles(self.archive.read(file).decode())
            except Exception as e:
                msg = f"Parsing styles file {file} for {self.id}, failed with error : {e}"
                logger.warning(msg)
        else:
            msg = f"Could not find styles {file} for {self.id}"
            logger.warning(msg)
        return styles
    
    def parse_articles(self):
        articles = []
        content_elements = []
        counter = 0
        # recompose each article by following the continuation links
        article_parts = []
        items = sorted([item for item in self.archive.namelist() if
                        ".xml" in item and not item.startswith("._") and ("/Ar" in item or "/Ad" in item)])
        
        while len(items) > 0:
            counter += 1
            
            # if out file already exists skip the data it contains
            # TODO: change this to work with the JSON output
            """
            if os.path.exists(out_file):
                exclude_data = BeautifulSoup(open(out_file).read())
                exclude_data = [
                    x.meta.id.string
                    for x in exclude_data.find_all("entity")
                ]
                for y in exclude_data:
                    for z in items:
                        if y in z:
                            items.remove(z)
                continue
            """
            internal_deque = deque([items[0]])
            items = items[1:]
            
            while len(internal_deque) > 0:
                item = internal_deque.popleft()
                try:
                    xml_data = self.archive.read(item).decode('windows-1252')
                    new_data = olive_parser(xml_data)
                except Exception as e:
                    logger.error(f'Parsing of {item} failed for {self.id}')
                    raise e
                
                # check if it needs to be parsed later on
                if new_data["legacy"]['continuation_from'] is not None:
                    target = new_data["legacy"]["continuation_from"]
                    target = [x for x in items if target in x]
                    if len(target) > 0:
                        items.append(item)
                        continue
                
                article_parts.append(new_data)
                
                if new_data["legacy"]['continuation_to'] is not None:
                    next_id = new_data["legacy"]["continuation_to"]
                    next_id = [x for x in items if next_id in x][0]
                    internal_deque.append(next_id)
                    items.remove(next_id)
            
            try:
                content_elements += article_parts
                combined_article = combine_article_parts(article_parts)
                
                if combined_article is not None:
                    articles.append(combined_article)
                
                article_parts = []
            except Exception as e:
                raise e
        return articles, content_elements
    
    def get_image_info(self):
        """
        Get the contents of the `image-info.json` file for a given issue.
        :return: the content of the `image-info.json` file
        :rtype: dict
        """
        
        issue_dir = os.path.join(
                self.image_dir,
                self.journal,
                str(self.date).replace("-", "/"),
                self.edition
                )
        
        issue_w_images = IssueDir(
                journal=self.journal,
                date=self.date,
                edition=self.edition,
                path=issue_dir
                )
        
        image_info_name = canonical_path(
                issue_w_images,
                name="image-info",
                extension=".json"
                )
        
        image_info_path = os.path.join(issue_w_images.path, image_info_name)
        
        with open(image_info_path, 'r') as inp_file:
            try:
                json_data = json.load(inp_file)
                return json_data
            except Exception as e:
                logger.error(f"Decoding file {image_info_path} failed with '{e}'")
                raise e
    
    def find_pages(self):
        if self.toc_data is not None:
            for page_n, data in self.toc_data.items():
                can_id = "{}-p{}".format(self.id, str(page_n).zfill(4))
                image_info_records = [p for p in self.image_info if int(p['pg']) == page_n]
                if len(image_info_records) == 0:
                    logger.warning(f"Could not find image info for page {can_id}")
                    image_info_record = None
                else:
                    image_info_record = image_info_records[0]
                
                try:
                    page_xml = self.page_xml[page_n]
                except Exception as e:
                    raise ValueError(f"Could not find page xml for {can_id}")

                self.convert_images(image_info_record, page_n, page_xml)
                
                self.pages.append(OliveNewspaperPage(can_id, data, self, image_info_record, page_xml))

    def convert_images(self, image_info_record, page_n, page_xml):
        if image_info_record is not None:
            box_strategy = image_info_record['strat']
            image_name = image_info_record['s']
            images_in_page = [content_item for content_item in self.content_items if
                              content_item['m']['tp'] == "picture" and page_n in content_item['m']['pp']]
            for image in images_in_page:
                image = convert_image_coordinates(
                        image,
                        self.archive.read(page_xml),
                        image_name,
                        self.archive,
                        box_strategy,
                        self.issue_dir
                        )
                image['m']['tp'] = 'image'
