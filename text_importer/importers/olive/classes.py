import json
import logging
import os
import shutil
from collections import deque
from time import strftime
from typing import List
from zipfile import ZipFile

from impresso_commons.path import IssueDir
from impresso_commons.path.path_fs import canonical_path

from text_importer.importers.classes import NewspaperIssue, NewspaperPage
from text_importer.importers.olive.helpers import (combine_article_parts, convert_image_coordinates,
                                                   convert_page_coordinates, recompose_ToC, recompose_page, get_clusters)
from text_importer.importers.olive.parsers import (olive_image_parser,
                                                   olive_parser,
                                                   olive_toc_parser,
                                                   parse_styles)
from text_importer.utils import get_issue_schema, get_page_schema

logger = logging.getLogger(__name__)
IssueSchema = get_issue_schema()
Pageschema = get_page_schema()
IMPRESSO_IIIF_BASEURI = "https://impresso-project.ch/api/proxy/iiif/"


class OliveArchive(object):
    def __init__(self, archive: ZipFile, temp_dir: str):
        logger.debug(f"Extracting archive in {temp_dir}")
        self.name_list = archive.namelist()
        self.dir = temp_dir
        self.extract_archive(archive)
        archive.close()
    
    def extract_archive(self, archive: ZipFile):
        if not os.path.exists(self.dir):
            logger.debug(f"Creating {self.dir}")
            try:
                os.makedirs(self.dir)
            except FileExistsError as e:
                pass
        for f in archive.filelist:
            if f.file_size > 0:
                try:
                    archive.extract(f.filename, path=self.dir)
                except FileExistsError as e:
                    pass
    
    def namelist(self):
        return self.name_list
    
    def read(self, file):
        path = os.path.join(self.dir, file)
        with open(path, 'rb') as f:
            f_bytes = f.read()
        return f_bytes
    
    def cleanup(self):
        logging.info(f"Deleting archive {self.dir}")
        shutil.rmtree(self.dir)
        prev_dir = os.path.split(self.dir)[0]
        while len(os.listdir(prev_dir)) == 0:
            logging.info(f"Deleting {prev_dir}")
            os.rmdir(prev_dir)
            prev_dir = os.path.split(prev_dir)[0]


class OliveNewspaperPage(NewspaperPage):
    def __init__(self, _id, n, toc_data, image_info, page_xml):
        super().__init__(_id, n)
        self.toc_data = toc_data
        self.page_data = None
        self.image_info = image_info
        self.page_xml = page_xml
        self.archive = None
    
    def parse(self):
        if self.issue is None:
            raise ValueError(f"No NewspaperIssue for {self.id}")
        
        element_ids = self.toc_data.keys()
        # all_element_ids = [el_id for el_id in element_ids if "Ar" in el_id or "Ad" in el_id]
        elements = {
                el["legacy"]["id"]: el
                for el in json.loads(self.issue.content_elements)
                if (el["legacy"]["id"] in element_ids)
                }
        
        self.page_data = recompose_page(
                self.id,
                self.toc_data,
                elements,
                self.issue.clusters
                )
        
        self.page_data['id'] = self.id
        self.page_data['iiif'] = os.path.join(IMPRESSO_IIIF_BASEURI, self.id)
        
        if len(self.page_data['r']) == 0:
            logger.warning(f"Page {self.id} has not OCR text")
        
        self._convert_page_coords()
        
        if all(p.page_data is not None for p in self.issue.pages):  # Means issue has been fully processed, can cleanup
            self.archive.cleanup()
    
    def _convert_page_coords(self):
        self.page_data['cc'] = False
        if self.image_info is not None:
            try:
                box_strategy = self.image_info['strat']
                image_name = self.image_info['s']
                was_converted = convert_page_coordinates(self.page_data, self.archive.read(self.page_xml), image_name,
                                                         self.archive, box_strategy, self.issue)
                if was_converted:
                    self.page_data['cc'] = True
            except Exception as e:
                logger.error("Page {} raised error: {}".format(self.id, e))
                logger.error("Couldn't convert coordinates in p. {}".format(self.id))
        else:
            logger.debug(f"Image {self.id} does not have image info")
    
    def add_issue(self, issue):
        self.issue = issue
        self.archive = issue.archive


class OliveNewspaperIssue(NewspaperIssue):
    
    def __init__(self, issue_dir: IssueDir, image_dirs: str, temp_dir: str):
        super().__init__(issue_dir)
        self.issue_dir = issue_dir
        self.image_dirs = image_dirs
        
        self.archive = self._parse_archive(temp_dir)  # First parse the archive and return it
        self.toc_data = self._parse_toc()  # Parse ToC
        
        images = self._parse_image_xml_files()  # Parse image xml files with olive_image_parser
        articles, self.content_elements = self._parse_articles()
        self.content_items = recompose_ToC(self.toc_data, articles, images)
        
        self.clusters = get_clusters(articles)
        self.content_elements = json.dumps(self.content_elements)  # To avoid non-pickle-able objects
        self._find_pages()
        
        styles = self._parse_styles_gallery()  # Then parse the styles
        
        self.issue_data = {
                "id": self.id,
                "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                "s": styles,
                "i": self.content_items,
                "pp": [p.id for p in self.pages],
                "ar": self.rights
                }
    
    def _parse_archive(self, temp_dir: str, file: str = "Document.zip") -> OliveArchive:
        """
        Parses the archive for this issue. Fails if archive could not be parsed
        :param file: The archive file to parse
        :return:
        """
        archive_path = os.path.join(self.path, file)
        if os.path.isfile(archive_path):
            archive_tmp_path = os.path.join(temp_dir, canonical_path(self.issue_dir, path_type='dir'))
            try:
                archive = ZipFile(archive_path)
                logger.debug(f"Contents of archive for {self.id}: {archive.namelist()}")
                return OliveArchive(archive, archive_tmp_path)
            except Exception as e:
                msg = f"Bad Zipfile for {self.id}, failed with error : {e}"
                raise ValueError(msg)
        else:
            msg = f"Could not find archive {file} for {self.id}"
            raise ValueError(msg)
    
    def _get_page_xml_files(self) -> dict:
        page_xml = None
        if self.archive is not None:
            page_xml = {int(item.split("/")[0]): item for item in self.archive.namelist() if
                        ".xml" in item and not item.startswith("._") and "/Pg" in item}
        
        return page_xml
    
    def _parse_toc(self, file: str = "TOC.xml"):
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
    
    def _parse_image_xml_files(self):
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
    
    def _parse_styles_gallery(self, file: str = 'styleGallery.txt') -> List[dict]:
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
    
    def _parse_articles(self):
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
    
    def _get_image_info(self):
        """
        Get the contents of the `image-info.json` file for a given issue.
        :return: the content of the `image-info.json` file
        :rtype: dict
        """
        json_data = []
        for im_dir in self.image_dirs.split(','):
            issue_dir = os.path.join(
                    im_dir,
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
            
            if os.path.exists(image_info_path):
                with open(image_info_path, 'r') as inp_file:
                    try:
                        json_data = json.load(inp_file)
                        if len(json_data) == 0:
                            logger.debug(f"Empty image info for {self.id} at {image_info_path}")
                        else:
                            return json_data
                    except Exception as e:
                        logger.error(f"Decoding file {image_info_path} failed with '{e}'")
                        raise e
        if len(json_data) == 0:
            msg = f"Could not find image info for {self.id}"
            raise ValueError(msg)
    
    def _find_pages(self):
        if self.toc_data is not None:
            image_info = self._get_image_info()
            pages_xml = self._get_page_xml_files()
            for page_n, data in self.toc_data.items():
                can_id = "{}-p{}".format(self.id, str(page_n).zfill(4))
                image_info_records = [p for p in image_info if int(p['pg']) == page_n]
                if len(image_info_records) == 0:
                    image_info_record = None
                else:
                    image_info_record = image_info_records[0]
                
                try:
                    page_xml = pages_xml[page_n]
                except Exception as e:
                    raise ValueError(f"Could not find page xml for {can_id}")
                
                self._convert_images(image_info_record, page_n, page_xml)
                
                self.pages.append(OliveNewspaperPage(can_id, page_n, data, image_info_record, page_xml))
    
    def _convert_images(self, image_info_record, page_n, page_xml):
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

