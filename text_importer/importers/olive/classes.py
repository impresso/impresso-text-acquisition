import codecs
import logging
import os
from collections import deque
from time import strftime

from impresso_commons.path.path_fs import canonical_path

from text_importer.importers.olive.helpers import convert_image_coordinates, convert_page_coordinates, get_image_info, \
    recompose_ToC, recompose_page
from text_importer.importers.olive.parsers import olive_image_parser, olive_parser, olive_toc_parser, parse_archive, \
    parse_styles

logger = logging.getLogger(__name__)
IMPRESSO_IIIF_BASEURI = "https://impresso-project.ch/api/proxy/iiif/"


def combine_article_parts(article_parts):
    """TODO.

    :param article_parts: one or more article parts
    :type article_parts: list of dict
    :rtype: a dictionary, with keys "meta", "fulltext", "stats", "legacy"
    """
    if len(article_parts) > 1:
        # if an article has >1 part, retain the metadata
        # from the first item in the list
        article_dict = {
                "meta": {},
                "fulltext": "",
                "stats": {},
                "legacy": {}
                }
        article_dict["legacy"]["id"] = [
                ar["legacy"]["id"]
                for ar in article_parts
                ]
        article_dict["legacy"]["source"] = [
                ar["legacy"]["source"]
                for ar in article_parts
                ]
        article_dict["meta"]["type"] = {}
        article_dict["meta"]["type"]["raw"] = \
            article_parts[0]["meta"]["type"]["raw"]
        
        article_dict["meta"]["title"] = article_parts[0]["meta"]["title"]
        article_dict["meta"]["page_no"] = [
                int(n)
                for ar in article_parts
                for n in ar["meta"]["page_no"]
                ]
        
        # TODO: remove from production
        if len(article_dict["meta"]["page_no"]) > 1:
            # pdb.set_trace()
            pass
        
        article_dict["meta"]["language"] = {}
        article_dict["meta"]["language"] = \
            article_parts[0]["meta"]["language"]
        article_dict["meta"]["issue_date"] = \
            article_parts[0]["meta"]["issue_date"]
    elif len(article_parts) == 1:
        article_dict = next(iter(article_parts))
    else:
        article_dict = None
    return article_dict


class OliveNewspaperPage(object):
    
    def __init__(self, n, _id):
        self.id_ = _id
        self.page_no = n
        self.iiif_url = os.path.join(IMPRESSO_IIIF_BASEURI, self.id_)
        self.data = {}
        self.issue = None
    
    def add_issue(self, issue):
        self.issue = issue
    
    def parse(self):
        toc_data = self.issue.toc_data_unchanged
        element_ids = toc_data[self.page_no].keys()
        
        elements = {
                el["legacy"]["id"]: el
                for el in self.issue.content_elements
                if (el["legacy"]["id"] in element_ids)
                }
        
        clusters = {}
        for ar in self.issue.articles:
            legacy_id = ar["legacy"]["id"]
            if isinstance(legacy_id, list):
                clusters[legacy_id[0]] = legacy_id
            else:
                clusters[legacy_id] = [legacy_id]
        
        self.data = recompose_page(self.page_no, toc_data, elements, clusters)
        self.data['id'] = self.id_
        self.data['iiif'] = self.iiif_url
        if len(self.data['r']) == 0:
            logger.warning(f"Page {self.id_} has no OCR text")
        
        try:
            image_info_record = [
                    page
                    for page in self.issue.image_info
                    if int(page['pg']) == self.page_no
                    ][0]
            box_strategy = image_info_record['strat']
            image_name = image_info_record['s']
            page_xml_file = self.issue.page_xml_files[self.page_no]
        except Exception as e:
            logger.error("Page {} in {} raised error: {}".format(self.page_no, self.issue.id, e))
            logger.error("Couldn't get information about page img {} in {}".format(self.page_no, self.issue.id))
        
        # TODO move this to the helper function
        # convert the box coordinates
        try:
            convert_page_coordinates(self.data, self.issue.archive.read(page_xml_file), image_name,
                                     self.issue.archive,
                                     box_strategy,
                                     self.issue.issue_dir
                                     )
            self.data['cc'] = True
        except Exception as e:
            logger.error("Page {} in {} raised error: {}".format(self.page_no, self.issue.id, e))
            logger.error("Couldn't convert coordinates in p. {} {}".format(self.page_no, self.issue.id))
            self.data['cc'] = False
        
        """
        conversion of image coordinates:
        - fetch all images belonging to current page
        - for each image, convert and set .cc=True if ok
        """
        
        images_in_page = [
                content_item
                for content_item in self.issue.issue_data['i']
                if content_item['m']['tp'] == "picture" and self.page_no in content_item['m']['pp']
                ]
        for image in images_in_page:
            image = convert_image_coordinates(image, self.issue.archive.read(page_xml_file), image_name,
                                              self.issue.archive, box_strategy, self.issue.issue_dir)
            image.m.tp = 'image'


class OliveNewspaperIssue(object):
    
    def __init__(self, issue_dir, image_dir, temp_dir):
        self.issue_dir = issue_dir
        self.id = canonical_path(issue_dir, path_type="dir").replace("/", "-")
        self.pages = []
        
        self.working_archive = os.path.join(issue_dir.path, "Document.zip")
        self.archive = parse_archive(self.working_archive)
        
        self.styles = self._parse_styles()
        self.toc_data = self._parse_toc()
        self.toc_data_unchanged = self._parse_toc()
        
        self.items = self._parse_items(temp_dir)  # Needed to recompose articles
        self.articles, self.content_elements = self._recompose_articles()
        
        self.page_xml_files = self._parse_page_xml()
        
        self.images = self._parse_images_data()
        
        self.contents = recompose_ToC(self.toc_data, self.articles, self.images)
        
        try:
            self.image_info = get_image_info(self.issue_dir, image_dir)
        except FileNotFoundError:
            logger.error(f"Missing image-info.json file for {issue_dir.path}")
        
        self._find_pages()
        
        self.issue_data = {
                "id": self.id,
                "cdt": strftime("%Y-%m-%d %H:%M:%S"),
                "s": self.styles,
                "i": self.contents,
                "pp": [p.id_ for p in self.pages]
                }
    
    def _parse_items(self, temp_dir):
        items = sorted(
                [item
                 for item in self.archive.namelist()
                 if ".xml" in item
                 and not item.startswith("._")
                 and ("/Ar" in item or "/Ad" in item)
                 ]
                )
        
        if temp_dir is not None:
            for item in items:
                with codecs.open(
                        os.path.join(temp_dir, item.replace('/', '-')),
                        'wb'
                        ) as out_file:
                    xml_data = self.archive.read(item)
                    out_file.write(xml_data)
        
        logger.debug("XML files contained in {}: {}".format(
                self.working_archive,
                items
                ))
        return items
    
    def _parse_page_xml(self):
        page_xml_files = {
                int(item.split("/")[0]): item
                for item in self.archive.namelist()
                if ".xml" in item and not item.startswith("._") and "/Pg" in item
                }
        return page_xml_files
    
    def _parse_image_xml(self):
        image_xml_files = [
                item
                for item in self.archive.namelist()
                if ".xml" in item and not item.startswith("._") and "/Pc" in item
                ]
        return image_xml_files
    
    def _parse_styles(self):
        styles = []
        if 'styleGallery.txt' in self.archive.namelist():
            try:
                styles = parse_styles(self.archive.read('styleGallery.txt').decode())
            except Exception as e:
                logger.warning("Parsing style file in {} failed with error {}".format(
                        self.working_archive, e))
        else:
            logger.warning(f"No styleGallery.txt found for {self.issue_dir}")
        return styles
    
    def _parse_toc(self):
        toc_path = os.path.join(self.issue_dir.path, "TOC.xml")
        try:
            toc_data = olive_toc_parser(toc_path, self.issue_dir)
            logger.debug(f"TOC Data for {self.issue_dir}: {toc_data}")
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Missing ToC.xml for {self.issue_dir.path}')
        except Exception as e:
            raise ValueError(f'Corrupted ToC.xml for {self.issue_dir.path}')
        return toc_data
    
    def _parse_images_data(self):
        images = []
        image_xml_files = self._parse_image_xml()
        for image_file in image_xml_files:
            try:
                image_data = olive_image_parser(self.archive.read(image_file))
            except Exception as e:
                # there are e.g. image file with empty coordinate attributes
                logger.error('Failed parsing img file {} in {}'.format(
                        image_file,
                        self.issue_dir.path
                        ))
                logger.error(e)
                continue
            
            # because of course there are empty files!
            if image_data is not None:
                images.append(image_data)
        return images
    
    def _recompose_articles(self):
        articles = []
        content_elements = []
        
        article_parts = []
        counter = 0
        while len(self.items) > 0:
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
            
            internal_deque = deque([self.items[0]])
            self.items = self.items[1:]
            
            while len(internal_deque) > 0:
                item = internal_deque.popleft()
                try:
                    xml_data = self.archive.read(item).decode('windows-1252')
                    new_data = olive_parser(xml_data)
                except Exception as e:
                    raise ValueError(f'Corrupted zip archive for {self.issue_dir.path}')
                
                # check if it needs to be parsed later on
                if new_data["legacy"]['continuation_from'] is not None:
                    target = new_data["legacy"]["continuation_from"]
                    target = [x for x in self.items if target in x]
                    if len(target) > 0:
                        self.items.append(item)
                        continue
                
                article_parts.append(new_data)
                
                if new_data["legacy"]['continuation_to'] is not None:
                    next_id = new_data["legacy"]["continuation_to"]
                    next_id = [x for x in self.items if next_id in x][0]
                    internal_deque.append(next_id)
                    self.items.remove(next_id)
            
            try:
                content_elements += article_parts
                combined_article = combine_article_parts(article_parts)
                
                if combined_article is not None:
                    articles.append(combined_article)
                
                article_parts = []
            except Exception as e:
                """
                logger.error("Import of issue {} failed with error {}".format(
                    issue_dir,
                    e
                ))
                return
                """
                raise e
        return articles, content_elements
    
    def _find_pages(self):
        for page_no in self.toc_data:
            page_canonical_id = canonical_path(self.issue_dir, "p" + str(page_no).zfill(4))
            try:
                self.pages.append(
                        OliveNewspaperPage(page_no, page_canonical_id))
            except Exception as e:
                logger.error(
                        f'Adding page {page_no} {page_canonical_id}',
                        f'raised following exception: {e}'
                        )
