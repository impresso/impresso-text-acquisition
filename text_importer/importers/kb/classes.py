"""This module contains the definition of the KB importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the KB version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.

Note: These classes only handle the case for ANNO data for now. Titles which
are part of the ANNOP collection cannot be imported with them for now.
Either two separate importers will be implemented, or the titles part of ANNOP
will first be converted to Alto before using this importer.
"""

import logging
import os
import re
from time import strftime
from bs4 import BeautifulSoup, Tag
from typing import Any

from text_importer.importers import (CONTENTITEM_TYPE_IMAGE, 
                                     CONTENTITEM_TYPE_ADVERTISEMENT,
                                     CONTENTITEM_TYPE_ARTICLE)
from text_importer.importers.classes import NewspaperIssue
from text_importer.importers.mets_alto.alto import (parse_printspace, 
                                                    parse_style,
                                                    find_alto_files_or_retry)
from text_importer.importers.mets_alto.classes import MetsAltoNewspaperPage
from text_importer.importers.kb.detect import KbIssueDir

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = 'info.json'
IIIF_IMAGE_SUFFIX = 'full/full/0/default.jpg'


TYPE_MAPPING = {
    'advertentie': CONTENTITEM_TYPE_ADVERTISEMENT, 
    'artikel': CONTENTITEM_TYPE_ARTICLE, 
    'familiebericht': 'Familial message',
    'Unknown': CONTENTITEM_TYPE_IMAGE
}
class KbNewspaperPage(MetsAltoNewspaperPage):
    """Newspaper page in KB (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML file for this page.
        basedir (str): Base directory where Alto files are located.
        iiif_identifier (str): KB IIIF identifier for this page's issue.
        encoding (str, optional): Encoding of XML file. Defaults to 'utf-8'.
    
    Attributes:
        id (str): Canonical Page ID (e.g. ``GDL-1900-01-02-a-p0004``).
        number (int): Page number.
        page_data (dict[str, Any]): Page data according to canonical format.
        issue (NewspaperIssue): Issue this page is from.
        filename (str): Name of the Alto XML page file.
        basedir (str): Base directory where Alto files are located.
        encoding (str, optional): Encoding of XML file.
        iiif (str): IIIF image base URI (no suffix) for this newspaper page.
        languages (list[str]): List of languages present on this page.
    """

    def __init__(self, _id: str, number: int, filename: str, 
                 basedir: str, encoding: str = 'utf-8') -> None:
        super().__init__(_id, number, filename, basedir, encoding)
        # create iiif base image URI for KB
        self.iiif = os.path.join(IIIF_ENDPOINT_URI, 
                                 self.id, 
                                 str(number).zfill(8))
        self.page_data['iiif_img_base_uri'] = self.iiif
        #self.parse_info_for_issue()

    def parse_info_for_issue(self) -> None:
        """Parse some the languages and text_styles page properties.

        This information is necessary for the `NewspaperIssue` this page is
        from, and is thus necessary upon creation of this page instead of when
        it's parsed. 
        Parsing both properties at once allows to prevent unnecessary calls to
        `self.xml` which are costly in processing time when repeated.
        """
        alto_doc = self.xml
        self.language_list = self.languages(alto_doc)
        _ = self.text_styles(alto_doc)

    def add_issue(self, issue: NewspaperIssue) -> None:
        self.issue = issue
    
    @property
    def text_styles(
        self, alto_doc: BeautifulSoup | None = None
    ) -> list[dict[str, float, str]]:
        """Return the text style fonts for this page. 

        Multiple pages have the same fonts, with different IDs.
        Each style returned by the `parse_style` function also contains this
        page's number to know which IDs correspond to each page's fonts.
        
        Returns:
            list[dict[str, float, str]]: List of text styles for this page
        """
        # only parse the xml for the text styles once
        if not self.styles_dict:
            alto_doc = self.xml if alto_doc is None else alto_doc
            self.styles_dict = {
                x.get('ID'): parse_style(x, page_num=self.number)
                for x in alto_doc.findAll('TextStyle')}
        return list(self.styles_dict.values())
    
    @property
    def languages(self, alto_doc: BeautifulSoup | None = None) -> list[str]:
        """Return the languages present on the page.

        Given that each KB page is considered as a content item, it can happen
        that multiple languages are present in the text.

        Returns:
            list[str]: List of languages present in this page's text.
        """
        # only parse the xml for the languages once
        if not self.language_list:
            alto_doc = self.xml if alto_doc is None else alto_doc
            lang_map = map(
                lambda x: x.get('language'), alto_doc.findAll('TextBlock')
            )
            self.language_list = list(set(lang_map))
        return self.language_list
    
    @property
    def ci_id(self) -> str:
        """Return the content item ID of the page.

        Given that KB data do not entail article-level segmentation,
        each page is considered as a content item. Thus, to mint the content
        item ID we take the canonical page ID and simply replace the "p"
        prefix with "i".

        Returns:
            str: Content item id corresponding to this page.
        """
        split = self.id.split('-')
        split[-1] = split[-1].replace('p', 'i')
        return "-".join(split)

    def parse(self) -> None:
        doc = self.xml
        pselement = doc.find('PrintSpace')
        ci_id = self.ci_id
        
        text_blocks = pselement.findAll('TextBlock')
        mappings = {k.get('ID'): ci_id for k in pselement.findAll('TextBlock')}
        page_data, notes = parse_printspace(pselement, mappings, self.styles_dict)
        
        # the coordinates in the XML files are already correct
        self.page_data['cc'], self.page_data['r'] = True, page_data
        self.language_list = self.languages(doc)

        # Add notes to page data
        if len(notes) > 0:
            self.page_data['n'] = notes
        return notes

class KbNewspaperIssue(NewspaperIssue):
    """Newspaper issue in KB Didl/Alto format.

    Args:
        issue_dir (KBIssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. `nwb-1874-01-09-a`).
        edition (str): Lower case letter ordering issues of the same day.
        journal (str): Newspaper unique identifier or name.
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        pages (list): list of :obj:`NewspaperPage` instances from this issue.
        rights (str): Access rights applicable to this issue.
        page_files (list[tuple[str, str]]): List of pages found for this issue.
            A page is a tuple (page_canonical_id, filename), where filename 
            corresponds to an Alto XML file present in the `path` directory.
        iiif_identifier (str): KB IIIF identifier for this issue.
        content_items (list[dict[str,Any]]): Content items from this issue.
        _notes (list[str]): Notes of missing pages gathered while parsing.
    """

    def __init__(self, issue_dir: KbIssueDir) -> None:
        super().__init__(issue_dir)
        # of format 'DDD:ddd:xxxxxxxxx:mpeg21'
        self.api_identifier = issue_dir.identifier
        # of format 'ddd:xxxxxxxxx:mpeg21'
        self.identifier = issue_dir.identifier[:4]
        self.content_items = []

        self.text_styles = [] 

        self._find_pages()
        self._parse_didl()

    @property
    def xml(self) -> BeautifulSoup:
        """Read Didl XML file of the issue and create a BeautifulSoup object.

        During the processing, some IO errors can randomly happen when listing
        the contents of the directory, or opening files, preventing the correct
        parsing of the issue. The error is raised after the third try.
        If the directory does not contain any Mets file, only try once.
    
        Returns:
            BeautifulSoup: BeautifulSoup object with Mets XML of the issue.
        """
        tries = 3
        for i in range(tries):
            try:
                didl_file = [
                    os.path.join(self.path, f)
                    for f in os.listdir(self.path)
                    if 'didl.xml' in f.lower()
                ]
                if len(didl_file) == 0:
                    logger.critical(f"Could not find Didl file in {self.path}")
                    tries = 1
                
                didl_file = didl_file[0]

                with open(didl_file, 'r', encoding="utf-8") as f:
                    raw_xml = f.read()
                
                didl_doc = BeautifulSoup(raw_xml, 'xml')

                return didl_doc
            except IOError as e:
                if i < tries - 1: # i is zero indexed
                    logger.warning(f"Caught error for {self.id}, "
                                   f"retrying (up to {tries} times) to read "
                                   f"xml file or listing the dir. Error: {e}.")
                    continue
                else:
                    logger.warning("Reached maximum amount of "
                                   f"errors for {self.id}.")
                    raise e

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        All the page files present in this issue's base directory are already
        sorted by page number in `self.page_files`, so if any number is missing
        it means the page is itself missing.

        Created :obj:`KbNewspaperPage` instances are added to :attr:`pages`.

        Raises:
            ValueError: No page was found for this issue.
        """
        page_file_names = find_alto_files_or_retry(self.path, self.id, 
                                                   name_contents='alto.xml')

        page_numbers = [int(fname.split('_')[-2]) for fname in page_file_names]

        page_canonical_names = [
            "{}-p{}".format(self.id, str(page_n).zfill(4))
            for page_n in page_numbers
        ]

        self.pages = []
        for filename, page_no, page_id in zip(page_file_names, 
                                              page_numbers, 
                                              page_canonical_names):
            try:
                self.pages.append(
                    KbNewspaperPage(page_id, page_no, filename, self.path)
                )
            except Exception as e:
                logger.error(
                    f'Adding page {page_no} {page_id} {filename}',
                    f'raised following exception: {e}'
                )
                raise e
            
    def _parse_content_parts(
        self, item_tag: Tag
    ) -> tuple[dict[str, Any], list[int]]:
        """Parse the various parts composing the given content item.

        Content items (often representing articles) are separated into various
        parts (title, paragraphs etc), which are identified with the IDs used
        in the corresponding ALTO files.

        Args:
            item_tag (Tag): Tag corresponding to a content item from the issue.

        Returns:
            tuple[dict[str, Any], list[int]]: List of parts composing the item.
        """
        parts = []
        page_nums = set()

        for ci_pt in item_tag.find_all('dcx:article-part'):
            for block in ci_pt.find_all('TextBlock'):
                # component id in the ALTO files 
                comp_id = block.get('ID')
                # page name
                comp_filename = block.parent.get('alto')
                # page id in the KB identifier system
                comp_page_id = ci_pt.get('pageid')
                # extract the page number from the kb identifier
                comp_page_no = int(comp_page_id.split(':')[-1].replace('p', ''))

                page_nums.add(comp_page_no)

                parts.append({
                    'comp_id': comp_id,
                    'comp_filename': comp_filename,
                    'comp_page_id': comp_page_id,
                    'comp_page_no': comp_page_no
                })

        page_nums = list(page_nums)

        parts, page_nums
            
    def _parse_content_item(
        self, item_tag: Tag, didl_doc: BeautifulSoup
    ) -> dict[str, Any]:
        """Parse the given tag representing a content item form the Didl file.

        Args:
            item_tag (Tag): Tag corresponding to a content item from the issue.
            didl_doc (BeautifulSoup): Contents of this issue's Didl XML file.

        Returns:
            dict[str, Any]: Parsed content item based to the Canonical format.
        """
        # of format '{issue_identifier}:a{article number}'
        item_kb_id = item_tag.get('dc:identifier')
        item_num = int(item_kb_id.split(':')[-1].replace('a', ''))

        elem_type = item_tag.find('dc:subject').text

        if elem_type not in TYPE_MAPPING.keys:
            logger.warning(f"Found new content item type: {elem_type}")
        else:
            # uniformize types
            item_type = TYPE_MAPPING[elem_type]

        item_title = item_tag.find('dc:title').text

        art_parts, page_nums = self._parse_content_parts(item_tag)

        metadata = {
            'id': "{}-i{}".format(self.id, str(item_num).zfill(4)),
            'tp': item_type,
            'pp': page_nums,
            't': item_title,
        }

        # add language (only at issue level for KB) 
        issue_lang = didl_doc.find('language')
        if issue_lang is not None:
            metadata['l'] = issue_lang.text
        else:
            m = f"Issue {self.id} did not contain language information"
            logger.info(m)
            self._notes.append(m)
        
        content_item = {
            "m": metadata,
            "l": {
                "id": item_kb_id,
                "parts": art_parts
            }
        }

        return content_item
             
    def _parse_content_items(
        self, didl_doc: BeautifulSoup
    ) -> list[dict[str, Any]]:
        """Parse the content items ofr this issue based on the didl file.

        Note: 
            No case of segmented illustration was found in the samples, 
            and all items segmented by OLR are named "articles".

        Args:
            didl_doc (BeautifulSoup): Contents of the Didl file for this issue.

        Returns:
            list[dict[str, Any]]: List of content items in Canonical format.
        """
        # fetch the articles page by page, to ensure they are sorted
        articles, content_items = [], []
        
        article_id_parts = [self.identifier, 'a\d{4}']
        articles = didl_doc.findAll(
            'Item', {'dc:identifier': re.compile(':'.join(article_id_parts))}
        )

        # TODO double check for illustration and/or ask KB.

        for item in articles:
            # Parse each article content item
            content_items.append(self._parse_content_item(item, didl_doc))

        return content_items


    def _parse_didl(self) -> None:
        """Parse the Mets XML file corresponding to this issue.

        Once the :attr:`issue_data` is created, containing all the relevant 
        information in the canonical Issue format, the `ReroNewspaperIssue`
        instance is ready for serialization.
        """
        didl_doc = self.xml
        
        # Parse all the content items
        content_items = self._parse_content_items(didl_doc)
        
        text_styles = []
        for p in self.pages:
            text_styles.extend(p.text_styles)

        self.issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "id": self.id,
            "i": content_items,
            "ar": self.rights,
            "pp": [p.id for p in self.pages],
            "s": text_styles
        }

        if self._notes:
            self.issue_data["n"] = "\n".join(self._notes)
