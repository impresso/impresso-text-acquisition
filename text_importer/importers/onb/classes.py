"""This module contains the definition of the ONB importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the ONB version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.

Note: These classes only handle the case for ANNO data for now. Titles which
are part of the ANNOP collection cannot be imported with them for now.
Either two separate importers will be implemented, or the titles part of ANNOP
will first be converted to Alto before using this importer.
"""

import logging
import os
from time import strftime
from bs4 import BeautifulSoup

from text_importer.importers.classes import NewspaperIssue
from text_importer.importers.mets_alto.alto import parse_printspace, parse_style
from text_importer.importers.mets_alto.classes import MetsAltoNewspaperPage
from text_importer.importers.onb.detect import OnbIssueDir

logger = logging.getLogger(__name__)
IIIF_IMG_BASE_URI = "https://iiif.onb.ac.at/images/ANNO"
IIIF_PRES_BASE_URI = "https://iiif.onb.ac.at/presentation/ANNO"
IIIF_MANIFEST_SUFFIX = "manifest"


class ONBNewspaperPage(MetsAltoNewspaperPage):
    """Newspaper page in ONB (Mets/Alto) format.

    Args:
        _id (str): Canonical page ID.
        number (int): Page number.
        filename (str): Name of the Alto XML file for this page.
        basedir (str): Base directory where Alto files are located.
        iiif_identifier (str): ONB IIIF identifier for this page's issue.
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

    def __init__(self, _id: str, number: int, filename: str, basedir: str, 
                 iiif_identifier: str, encoding: str = 'utf-8') -> None:
        super().__init__(_id, number, filename, basedir, encoding)
        # create iiif base image URI for ONB
        self.iiif = os.path.join(IIIF_IMG_BASE_URI, 
                                 iiif_identifier, 
                                 str(number).zfill(8))
        self.page_data['iiif_img_base_uri'] = self.iiif
        self.parse_info_for_issue()

    def parse_info_for_issue(self) -> None:
        alto_doc = self.xml
        self.language_list = self.languages(alto_doc)
        _ = self.text_styles(alto_doc)

    def add_issue(self, issue: NewspaperIssue) -> None:
        self.issue = issue
    
    @property
    def text_styles(self, alto_doc: BeautifulSoup | None = None) -> list[str]:
        """Return the TODO

        Given that each ONB page is considered as a content item, it can happen
        that multiple languages are present in the text.

        Returns:
            list[str]: List of languages present in this page's text.
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

        Given that each ONB page is considered as a content item, it can happen
        that multiple languages are present in the text.

        Returns:
            list[str]: List of languages present in this page's text.
        """
        # only parse the xml for the languages once
        if not self.language_list:
            alto_doc = self.xml if alto_doc is None else alto_doc
            lang_map = map(lambda x: x.get('language'), alto_doc.findAll('TextBlock'))
            self.language_list = list(set(lang_map))
        return self.language_list
    
    @property
    def ci_id(self) -> str:
        """Return the content item ID of the page.

        Given that ONB data do not entail article-level segmentation,
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

class ONBNewspaperIssue(NewspaperIssue):
    """Newspaper issue in ONB Mets/Alto format.

    Note: 
        ONB is in ALTO format, but there isn't any Mets file. So in that case,
        issues are simply a collection of pages.

    Args:
        issue_dir (ONBIssueDir): Identifying information about the issue.

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
        iiif_identifier (str): ONB IIIF identifier for this issue.
        content_items (list[dict[str,Any]]): Content items from this issue.
        _notes (list[str]): Notes of missing pages gathered while parsing.
    """

    def __init__(self, issue_dir: OnbIssueDir) -> None:
        super().__init__(issue_dir)
        # create the canonical issue id
        self.page_files = issue_dir.pages
        self.iiif_identifier = ''.join(self.path.split('/')[-4:])
        self.content_items = []

        self.text_styles = [] #self._parse_font_styles()

        self._find_pages()
        self._find_content_items()

        iiif_manifest = os.path.join(IIIF_PRES_BASE_URI, 
                                     self.iiif_identifier,
                                     IIIF_MANIFEST_SUFFIX)
        
        self.issue_data = {
            'id': self.id,
            'cdt': strftime("%Y-%m-%d %H:%M:%S"),
            'i': self.content_items,
            'ar': self.rights,
            'pp': [p.id for p in self.pages],
            'iiif_manifest_uri': iiif_manifest,
            's': self.text_styles,
            'notes': self._notes
        }

    def _parse_font_styles(self):
        """ Parses the styles at page level"""
        style_divs = self.xml.findAll("TextStyle")
        
        styles = []
        for d in style_divs:
            styles.append(parse_style(d))
        
        return styles

    def _find_pages(self) -> None:
        """Detect and create the issue pages using the relevant Alto XML files.

        All the page files present in this issue's base directory are already
        sorted by page number in `self.page_files`, so if any number is missing
        it means the page is itself missing.

        Created :obj:`ONBNewspaperPage` instances are added to :attr:`pages`.

        Raises:
            ValueError: No page was found for this issue.
        """
        if len(self.page_files) == 0:
            raise ValueError(f"Could not find any page for {self.id}.")

        for n, (p_id, p_file) in enumerate(self.page_files):
            # page_files is sorted by page number
            if int(p_file.replace('.xml', '')) == n + 1:
                page = ONBNewspaperPage(p_id, n + 1, p_file, self.path, 
                                        self.iiif_identifier)
                self.pages.append(page)
            else:
                # No list of pages per issue;
                # a page is missing if a number is skipped.
                self._notes.append(f"Issue {self.id}: No Alto "
                                   f"file for page {n + 1} in {self.path}")
                
    def _find_content_items(self) -> None:
        """Create content items for the pages in this issue.

        Given that ONB data do not entail article-level segmentation,
        each page is considered as a content item.
        """
        for page in sorted(self.pages, key=lambda x: x.number):
            p_num = page.number
            ci = {
                'm': {
                    'id': self.id + '-i' + str(p_num).zfill(4),
                    'pp': [p_num],
                    'l': page.languages,
                    'tp': 'page',
                }
            }
            self.content_items.append(ci)