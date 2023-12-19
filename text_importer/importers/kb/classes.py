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
from time import strftime
from bs4 import BeautifulSoup

from text_importer.importers.classes import NewspaperIssue
from text_importer.importers.mets_alto.alto import parse_printspace, parse_style
from text_importer.importers.mets_alto.classes import MetsAltoNewspaperPage
from text_importer.importers.kb.detect import KbIssueDir

logger = logging.getLogger(__name__)
IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"
IIIF_SUFFIX = 'info.json'
IIIF_IMAGE_SUFFIX = 'full/full/0/default.jpg'

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

    def __init__(self, _id: str, number: int, filename: str, basedir: str, 
                 iiif_identifier: str, encoding: str = 'utf-8') -> None:
        super().__init__(_id, number, filename, basedir, encoding)
        # create iiif base image URI for KB
        self.iiif = os.path.join(IIIF_ENDPOINT_URI, 
                                 iiif_identifier, 
                                 str(number).zfill(8))
        self.page_data['iiif_img_base_uri'] = self.iiif
        self.parse_info_for_issue()

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
    """Newspaper issue in KB Mets/Alto format.

    Note: 
        KB is in ALTO format, but there isn't any Mets file. So in that case,
        issues are simply a collection of pages.

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
        self.api_identifier = issue_dir.identifier
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

        Note:
            By default the issue Mets file is the only file containing
            `mets.xml` in its file name and located in the directory
            `self.path`. Individual importers can overwrite this behavior
            if necessary.
    
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
        pass
                
    def _parse_content_items(self) -> None:
        """Create content items for the pages in this issue.

        Given that KB data do not entail article-level segmentation,
        each page is considered as a content item.
        """
        pass

    def _parse_didl(self) -> None:
        """Parse the Mets XML file corresponding to this issue.

        Once the :attr:`issue_data` is created, containing all the relevant 
        information in the canonical Issue format, the `ReroNewspaperIssue`
        instance is ready for serialization.
        """
        didl_doc = self.xml
        
        # Parse all the content items
        content_items = self._parse_content_items(didl_doc)
        
        self.issue_data = {
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "id": self.id,
            "i": content_items,
            "ar": self.rights,
            "pp": [p.id for p in self.pages]
        }

        # TODO add text_styles
        if self._notes:
            self.issue_data["n"] = "\n".join(self._notes)
