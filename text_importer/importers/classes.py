"""This module contains the definition of abstract classes."""

from abc import ABC, abstractmethod

from impresso_commons.path.path_fs import IssueDir, canonical_path

from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()


class NewspaperIssue(ABC):
    """Abstract class representing a newspaper issue.

    Each text importer needs to define a subclass of `NewspaperIssue` which
    specifies the logic to handle OCR data in a given format (e.g. Olive).
    """
    def __init__(self, issue_dir: IssueDir):
        self.id = canonical_path(issue_dir, path_type='dir').replace('/', '-')
        self.edition = issue_dir.edition
        self.journal = issue_dir.journal
        self.path = issue_dir.path
        self.date = issue_dir.date
        self.issue_data = {}
        self._notes = []
        self.pages = []
        self.rights = issue_dir.rights

    @abstractmethod
    def _find_pages(self):
        pass

    @property
    def issuedir(self) -> IssueDir:
        return IssueDir(self.journal, self.date, self.edition, self.path)

    def to_json(self) -> str:

        """Validates ``self.issue_data`` against ``IssueSchema`` & serializes it
         to string.

        .. note ::
            Validation adds a substantial overhead to computing time. For
            serialization of large amounts of issues it is recommendable to
            bypass schema validation.
        """
        issue = IssueSchema(**self.issue_data)
        return issue.serialize()


class NewspaperPage(ABC):
    """Abstract class representing a newspaper page.

    Each text importer needs to define a subclass of ``NewspaperPage`` which
    specifies the logic to handle OCR data in a given format (e.g. Alto).
    """
    def __init__(self, _id: str, number: int):
        """Initializes a ``NewspaperPage`` object.

        :param str _id: Description of parameter `_id`.
        :param int number: Description of parameter `number`.

        """
        self.id = _id
        self.number = number
        self.page_data = {}
        self.issue = None

    def to_json(self) -> str:
        """Validates ``self.page.data`` & serializes it to string.

        .. note ::
            Validation adds a substantial overhead to computing time. For
            serialization of large amounts of pages it is recommendable to
            bypass schema validation.
        """
        page = Pageschema(**self.page_data)
        return page.serialize()

    @abstractmethod
    def add_issue(self, issue: NewspaperIssue):
        """Adds to a page object its parent, i.e. the newspaper issue.

        This allows each page to preserve contextual information coming from
        the newspaper issue.
        """
        pass

    @abstractmethod
    def parse(self):
        pass