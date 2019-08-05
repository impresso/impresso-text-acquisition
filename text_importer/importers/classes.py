from abc import ABC, abstractmethod

from impresso_commons.path.path_fs import IssueDir, canonical_path

from text_importer.utils import get_issue_schema, get_page_schema

IssueSchema = get_issue_schema()
Pageschema = get_page_schema()


class NewspaperPage(ABC):
    def __init__(self, _id, number):
        self.id = _id
        self.number = number
        self.page_data = {}
        self.issue = None
    
    def to_json(self) -> str:
        """Validates `page.data` against PageSchema & serializes to string.

        ..note::
            Validation adds a substantial overhead to computing time. For
            serialization of lots of pages it is recommendable to bypass
            schema validation.
        """
        page = Pageschema(**self.page_data)
        return page.serialize()
    
    @abstractmethod
    def add_issue(self, issue):
        pass
    
    @abstractmethod
    def parse(self):
        pass


class NewspaperIssue(ABC):
    
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
        issue = IssueSchema(**self.issue_data)
        return issue.serialize()
