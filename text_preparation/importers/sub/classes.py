"""This module contains the definition of SUB importer classes.

The classes define newspaper Issues and Pages objects which convert OCR data in
the SUB version of the Mets/Alto format to a unified canoncial format.
Theses classes are subclasses of generic Mets/Alto importer classes.
"""

from impresso_essentials.utils import IssueDir, SourceMedium, SourceType, timestamp
from text_preparation.importers.mets_alto import (
    MetsAltoCanonicalIssue,
    MetsAltoCanonicalPage,
)


class BnfEnNewspaperPage(MetsAltoCanonicalPage):

    def __init__(self, _id: str, number: int, filename: str, basedir: str) -> None:
        super().__init__(_id, number, filename, basedir)

    def add_issue(self, issue: MetsAltoCanonicalIssue) -> None:
        # TODO define
        pass


class BnfEnNewspaperIssue(MetsAltoCanonicalIssue):

    def __init__(self, issue_dir: IssueDir) -> None:
        super().__init__(issue_dir)
        # TODO define for SUB case

    def _find_pages(self) -> None:
        # TODO define for SUB case
        pass

    def _parse_mets(self) -> None:
        # TODO define for SUB case
        pass
