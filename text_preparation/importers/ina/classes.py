import os
import logging
import json
from time import strftime
from typing import Any
from statistics import mean

from text_preparation.utils import coords_to_xywh
from text_preparation.importers.classes import CanonicalIssue, CanonicalAudioRecord
from text_preparation.importers.swissinfo.helpers import parse_lines, compute_agg_coords
from impresso_essentials.utils import IssueDir, SourceType, SourceMedium

logger = logging.getLogger(__name__)


class INABroadcastAudioRecord(CanonicalAudioRecord):
    """Radio-Broadcast Audio Record for INA's ASR format.

    Args:
        _id (str): Canonical Audio Record ID (e.g. ``-1900-01-02-a-r0001``).
        number (int): Record number (for compatibility with other source mediums).

    Attributes:
        id (str): Canonical Audio Record ID (e.g. ``INA-1900-01-02-a-r0001``).
        number (int): Record number.
        record_data (dict[str, Any]): Audio record data according to canonical format.
        issue (CanonicalIssue | None): Issue this page is from.
    """


class INABroadcastIssue(CanonicalIssue):
    """Radio-Broadcast Issue for INA's OCR format.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``[alias]-1940-01-05-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Media title unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        audio_records (list): list of :obj: `INABroadcastAudioRecord` instances from this issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        super().__init__(issue_dir)
        # self.json_file = self.find_json_file()
        # self.metadata_file = issue_dir.metadata_file
        self._notes = []
        self.audio_records = []

        self._find_audios()
        self._parse_content_item()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "i": self.content_items,
            "rr": [r.id for r in self.audio_records],
            "rc": "",
        }

        if self.program:
            self.issue_data["rp"] = self.program

        self.issue_data["n"] = self._notes

    def _find_audios(self) -> None:

        return None

    def _parse_content_item(self) -> None:
        return None
