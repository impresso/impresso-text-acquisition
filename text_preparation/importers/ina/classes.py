import os
import logging
import json
from time import strftime
from typing import Any

from bs4 import BeautifulSoup

from text_preparation.importers.classes import CanonicalIssue, CanonicalAudioRecord
from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/api/proxy/iiif/"


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

    def __init__(self, _id: str, number: int, xml_filepath: str) -> None:
        super().__init__(_id, number)
        # TODO fix for the correct IIIF
        self.xml_filepath = xml_filepath
        self.json_filepath = xml_filepath.replace(".xml", ".json")
        self.iiif_base_uri = f"{IIIF_ENDPOINT_URI}"
        self.notes = []

        self.page_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "r": [],  # here go the page regions
            "iiif_base_uri": self.iiif_base_uri,
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "cc": True,  # kept for conformity but not very relevant
        }

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue

    @property
    def xml(self) -> BeautifulSoup:
        """Read XML file of the audio record and create a BeautifulSoup object.

        Returns:
            BeautifulSoup: BeautifulSoup object with XML of the audio record.
        """
        # In case of I/O error, retry twice,
        tries = 3
        for i in range(tries):
            try:
                with open(self.xml_filepath, "r", encoding="utf-8") as f:
                    raw_xml = f.read()

                xml_doc = BeautifulSoup(raw_xml, "xml")
                return xml_doc
            except IOError as e:
                if i < tries - 1:  # i is zero indexed
                    msg = (
                        f"Caught error for {self.id}, retrying (up to {tries} "
                        f"times) to read xml file. Error: {e}."
                    )
                    logger.error(msg)
                    continue
                else:
                    logger.error("Reached maximum amount of errors for %s.", self.id)
                    raise e

    def parse(self) -> None:
        xml_doc = self.xml

        return None


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
        self.metadata_file = issue_dir.metadata_file
        self._notes = []
        self.audio_records = []

        self._find_asr_files()
        self._find_audios()
        self._parse_content_item()
        self._add_broadcast_metadata()

        self.issue_data = {
            "id": self.id,
            "cdt": strftime("%Y-%m-%d %H:%M:%S"),
            "ts": timestamp(),
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "i": self.content_items,
            "rr": [r.id for r in self.audio_records],
            "rc": "",
        }

        if self.program:
            self.issue_data["rp"] = self.program

        self.issue_data["n"] = self._notes

    def _find_asr_files(self) -> None:
        # TODO modifiy this once we have more context
        # the key of this rb is the directory
        self.rb_issue_key = os.path.basename(self.path)
        # read the contents of the metadata json
        with open(self.metadata_file, "r", encoding="utf-8") as f:
            metadata_json = json.load(f)

        self.metadata = metadata_json[self.rb_issue_key]
        xml_filename = f"{self.metadata['Identifiant de la notice']}_{self.metadata['Noms fichers']}_EXPORT.xml"

        dir_contents = os.listdir(self.path)
        if xml_filename not in dir_contents:
            msg = (
                f"{self.id} - The issue's folder {self.path} does not contain the expected"
                f" xml file {xml_filename}. Contents of the folder are {dir_contents} will be used."
            )
            print(msg)
            logger.warning(msg)
            self._notes.append(msg)
            xml_files = [f for f in dir_contents if f.endswith(".xml")]
            if len(xml_files) > 1:
                msg = f"{self.id} - There is more than one xml file in dir!!"
                print(msg)
                logger.warning(msg)
                self._notes.append(msg)
                raise Exception(msg)
            else:
                xml_filename = xml_files[0]

        self.xml_file_path = os.path.join(os.path.dirname(self.path), xml_filename)
        self.json_file_path = self.xml_file_path.replace(".xml", ".json")

    def _find_audios(self) -> None:
        # currently nothing but once we have put the audios in a IIIF server
        # TODO change to go fetch the actual final audio MP3 file
        return None

    def _parse_content_item(self) -> None:
        self.content_items = []

    def _add_broadcast_metadata(self) -> None:
        self.program = None
        pass
