"""This module contains the definition of INA importer classes.

The classes define Issues and Audio record objects which convert ASR data
to a unified canoncial format.
"""

import os
import logging
import json
from time import strftime, gmtime
from collections import Counter

from bs4 import BeautifulSoup
from mutagen.mp3 import MP3

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

from text_preparation.importers.classes import CanonicalIssue, CanonicalAudioRecord
from text_preparation.importers.ina.helpers import get_utterances

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/media/audio/"

# TODO update and add new languages once they are found in the data.
LANG_MAPPING = {"fre": "fr"}


class INABroadcastAudioRecord(CanonicalAudioRecord):
    """Radio-Broadcast Audio Record for INA's ASR format.

    Args:
        _id (str): Canonical Audio Record ID (e.g. ``CFCE-1900-01-02-a-r0001``).
        number (int): Record number (for compatibility with other source mediums).

    Attributes:
        id (str): Canonical Audio Record ID (e.g. ``CFCE-1900-01-02-a-r0001``).
        number (int): Record number.
        record_data (dict[str, Any]): Audio record data according to canonical format.
        issue (CanonicalIssue | None): Issue this page is from.
    """

    def __init__(self, _id: str, number: int, xml_filepath: str) -> None:
        super().__init__(_id, number)
        self.xml_filepath = xml_filepath
        self.json_filepath = xml_filepath.replace(".xml", ".json")
        # TODO change once the mp3 files are moved and renamed
        self.mp3_filepath = xml_filepath.replace(".xml", ".MP3")
        self.iiif_base_uri = self.create_iiif()
        self.notes = []

        self.record_data = {
            "id": self.id,
            "ts": timestamp(),
            "s": [],  # here go the audio sections
            "iiif_base_uri": self.iiif_base_uri,
            "stt": "00:00:00",
            "dur": "",
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "cc": True,  # kept for conformity but not very relevant
        }

    def create_iiif(self) -> str:
        """Create the IIIF URI for this audio record from all its parts

        Returns:
            str: Created IIIF URI for this audio record.
        """
        internal_path = os.path.dirname(self.id.replace("-", "/"))
        return os.path.join(IIIF_ENDPOINT_URI, "INA", internal_path, f"{self.id}.mp3")

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

    def _get_duration(self) -> str:
        dur_in_sec = MP3(self.mp3_filepath).info.length
        return strftime("%H:%M:%S", gmtime(dur_in_sec))

    def parse(self) -> None:

        self.record_data["dur"] = self._get_duration()
        xml_doc = self.xml

        utterances = get_utterances(xml_doc)

        section_stime = utterances[0]["tc"][0]
        section_etime = max(float(ss.get("etime")) for ss in xml_doc.findAll("SpeechSegment"))

        self.record_data["s"] = [
            {
                "tc": [section_stime, section_etime - section_stime],
                "u": utterances,
                "pOf": self.issue.content_items[0]["m"]["id"],
            }
        ]


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

        program, channel = self._fetch_broadcast_metadata()

        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "i": self.content_items,
            "rr": [r.id for r in self.audio_records],
        }

        # add the radio program and channel to the data if they were not None
        if program:
            self.issue_data["rp"] = program
        if channel:
            self.issue_data["rc"] = channel

        self.issue_data["n"] = self._notes

    def _find_pages(self) -> None:
        # Not defined in this context
        pass

    def _find_asr_files(self) -> None:
        # TODO modifiy this once we have more context
        # the key of this rb is the directory
        self.rb_issue_key = os.path.basename(self.path)
        # read the contents of the metadata json
        with open(self.metadata_file, "r", encoding="utf-8") as f:
            metadata_json = json.load(f)

        self.metadata = metadata_json[self.rb_issue_key]
        exp_xml_filename = (
            f"{self.metadata['Identifiant de la notice']}_{self.metadata['Noms fichers']}"
        )

        dir_contents = os.listdir(self.path)
        xml_files = [f for f in dir_contents if f.endswith(".xml")]
        if len(xml_files) > 1:
            msg = f"{self.id} - There is more than one xml file in dir!!"
            print(msg)
            logger.warning(msg)
            self._notes.append(msg)
            raise Exception(msg)
        else:
            xml_filename = xml_files[0]

        if exp_xml_filename not in xml_filename:
            msg = (
                f"{self.id} - The issue's folder {self.path} does not contain the expected"
                f" xml file {exp_xml_filename}. Contents of the folder are {dir_contents} will be used."
            )
            print(msg)
            logger.warning(msg)
            self._notes.append(msg)

        self.xml_file_path = os.path.join(self.path, xml_filename)
        self.json_file_path = self.xml_file_path.replace(".xml", ".json")

    def _find_audios(self) -> None:
        # currently nothing but once we have put the audios in a IIIF server
        # TODO change to go fetch the actual final audio MP3 file
        # There is only one audio for each issue
        audio_id = self.metadata["Audio Record ID"]
        self.audio_file_path = self.xml_file_path.replace(".xml", ".MP3")

        if not os.path.exists(self.audio_file_path):
            msg = f"{self.id} - The issue's audio record MP3 file {self.audio_file_path} cannot be found!"
            print(msg)
            logger.warning(msg)
            self._notes.append(msg)

        self.audio_records.append(INABroadcastAudioRecord(audio_id, 1, self.xml_file_path))

    def _find_lang(self) -> str:

        # sometimes the language is given inside the metadata
        if self.metadata["Résumé"] is not None and "En anglais" in self.metadata["Résumé"]:
            return "en"

        else:
            xml_doc = self.audio_records[0].xml
            # identify all the languages found in the xml (speakers or speechsegments)
            langs = Counter(
                [s.get("lang") for s in xml_doc.find_all("Speaker")]
                + [ss.get("lang") for ss in xml_doc.find_all("SpeechSegment")]
            )
            if len(langs) > 1:
                msg = (
                    f"{self.id} - Warning, more than one language was found in the ASR XML. "
                    f"Choosing the most frequent one: {langs}."
                )
                logger.warning(msg)
                print(msg)
                self._notes.append(msg)

            return LANG_MAPPING[max(langs)]

    def _parse_content_item(self) -> None:

        ci_metadata = {
            "id": f"{self.id}-i{str(1).zfill(4)}",
            "lg": self._find_lang(),
            "rr": [r.number for r in self.audio_records],
            # only this type for now
            "tp": "radio_broadcast_episode",
            "ro": 1,
        }

        if self.metadata["Titre propre"] is not None:
            ci_metadata["t"] = self.metadata["Titre propre"]

        if self.metadata["Résumé"] is not None:
            ci_metadata["archival_note"] = self.metadata["Résumé"]

        # the legacy we can provide is the original notice ID and filename in the metadata
        ci_legacy = {
            "source": [
                f"Identifiant de la notice (in metadata): {self.metadata['Identifiant de la notice']}",
                f"Noms fichers (in metadata): {self.metadata['Noms fichers']}",
                f"Noms fichers (in practice): {os.path.basename(self.xml_file_path).replace('.xml', '')}",
            ]
        }

        self.content_items = [{"m": ci_metadata, "l": ci_legacy}]

    def _fetch_broadcast_metadata(self) -> tuple[str | None, str | None]:

        program, channel = None, None
        if self.metadata["Titre collection"] is not None:
            program = self.metadata["Titre collection"]
        if self.metadata["Canal de diffusion"] is not None:
            channel = self.metadata["Canal de diffusion"]
            if self.metadata["Société de programmes"] is not None:
                channel = f"{channel} ({self.metadata['Société de programmes']})"

        return program, channel
