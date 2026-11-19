"""This module contains the definition of RTS importer classes.

The classes define Issues and audio record objects which convert RTS ASR data
to a unified canonical format.
"""

import os
import logging
from time import strftime, gmtime
from collections import Counter
from typing import Any

from bs4 import BeautifulSoup
from mutagen.mp3 import MP3

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

from text_preparation.importers import CONTENTITEM_TYPE_ARTICLE
from text_preparation.importers.classes import CanonicalIssue, CanonicalAudioRecord
from text_preparation.importers.rts.helpers import get_utterances

logger = logging.getLogger(__name__)

IIIF_ENDPOINT_URI = "https://impresso-project.ch/media/audio/"

# TODO update and add new languages once they are found in the data.
LANG_MAPPING = {"fre": "fr", "fre-rts": "fr"}


class RTSBroadcastAudioRecord(CanonicalAudioRecord):
    """Radio-Broadcast Audio Record for RTS's ASR format.

    Args:
        _id (str): Canonical Audio Record ID (e.g. ``CFCE-1900-01-02-a-r0001``).
        number (int): Record number (for compatibility with other source mediums).

    Attributes:
        id (str): Canonical Audio Record ID (e.g. ``CFCE-1900-01-02-a-r0001``).
        number (int): Record number.
        record_data (dict[str, Any]): Audio record data according to canonical format.
        issue (CanonicalIssue | None): Issue this page is from.
    """

    def __init__(self, _id: str, number: int, xml_filepath: str, mp3_filepath: str) -> None:
        super().__init__(_id, number)
        self.xml_filepath = xml_filepath
        self.mp3_filepath = mp3_filepath
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
        }

    def create_iiif(self) -> str:
        """Create the IIIF URI for this audio record from all its parts

        Returns:
            str: Created IIIF URI for this audio record.
        """
        internal_path = os.path.dirname(self.id.replace("-", "/"))
        return os.path.join(IIIF_ENDPOINT_URI, "RTS", internal_path, f"{self.id}.mp3")

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
        formatted_dur = strftime("%H:%M:%S", gmtime(dur_in_sec))
        # also extract the milliseconds
        milliseconds = int((dur_in_sec % 1) * 1000)
        millisecs_dur = f"{formatted_dur}.{milliseconds:03d}"

        if self.issue.duration is not None and self.issue.duration != millisecs_dur:
            msg = f"audio {self.id} - The found duration {millisecs_dur} does not match the on from the metadata {self.issue.duration}!"
            print(msg)
            logger.info(msg)

        return formatted_dur

    def parse(self) -> None:

        if self.record_data["dur"] == "":
            # if the duration is not yet defined at this stage, define it.
            self.record_data["dur"] = self._get_duration()
        xml_doc = self.xml

        utterances = get_utterances(xml_doc)

        if len(utterances) != 0:
            section_stime = utterances[0]["tc"][0]
        else:
            section_stime = 0

        section_etime = max(float(ss.get("etime")) for ss in xml_doc.findAll("SpeechSegment"))

        self.record_data["s"] = [
            {
                "tc": [section_stime, section_etime - section_stime],
                "u": utterances,
                "pOf": self.issue.content_items[0]["m"]["id"],
            }
        ]


class RTSBroadcastIssue(CanonicalIssue):
    """Radio-Broadcast Issue for RTS's OCR format.

    Args:
        issue_dir (IssueDir): Identifying information about the issue.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``[alias]-1940-01-05-a``).
        edition (str): Lower case letter ordering issues of the same day.
        alias (str): Media title unique alias (identifier or name).
        path (str): Path to directory containing the issue's OCR data.
        date (datetime.date): Publication date of issue.
        issue_data (dict[str, Any]): Issue data according to canonical format.
        audio_records (list): list of :obj: `RTSBroadcastAudioRecord` instances from this issue.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        super().__init__(issue_dir)

        self.metadata = issue_dir.issue_metadata
        # recover and add all the provider given metadata which will be included almost "as-is" in the issues
        # self.provided_metadata = issue_dir.issue_metadata["partner_provided_metadata"]

        self._notes = []
        self.audio_records = []

        # parse all the boradcast!!
        # self._find_asr_files()
        self._find_audios()
        self._parse_content_item()

        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "i": self.content_items,
            "rr": [r.id for r in self.audio_records],
            # keep track of braodcasts for which the data is known to be inexact
            "is_exact_date": self.metadata["exact_date"],
        }

        # add the radio program and channel to the data if they were not None
        if self.metadata["broadcast_program_name"]:
            self.issue_data["rp"] = self.metadata["broadcast_program_name"]
        if self.metadata["radio_channel"]:
            self.issue_data["rc"] = self.metadata["radio_channel"]

        # if we know the full duration of the record, save it
        self.duration = (
            None
            if (
                self.metadata["work_duration"] is None
                or self.metadata["work_duration"] == "00:00:00.000"
            )
            else self.metadata["work_duration"]
        )

        # recover and lightly clean all the provider given metadata which will be included almost "as-is" in the issues
        self.issue_data["provided_metadata"] = self._clean_provided_metadata()

        self.issue_data["n"] = self._notes

    def _find_pages(self) -> None:
        # Not defined in this context
        pass

    def _find_audios(self) -> None:

        # define the base mp3 and xml paths
        self.xml_filepath = os.path.join(self.path, self.metadata["xml_filepath"])
        self.mp3_filepath = os.path.join(self.path, self.metadata["mp3_filepath"])

        audio_id = f"{self.id}-r0001"

        if not os.path.exists(self.mp3_filepath):
            msg = f"{self.id} - The issue's audio record MP3 file {self.mp3_filepath} cannot be found!"
            print(msg)
            logger.warning(msg)
            self._notes.append(msg)

        self.audio_records.append(
            RTSBroadcastAudioRecord(audio_id, 1, self.xml_filepath, self.mp3_filepath)
        )

    def _find_lang(self) -> str | None:

        xml_doc = self.audio_records[0].xml

        # sometimes there is no contents to the xml (no speechsegments)
        # --> directly identify this and raise an error!
        xml_speech_segs = xml_doc.findAll("SpeechSegment")
        if not xml_speech_segs:
            msg = f"{self.id} - No SpeechSegments were found in the xml file! Raising an error as this issue cannot be processed."
            print(msg)
            logger.error(msg)
            raise Exception(msg)

        # identify all the languages found in the xml (speakers or speechsegments)
        langs = Counter(
            [s.get("lang") for s in xml_doc.find_all("Speaker") if s.get("lang") is not None]
            + [
                ss.get("lang")
                for ss in xml_doc.find_all("SpeechSegment")
                if ss.get("lang") is not None
            ]
        )
        if len(langs) > 1:
            msg = (
                f"{self.id} - Warning, more than one language was found in the ASR XML. "
                f"Choosing the most frequent one: {langs}."
            )
            logger.warning(msg)
            print(msg)
            self._notes.append(msg)

        if not langs:
            return None

        return LANG_MAPPING[max(langs, key=langs.get)]

    def _parse_content_item(self) -> None:

        ci_metadata = {
            "id": f"{self.id}-i{str(1).zfill(4)}",
            "lg": self._find_lang(),
            "rr": [r.number for r in self.audio_records],
            # assign the pre-normalized type, and "article" by default if it's not defined
            "tp": (
                self.metadata["broadcast_type"]
                if self.metadata["broadcast_type"]
                else CONTENTITEM_TYPE_ARTICLE
            ),
            "ro": 1,
        }

        if self.metadata["broadcast_episode_title"] is not None:
            ci_metadata["t"] = self.metadata["broadcast_episode_title"]

        if self.metadata["participants"] is not None:
            # process and set the speakers
            ci_metadata["speakers"] = self._prepare_speakers()

        if self.metadata["content_summary"] is not None:
            # set the summary or the thematical descriptors?
            ci_metadata["archival_note"] = self.metadata["content_summary"]

        # the legacy we can provide is the original notice ID and filename in the metadata
        ci_legacy = {
            "OID": self.metadata["OID"],
            "src_files": {
                "audio_xml": self.metadata["xml_filepath"],
                "audio_mp3": self.metadata["mp3_filepath"],
            },
        }

        self.content_items = [{"m": ci_metadata, "l": ci_legacy}]

    def _prepare_speakers(self) -> list[str | dict]:
        """Parse the speakers objects.
        The speakers are typically in the format:
        {
            "name": "Schwok, René",
            "function": "Interviewé/e",
            "role": "chargé de recherche à l'Institut universitaire des Hautes Etudes Internationales de Genève"
        }
        """
        # For now we will just set the names and surnames in the "speakers" field.
        # If necessary, the full dict will still be available in the "participants" entry
        full_names = []
        for s_dict in self.metadata["participants"]:
            # the name will always be formatted as [surname], [firstname] unless we don't know it ("inconnu").
            name = (
                # separate the first and last name, swapp them and rejoin them.
                " ".join(s_dict["name"].split(", ")[::-1])
                if ", " in s_dict["name"]
                else s_dict["name"]
            )
            full_names.append(name)

        return full_names

    def _clean_provided_metadata(self) -> dict[str, Any]:

        clean_meta = {}
        for k, v in self.metadata.items():
            # filter out some unnecessary keys
            if k not in [
                "mp3_filepath",
                "xml_filepath",
                "exact_date",
                "radio_channel",
                "broadcast_type",
                "OID",
            ]:
                if v is not None and not (isinstance(v, list) and not v):
                    # set all the non-null and non-empty values
                    clean_meta[k] = v

        return clean_meta
