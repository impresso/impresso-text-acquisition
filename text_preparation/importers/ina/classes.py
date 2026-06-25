"""This module contains the definition of INA importer classes.

The classes define Issues and Audio record objects which convert ASR data
to a unified canoncial format.
"""

import os
import logging
import json
from time import strftime, gmtime
from typing import Any

from bs4 import BeautifulSoup
from mutagen.mp3 import MP3

from impresso_essentials.utils import IssueDir, SourceType, SourceMedium, timestamp

from text_preparation.importers import CONTENTITEM_TYPE_ARTICLE
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

    def __init__(self, _id: str, number: int, json_filepath: str, mp3_filepath: str) -> None:
        super().__init__(_id, number)
        # the text is actually stored in a json
        self.json_filepath = json_filepath
        self.mp3_filepath = mp3_filepath
        self.iiif_base_uri = self.create_iiif()
        self.dur_in_sec = None
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
        return os.path.join(IIIF_ENDPOINT_URI, "INA", internal_path, f"{self.id}.mp3")

    def add_issue(self, issue: CanonicalIssue) -> None:
        self.issue = issue

    @property
    def json(self) -> BeautifulSoup:
        """Read json file of the audio record and return the corresponding dict object.

        Returns:
            BeautifulSoup: BeautifulSoup object with XML of the audio record.
        """
        # In case of I/O error, retry twice,
        tries = 3
        for i in range(tries):
            try:
                with open(self.json_filepath, "r", encoding="utf-8") as f:
                    json_contents = json.load(f)

                return json_contents
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

    def _set_duration(self) -> None:
        self.dur_in_secs = MP3(self.mp3_filepath).info.length
        formatted_dur = strftime("%H:%M:%S", gmtime(self.dur_in_secs))

        if self.issue.duration is not None and self.issue.duration != formatted_dur:
            msg = f"audio {self.id} - The found duration {formatted_dur} does not match the on from the metadata {self.issue.duration}!"
            print(msg)
            logger.info(msg)

        # set the duration in the record data
        self.record_data["dur"] = formatted_dur

    def _get_duration(self, in_secs=True) -> str:
        # set the duration in the record data
        if self.record_data["dur"] == "":
            self._set_duration()

        return self.dur_in_sec if in_secs else self.record_data["dur"]

    def parse(self) -> None:

        if self.record_data["dur"] == "":
            self._set_duration()

        json_doc = self.json
        # TODO
        utterances = get_utterances(json_doc)

        section_stime = utterances[0]["tc"][0]
        # the section end time is the end time of the last word in the last speech seg
        section_etime = json_doc[-1]["words"][-1]["end"]
        # max(float(ss.get("etime")) for ss in json_doc.findAll("SpeechSegment"))

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
        self.metadata = issue_dir.issue_metadata
        self._notes = []
        self.audio_records = []

        self._find_audios()
        self._parse_content_item()

        self.issue_data = {
            "id": self.id,
            "ts": timestamp(),
            "st": SourceType.RB.value,
            "sm": SourceMedium.AO.value,
            "i": self.content_items,
            "rr": [r.id for r in self.audio_records],
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
                or self.metadata["work_duration"] == "00:00:00"
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

        # define the base mp3 and xml paths, both are lists
        self.json_filepath = [os.path.join(self.path, f) for f in self.metadata["xml_filepath"]]
        self.mp3_filepath = [os.path.join(self.path, f) for f in self.metadata["mp3_filepath"]]

        if len(self.mp3_filepath) > 1:
            msg = f"{self.id} - This issue has more than one audio file!!"
            print(msg)

        full_audio_length = 0

        for idx, mp3_path in enumerate(self.mp3_filepath):
            if not os.path.exists(mp3_path):
                msg = f"{self.id} - The issue's audio record n°{idx+1} MP3 file {mp3_path} cannot be found!"
                print(msg)
                logger.warning(msg)
                self._notes.append(msg)

            audio_id = f"{self.id}-r{str(idx+1).zfill(4)}"

            audio_rec = INABroadcastAudioRecord(
                audio_id, idx + 1, self.json_filepath, self.mp3_filepath
            )

            self.audio_records.append(audio_rec)

            # sum the duration in seconds of the records from the length of each audio
            full_audio_length += audio_rec._get_duration()

        full_formatted_dur = strftime("%H:%M:%S", gmtime(full_audio_length))
        if self.duration is not None and self.duration != full_formatted_dur:
            msg = f"audio {self.id} - The sum of the found durations of records ({full_formatted_dur}) does not match the one from the metadata {self.duration}!"
            print(msg)
            self._notes.append(msg)
            logger.info(msg)

    def _parse_content_item(self) -> None:

        ci_metadata = {
            "id": f"{self.id}-i{str(1).zfill(4)}",
            "lg": "fr",
            "rr": [r.number for r in self.audio_records],
            # only this type for now
            "tp": (
                self.metadata["broadcast_type"]
                if self.metadata["broadcast_type"]
                else CONTENTITEM_TYPE_ARTICLE
            ),
            "ro": 1,
        }

        if self.metadata["broadcast_episode_title"] is not None:
            ci_metadata["t"] = self.metadata["broadcast_episode_title"]

        # the legacy we can provide is the original notice ID and filename in the metadata
        ci_legacy = {
            "notice_id": self.metadata["notice_id"],
            "src_files": {
                "audio_json": self.metadata["xml_filepath"],
                "audio_mp3": self.metadata["mp3_filepath"],
            },
        }

        self.content_items = [{"m": ci_metadata, "l": ci_legacy}]

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
                "notice_id",
            ]:
                if v is not None and not (isinstance(v, list) and not v):
                    # set all the non-null and non-empty values
                    clean_meta[k] = v

        return clean_meta
