"""INA importer classes for converting ASR data to a unified canonical format.

This module defines Issue and Audio record objects used by the INA importer
to convert Automatic Speech Recognition (ASR) data into the impresso canonical
format for radio-broadcast content.
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
        json_filepath (str): Path to the JSON file containing the ASR transcript.
        mp3_filepath (str): Path to the MP3 audio file.

    Attributes:
        id (str): Canonical Audio Record ID (e.g. ``CFCE-1900-01-02-a-r0001``).
        number (int): Record number.
        json_filepath (str): Path to the JSON file containing the ASR transcript.
        mp3_filepath (str): Path to the MP3 audio file.
        iiif_base_uri (str): Constructed IIIF URI for this audio record.
        dur_in_sec (float | None): Duration of the audio record in seconds.
        notes (list[str]): Informational or warning messages collected during parsing.
        record_data (dict[str, Any]): Audio record data according to canonical format.
        issue (CanonicalIssue | None): Issue this audio record belongs to.
    """

    def __init__(self, _id: str, number: int, json_filepath: str, mp3_filepath: str) -> None:
        """Initialise the audio record and construct its canonical record data skeleton.

        Args:
            _id (str): Canonical Audio Record ID (e.g. ``CFCE-1900-01-02-a-r0001``).
            number (int): Record number (for compatibility with other source mediums).
            json_filepath (str): Path to the JSON file containing the ASR transcript.
            mp3_filepath (str): Path to the MP3 audio file.
        """
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
        """Create the IIIF URI for this audio record from its constituent parts.

        Returns:
            str: Constructed IIIF URI pointing to the MP3 file for this record.
        """
        internal_path = os.path.dirname(self.id.replace("-", "/"))
        return os.path.join(IIIF_ENDPOINT_URI, "INA", internal_path, f"{self.id}.mp3")

    def add_issue(self, issue: CanonicalIssue) -> None:
        """Attach the parent issue to this audio record.

        Args:
            issue (CanonicalIssue): The canonical issue this audio record belongs to.
        """
        self.issue = issue

    @property
    def json(self) -> dict:
        """Read the JSON transcript file and return its contents as a dictionary.

        Retries up to three times on I/O errors before re-raising the exception.

        Returns:
            dict: Parsed contents of the ASR transcript JSON file.

        Raises:
            IOError: If the file cannot be read after the maximum number of retries.
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
        """Read the MP3 file to determine the audio duration and store it.

        Reads the duration from the MP3 metadata, formats it as ``HH:MM:SS``,
        and writes it to ``record_data["dur"]``. Logs a warning if the computed
        duration differs from the duration stored in the parent issue metadata.
        """
        self.dur_in_secs = MP3(self.mp3_filepath).info.length
        formatted_dur = strftime("%H:%M:%S", gmtime(self.dur_in_secs))

        if self.issue and self.issue.duration is not None and self.issue.duration != formatted_dur:
            msg = f"audio {self.id} - The found duration {formatted_dur} does not match the on from the metadata {self.issue.duration}!"
            print(msg)
            logger.info(msg)

        # set the duration in the record data
        self.record_data["dur"] = formatted_dur

    def _get_duration(self, in_secs: bool = True) -> float | str:
        """Return the audio duration, computing it from the MP3 file if necessary.

        Args:
            in_secs (bool): If ``True`` (default), return the raw duration in
                seconds as a float. If ``False``, return the formatted
                ``HH:MM:SS`` string stored in ``record_data``.

        Returns:
            float | str: Duration in seconds when ``in_secs`` is ``True``,
            otherwise the formatted duration string.
        """
        if self.record_data["dur"] == "":
            self._set_duration()

        return self.dur_in_secs if in_secs else self.record_data["dur"]

    def parse(self) -> None:
        """Parse the ASR transcript and populate ``record_data`` with audio sections.

        Reads the JSON transcript, extracts utterances, and builds a single audio
        section spanning the full broadcast. If no utterances are found, an empty
        section list is stored and a warning is appended to ``notes``.
        """
        if self.record_data["dur"] == "":
            self._set_duration()

        json_doc = self.json
        utterances = get_utterances(json_doc)

        if len(utterances) > 0:
            section_stime = utterances[0]["tc"][
                0
            ]  # if len(utterances) > 0 else json_doc[0]["words"][0]["start"]
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
        else:
            msg = f"{self.id} - len(utturances)=0!! json_doc={json_doc}, setting empty sections."
            print(msg)
            self.record_data["s"] = []
            self.notes.append(msg)


class INABroadcastIssue(CanonicalIssue):
    """Radio-Broadcast Issue for INA's ASR format.

    Wraps a single broadcast entry from the INA archive, aggregating its
    associated audio records and constructing the canonical issue representation.

    Args:
        issue_dir (IssueDir): Identifying information about the issue, including
            the path to the data directory and provider-supplied metadata.

    Attributes:
        id (str): Canonical Issue ID (e.g. ``CFCE-1940-01-05-a``).
        edition (str): Lower-case letter ordering issues of the same day.
        alias (str): Media title unique alias (identifier or name).
        path (str): Path to the directory containing the issue's data files.
        date (datetime.date): Broadcast date of the issue.
        metadata (dict[str, Any]): Raw provider-supplied metadata from ``issue_dir``.
        duration (str | None): Full broadcast duration as ``HH:MM:SS``, or ``None``
            if unavailable or zero.
        audio_records (list[INABroadcastAudioRecord]): Audio records belonging to
            this issue.
        issue_data (dict[str, Any]): Issue data serialised to canonical format.
    """

    def __init__(self, issue_dir: IssueDir) -> None:
        """Initialise the issue, discover audio records, and build canonical issue data.

        Args:
            issue_dir (IssueDir): Identifying information about the issue, including
                the path to the data directory and provider-supplied metadata.
        """
        super().__init__(issue_dir)
        self.metadata = issue_dir.issue_metadata
        self._notes = []
        self.audio_records = []

        # if we know the full duration of the record, save it
        self.duration = (
            None
            if (
                self.metadata["work_duration"] is None
                or self.metadata["work_duration"] == "00:00:00"
            )
            else self.metadata["work_duration"]
        )

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
        if self.metadata["broadcast_program_name"] and isinstance(
            self.metadata["broadcast_program_name"], str
        ):
            self.issue_data["rp"] = self.metadata["broadcast_program_name"]
        if self.metadata["radio_channel"] and isinstance(self.metadata["radio_channel"], str):
            self.issue_data["rc"] = self.metadata["radio_channel"]

        # recover and lightly clean all the provider given metadata which will be included almost "as-is" in the issues
        self.issue_data["provided_metadata"] = self._clean_provided_metadata()

        self.issue_data["n"] = self._notes

    def _find_pages(self) -> None:
        """No-op override: page discovery is not applicable for audio content."""
        pass

    def _find_audios(self) -> None:
        """Locate audio files and instantiate :class:`INABroadcastAudioRecord` objects.

        Resolves the absolute paths for all MP3 and JSON transcript files listed
        in the issue metadata, creates one ``INABroadcastAudioRecord`` per MP3
        file, and validates the total computed duration against the metadata value.
        Missing MP3 files are logged as warnings and appended to ``_notes``.
        """
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
                audio_id, idx + 1, self.json_filepath[idx], mp3_path
            )

            self.audio_records.append(audio_rec)

            # sum the duration in seconds of the records from the length of each audio
            audio_dur = audio_rec._get_duration()
            full_audio_length += audio_dur

        full_formatted_dur = strftime("%H:%M:%S", gmtime(full_audio_length))
        if self.duration is not None and self.duration != full_formatted_dur:
            msg = f"audio {self.id} - The sum of the found durations of records ({full_formatted_dur}) does not match the one from the metadata {self.duration}!"
            print(msg)
            self._notes.append(msg)
            logger.info(msg)

    def _parse_content_item(self) -> None:
        """Build the canonical content item for this issue and store it in ``content_items``.

        Constructs a single content item entry that groups all audio records
        belonging to this issue and records legacy provenance information such
        as the original notice ID and source file paths.
        """
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
        """Filter and return a cleaned copy of the provider-supplied metadata.

        Removes keys that are already represented elsewhere in the canonical
        schema (e.g. file paths, channel, broadcast type) and drops ``None``
        values and empty lists.

        Returns:
            dict[str, Any]: Cleaned metadata dictionary ready for inclusion in
            ``issue_data["provided_metadata"]``.
        """
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
