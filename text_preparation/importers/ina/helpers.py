"""Helper functions used by the INA Importer."""

from bs4 import BeautifulSoup
from bs4.element import Tag


def extract_time_coords_from_elem(elem: dict, is_sseg: bool = False) -> list[float] | None:
    """Extract the time coordinates ``[start, duration]`` from a speech element.

    Args:
        elem (dict): A word-level or speech-segment-level dict from the ASR JSON
            document, containing ``"start"`` / ``"end"`` timestamps and, for
            speech segments, a ``"words"`` list.
        is_sseg (bool): If ``True``, treat ``elem`` as a speech segment and
            derive coordinates from its first and last word. If ``False``
            (default), treat ``elem`` as a single word.

    Returns:
        list[float] | None: A two-element list ``[start_time, duration]`` in
        seconds, rounded to five decimal places for the duration.
    """
    if is_sseg:
        return [
            elem["words"][0]["start"],  # start time of the first word
            # duration = end time of last word - start time of first word
            round(elem["words"][-1]["end"] - elem["words"][0]["start"], 5),
        ]

    return [elem["start"], round(elem["end"] - elem["start"], 5)]


def get_utterances(json_doc: list[dict]) -> list[dict]:
    """Construct utterances from consecutive speech segments sharing the same speaker.

    Iterates over the speech segments in the ASR JSON document and groups
    consecutive segments belonging to the same speaker into a single utterance.
    Each utterance contains a time-code span, the speaker identifier, and the
    constituent speech segments with their token-level time codes.

    Args:
        json_doc (list[dict]): Parsed ASR JSON document for one audio record.
            Each element is a speech-segment dict with at least ``"speaker"``
            and ``"words"`` keys.

    Returns:
        list[dict]: List of utterance dicts, each containing:

            - ``"tc"`` (list[float]): ``[start_time, duration]`` of the utterance.
            - ``"speaker"`` (str): Speaker identifier.
            - ``"ss"`` (list[dict]): Speech segments belonging to this utterance.
    """
    utterances = []

    same_speaker_speech_segs = []
    last_speaker = None
    last_utt_stime = 0
    last_utt_etime = 0

    # the json file has already extracted speech segments
    for idx, json_ss in enumerate(json_doc):

        tokens = [
            # each words start with a space, we want to remove them immediately
            {"tc": extract_time_coords_from_elem(word), "tx": word["word"].strip()}
            for word in json_ss["words"]
        ]

        if json_ss.get("speaker") == last_speaker:
            # case 1, same speaker as last speech segment
            same_speaker_speech_segs.append(
                {"tc": extract_time_coords_from_elem(json_ss, is_sseg=True), "t": tokens}
            )
            # update the last end time for the current utterance
            last_utt_etime = json_ss["words"][-1]["end"]
        else:
            # case 2: new speaker, save the last utterance if possible and start a new one
            if last_speaker is not None:
                utterances.append(
                    {
                        "tc": [last_utt_stime, last_utt_etime - last_utt_stime],
                        "speaker": last_speaker,
                        "ss": same_speaker_speech_segs,
                    }
                )

            # start the new utterance
            last_utt_stime = json_ss["words"][0]["start"]
            last_utt_etime = json_ss["words"][-1]["end"]
            last_speaker = json_ss["speaker"]
            same_speaker_speech_segs = [
                {"tc": extract_time_coords_from_elem(json_ss, is_sseg=True), "t": tokens}
            ]

        if idx == len(json_doc) - 1:
            # if it's the last speech segment, save the current utterance
            utterances.append(
                {
                    "tc": [last_utt_stime, last_utt_etime - last_utt_stime],
                    "speaker": last_speaker,
                    "ss": same_speaker_speech_segs,
                }
            )

    return utterances
