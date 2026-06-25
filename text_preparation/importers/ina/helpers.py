"""Helper functions used by the INA Importer."""

from bs4 import BeautifulSoup
from bs4.element import Tag


def extract_time_coords_from_elem(elem: dict, is_sseg: bool = False) -> list[float] | None:
    """Extract the time coordinates (start, duration) from a given speech element.

    Args:
        elem (Tag): Element from the beautifulsoup object extracted from the ASR.

    Raises:
        NotImplementedError: The element did not have one of the expected names.

    Returns:
        list[float] | None: The time coordinates for the given ASR element.
    """
    if is_sseg:
        return [
            elem["words"][0]["start"],  # start time of the first word
            # duration = end time of last word - start time of first word
            elem["words"][-1]["end"] - elem["words"][0]["start"],
        ]

    return [elem["start"], elem["end"] - elem["start"]]


def get_utterances(json_doc: dict) -> list[dict]:
    """Construct the utterances composed of speech segments for a given record.

    An utterance is a list of consecutive speechsegments with the same speaker ID.

    Args:
        xml_doc (BeautifulSoup): Contents of the ASR xml document of the record.

    Returns:
        list[dict]: List of utterances, composed of speechsegments for the record.
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
            {"tc": extract_time_coords_from_elem(word), "tx": word["word"].split()}
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
