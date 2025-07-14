"""Helper functions used by the INA Importer."""

from bs4 import BeautifulSoup
from bs4.element import Tag


def extract_time_coords_from_elem(elem: Tag) -> list[float] | None:
    """Extract the time coordinates from a given speech element.

    Args:
        elem (Tag): Element from the beautifulsoup object extracted from the ASR.

    Raises:
        NotImplementedError: The element did not have one of the expected names.

    Returns:
        list[float] | None: The time coordinates for the given ASR element.
    """
    if elem.name == "SpeechSegment":
        return [
            float(elem.get("stime")),
            float(elem.get("etime")) - float(elem.get("stime")),
        ]
    if elem.name == "Word":
        return [float(elem.get("stime")), float(elem.get("dur"))]

    raise NotImplementedError()


def get_utterances(xml_doc: BeautifulSoup) -> list[dict]:
    """Construct the utterances composed of speech segments for a given record.

    An utterance is a list of consecutive speechsegments with the same speaker ID.

    Args:
        xml_doc (BeautifulSoup): Contents of the ASR xml document of the record.

    Returns:
        list[dict]: List of utterances, composed of speechsegments for the record.
    """
    xml_speech_segs = xml_doc.findAll("SpeechSegment")
    utterances = []

    same_speaker_speech_segs = []
    last_speaker = None
    last_utt_stime = 0
    last_utt_etime = 0
    for idx, xml_ss in enumerate(xml_speech_segs):

        tokens = [
            {"tc": extract_time_coords_from_elem(word), "tx": word.get_text()}
            for word in xml_ss.findAll("Word")
        ]

        if xml_ss.get("spkid") == last_speaker:
            # case 1, same speaker as last speech segment
            same_speaker_speech_segs.append(
                {"tc": extract_time_coords_from_elem(xml_ss), "t": tokens}
            )
            # update the last end time for the current utterance
            last_utt_etime = float(xml_ss.get("etime"))
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
            last_utt_stime = float(xml_ss.get("stime"))
            last_utt_etime = float(xml_ss.get("etime"))
            last_speaker = xml_ss.get("spkid")
            same_speaker_speech_segs = [{"tc": extract_time_coords_from_elem(xml_ss), "t": tokens}]

        if idx == len(xml_speech_segs) - 1:
            # if it's the last speech segment, save the current utterance
            utterances.append(
                {
                    "tc": [last_utt_stime, last_utt_etime - last_utt_stime],
                    "speaker": last_speaker,
                    "ss": same_speaker_speech_segs,
                }
            )

    return utterances
