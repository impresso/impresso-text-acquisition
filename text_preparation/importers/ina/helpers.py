def extract_time_coords_from_elem(elem):
    if elem.name == "SpeechSegment":
        return [
            float(elem.get("stime")),
            float(elem.get("etime")) - float(elem.get("stime")),
        ]
    elif elem.name == "Word":
        return [float(elem.get("stime")), float(elem.get("dur"))]
    else:
        raise NotImplementedError()


def get_utterances(xml_doc) -> list[dict]:
    xml_speech_segs = xml_doc.findAll("SpeechSegment")
    utterances = []

    same_speaker_speech_segs = []
    last_speaker = None
    last_utt_stime = 0
    last_utt_etime = 0
    for xml_ss in xml_speech_segs:

        tokens = [
            {"tc": extract_time_coords_from_elem(word), "tx": word.get_text()}
            for word in xml_ss.findAll("Word")
        ]

        if xml_ss.get("spkid") == last_speaker:
            # case 1, same speaker as last speech segment,
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
                print(f"Saving utterance: {utterances[-1]}")

            # start the new utterance
            last_utt_stime = float(xml_ss.get("stime"))
            last_utt_etime = float(xml_ss.get("etime"))
            last_speaker = xml_ss.get("spkid")
            same_speaker_speech_segs = [
                {"tc": extract_time_coords_from_elem(xml_ss), "t": tokens}
            ]

    return utterances
