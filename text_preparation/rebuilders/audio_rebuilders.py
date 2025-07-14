"""Rebuild helpers for audio data."""

import logging
from typing import Any

from impresso_essentials.text_utils import insert_whitespace

logger = logging.getLogger(__name__)


def rebuild_audio_text(
    audio: list[dict], language: str | None, string: str | None = None
) -> tuple[str, dict[list], dict[list]]:
    """Rebuild the text of an article for Solr ingestion.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, dict[list], dict[list]]: [0] CI fulltext, [1] offsets and
            [2] coordinates of token regions.
    """
    coordinates = {"sections": [], "utterances": [], "tokens": []}

    offsets = {"speech_seg": [], "utterance": [], "section": []}

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)
    for sec in audio:

        if len(string) > 0:
            offsets["section"].append(len(string))

        coordinates["sections"].append(sec["tc"])

        for utterance in sec["u"]:

            if len(string) > 0:
                offsets["utterance"].append(len(string))

            for speech_seg in utterance["ss"]:

                for n, token in enumerate(speech_seg["t"]):
                    section = {}
                    if "tc" not in token:
                        print(f"'tc' was not present in token: {token}, speech_seg: {speech_seg}")
                        continue
                    section["tc"] = token["tc"]
                    section["s"] = len(string)

                    if token["tx"]:
                        section["l"] = len(token["tx"])
                        token_text = token["tx"]
                    else:
                        section["l"] = 0
                        token_text = ""

                    # don't add the tokens corresponding to the first part of a hyphenated word
                    if "hy" not in token:
                        next_token = (
                            speech_seg["t"][n + 1]["tx"] if n != len(speech_seg["t"]) - 1 else None
                        )
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=next_token,
                            prev_t=speech_seg["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        string += f"{token_text} " if ws else f"{token_text}"

                    # if token is the last in a speech segment
                    if n == len(speech_seg["t"]) - 1:
                        offsets["speech_seg"].append(section["s"] + section["l"])

                    coordinates["tokens"].append(section)

    return (string, coordinates, offsets)


def rebuild_audio_text_passim(
    page: list[dict], language: str | None, string: str | None = None
) -> tuple[str, list[dict]]:
    """The text rebuilding function from pages for passim.

    If `string` is not `None`, then the rebuilt text is appended to it.

    Args:
        page (list[dict]): Newspaper page conforming to the impresso JSON pages schema.
        language (str | None): Language of the article being rebuilt
        string (str | None, optional): Rebuilt text of previous page. Defaults to None.

    Returns:
        tuple[str, list[dict]]: [0] article fulltext, [1] coordinates of token regions.
    """
    # TODO adapt to radio data!!!
    regions = []

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)

    for region in page:

        for para in region["p"]:

            for line in para["l"]:

                for n, token in enumerate(line["t"]):

                    region_string = ""

                    if "c" not in token:
                        # if the coordniates are missing, they should be skipped
                        logger.debug("Missing 'c' in token %s", token)
                        print(f"Missing 'c' in token {token}")
                        continue

                    # each page region is a token
                    output_region = {
                        "start": None,
                        "length": None,
                        "coords": {
                            "x": token["c"][0],
                            "y": token["c"][1],
                            "w": token["c"][2],
                            "h": token["c"][3],
                        },
                    }

                    if len(string) == 0:
                        output_region["start"] = 0
                    else:
                        output_region["start"] = len(string)

                    # if token is the last in a line
                    if n == len(line["t"]) - 1:
                        tmp = f"{token['tx']}\n"
                        region_string += tmp
                    else:
                        ws = insert_whitespace(
                            token["tx"],
                            next_t=line["t"][n + 1]["tx"],
                            prev_t=line["t"][n - 1]["tx"] if n != 0 else None,
                            lang=language,
                        )
                        region_string += f"{token['tx']} " if ws else f"{token['tx']}"

                    string += region_string
                    output_region["length"] = len(region_string)
                    regions.append(output_region)

    return (string, regions)


def recompose_ci_from_audio_solr(
    solr_ci: dict[str, Any], content_item: dict[str, Any]
) -> dict[str, Any]:
    """Given a partly constructed solr rebuilt CI, reconstruct the audio elements.

    The parts added are the components of the `rebuilt record`, `rreb` composed of sections,
    utterances, speechsegments and tokens.
    Then the fulltext and the offsets of the breaks separating each element are also added
    to the Solr representation.

    Args:
        solr_ci (dict[str, Any]): Solr/rebuilt representation of the CI to complete.
        content_item (dict[str, Any]): Temporary version of the canonical CI used to reconstruct.

    Returns:
        dict[str, Any]: Rebuilt CI with the recomposed audio elements added to it.
    """
    issue_id = "-".join(solr_ci["id"].split("-")[:-1])
    audio_file_names = {r: f"{issue_id}-r{str(r).zfill(4)}.json" for r in content_item["m"]["rr"]}

    fulltext = ""
    speechsegsbreaks = []
    utterancebreaks = []
    sectionbreaks = []
    solr_ci["rreb"] = []

    # there should be only one record, but keeping the formatting
    for n, audio_rec_no in enumerate(solr_ci["rr"]):

        audio = content_item["rrss"][n]

        if fulltext == "":
            fulltext, coords, offsets = rebuild_audio_text(audio, solr_ci["lg"])
        else:
            fulltext, coords, offsets = rebuild_audio_text(audio, solr_ci["lg"], fulltext)

        speechsegsbreaks += offsets["speech_seg"]
        utterancebreaks += offsets["utterance"]
        sectionbreaks += offsets["section"]

        audio_doc = {
            "id": audio_file_names[audio_rec_no].replace(".json", ""),
            "n": audio_rec_no,
            "t": coords["tokens"],
            "u": coords["utterances"],
            "s": coords["sections"],
        }
        solr_ci["rreb"].append(audio_doc)
    solr_ci["ssb"] = speechsegsbreaks
    solr_ci["ub"] = utterancebreaks
    solr_ci["sb"] = sectionbreaks
    solr_ci["ft"] = fulltext

    return solr_ci


def recompose_ci_from_audio_passim(
    content_item: dict[str, Any], passim_document: dict[str, Any]
) -> dict[str, Any]:
    """_summary_
    TODO documentation and ensuring the code works for our needs.

    Args:
        content_item (dict[str, Any]): _description_
        passim_document (dict[str, Any]): _description_

    Returns:
        dict[str, Any]: _description_
    """
    issue_id = "-".join(passim_document["id"].split("-")[:-1])

    audio_file_names = {r: f"{issue_id}-r{str(r).zfill(4)}.json" for r in content_item["m"]["rr"]}

    fulltext = ""
    # TODO, ensure that ["rr"] is present in i['m']
    for n, audio_no in enumerate(content_item["m"]["rr"]):

        audio = content_item["pprr"][n]

        if fulltext == "":
            fulltext, sections = rebuild_audio_text_passim(audio, passim_document["lg"])
        else:
            fulltext, sections = rebuild_audio_text_passim(audio, passim_document["lg"], fulltext)

        page_doc = {
            "id": audio_file_names[audio_no].replace(".json", ""),
            "seq": audio_no,
            "sections": sections,
        }
        passim_document["audios"].append(page_doc)

    passim_document["text"] = fulltext

    return passim_document


def reconstruct_audios(
    issue_json: dict[str, Any], ci: dict[str, Any], cis: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Reconstruct the audios of a given issue to prepare for the rebuilt CI composition.

    Args:
        issue_json (dict[str, Any]): Issue for which to rebuild the audio sections.
        ci (dict[str, Any]): Current CI for which to rebuild the sections.
        cis (list[dict[str, Any]]): Current list of previously processed CIs from this issue.

    Returns:
        list[dict[str, Any]]: List of processed CIs with this new CI added to it.
    """
    audios = []
    audio_ids = [audio["id"] for audio in issue_json["rr"]]
    # there should only be one record, but keeping the same approach
    for audio_no in ci["m"]["rr"]:
        # given a page  number (from issue.json) and its canonical ID
        # find the position of that page in the array of pages (with text sections)
        audio_no_string = f"r{str(audio_no).zfill(4)}"
        try:
            audio_idx = [
                n for n, audio in enumerate(issue_json["rr"]) if audio_no_string in audio["id"]
            ][0]
            audios.append(issue_json["rr"][audio_idx])
        except IndexError:
            ci["has_problem"] = True
            cis.append(ci)
            logger.error(
                "Audio %s not found for item %s. Issue %s has audios %s",
                audio_no_string,
                ci["m"]["id"],
                issue_json["id"],
                audio_ids,
            )
            continue

    sections_by_audio = []
    for audio in audios:
        sections_by_audio.append(
            [
                section
                for section in audio["s"]
                if "pOf" in section and section["pOf"] == ci["m"]["id"]
            ]
        )
    ci["rrss"] = sections_by_audio
    cis.append(ci)

    return cis
