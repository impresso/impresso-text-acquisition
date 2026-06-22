"""Local filesystem layout for METS/ALTO issue directories."""

from __future__ import annotations

import os

from bs4 import BeautifulSoup

from text_preparation.importers.chronicling_america.issues import issue_local_dir
from text_preparation.importers.chronicling_america.models import IssueInfo


def parse_mets_alto_filenames(mets_xml: bytes) -> list[str]:
    soup = BeautifulSoup(mets_xml, "xml")
    file_map: dict[str, str] = {}
    file_sec = soup.find("fileSec")
    if file_sec:
        for file_tag in file_sec.find_all("file"):
            file_id = file_tag.get("ID")
            flocat = file_tag.find("FLocat")
            if file_id and flocat and flocat.get("xlink:href"):
                file_map[file_id] = os.path.basename(flocat["xlink:href"])

    ordered: list[str] = []
    struct_map = soup.find("structMap")
    if struct_map:
        page_divs = struct_map.find_all(
            "div",
            {"TYPE": lambda value: value and "page" in value.lower()},
        )
        for div in page_divs:
            fptr = div.find("fptr", {"FILEID": lambda value: value and value.startswith("ocrFile")})
            if not fptr:
                continue
            filename = file_map.get(fptr.get("FILEID", ""))
            if filename:
                ordered.append(filename)
    return ordered


def write_issue_layout(
    output_dir: str,
    issue: IssueInfo,
    mets_bytes: bytes,
    alto_by_seq: dict[int, bytes],
) -> None:
    issue_dir = issue_local_dir(output_dir, issue)
    alto_dir = os.path.join(issue_dir, "alto")
    os.makedirs(alto_dir, exist_ok=True)

    mets_path = os.path.join(issue_dir, f"{issue.issue_dir_name}.xml")
    with open(mets_path, "wb") as handle:
        handle.write(mets_bytes)

    href_names = parse_mets_alto_filenames(mets_bytes)
    if href_names:
        for seq_num, alto_bytes in sorted(alto_by_seq.items()):
            if seq_num <= len(href_names):
                filename = href_names[seq_num - 1]
            else:
                filename = f"seq-{seq_num}.xml"
            with open(os.path.join(alto_dir, filename), "wb") as handle:
                handle.write(alto_bytes)
    else:
        for seq_num, alto_bytes in sorted(alto_by_seq.items()):
            with open(os.path.join(alto_dir, f"seq-{seq_num}.xml"), "wb") as handle:
                handle.write(alto_bytes)
