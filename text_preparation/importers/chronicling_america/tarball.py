"""OCR tarball extraction and integrity checks."""

from __future__ import annotations

import hashlib
import re
import tarfile


def verify_sha1(path: str, expected: str) -> bool:
    digest = hashlib.sha1()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest() == expected.lower()


def extract_alto_members(
    tarball_path: str,
    lccns: set[str],
) -> dict[str, dict[int, bytes]]:
    """Return {issue_key: {seq_num: alto_xml_bytes}} extracted from tarball."""
    extracted: dict[str, dict[int, bytes]] = {}
    with tarfile.open(tarball_path, mode="r:bz2") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            # Expected layout: lccn/YYYY/MM/DD/ed-N/seq-N/ocr.xml
            parts = member.name.split("/")
            if len(parts) != 7:
                continue
            lccn, year, month, day, edition, seq_dir, filename = parts
            if lccn not in lccns:
                continue
            if not filename.endswith(".xml"):
                continue
            seq_match = re.match(r"seq-(\d+)$", seq_dir)
            if not seq_match:
                continue
            issue_key = f"{lccn}/{year}-{month}-{day}/{edition}"
            file_obj = archive.extractfile(member)
            if file_obj is None:
                continue
            content = file_obj.read()
            extracted.setdefault(issue_key, {})[int(seq_match.group(1))] = content
    return extracted
