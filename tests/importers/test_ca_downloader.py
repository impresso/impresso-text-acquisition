"""Unit tests for the Chronicling America bulk downloader."""

from __future__ import annotations

import io
import json
import os
import tarfile
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from text_preparation.importers.chronicling_america.bulk import (
    DownloadState,
    IssueInfo,
    TitleSpec,
    batch_family,
    batch_version,
    batches_for_lccns,
    build_download_plan,
    dedupe_batch_versions,
    dedupe_issues,
    extract_alto_members,
    issue_in_date_range,
    parse_mets_alto_filenames,
    print_dry_run_report,
    tarball_batch_name,
    verify_sha1,
    write_issue_layout,
)


SAMPLE_METS = b"""<?xml version="1.0" encoding="UTF-8"?>
<mets xmlns="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink">
<fileSec>
<file ID="ocrFile1"><FLocat xlink:href="./0567.xml"/></file>
<file ID="ocrFile2"><FLocat xlink:href="./0568.xml"/></file>
</fileSec>
<structMap>
<div TYPE="page"><fptr FILEID="ocrFile1"/></div>
<div TYPE="page"><fptr FILEID="ocrFile2"/></div>
</structMap>
</mets>
"""


def _make_tarball_bytes(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:bz2") as archive:
        for name, content in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


@pytest.mark.parametrize(
    ("batch", "family", "version"),
    [
        ("dlc_ferguson_ver01", "dlc_ferguson", 1),
        ("dlc_ferguson_ver02", "dlc_ferguson", 2),
        ("ak_albatross_ver01", "ak_albatross", 1),
    ],
)
def test_batch_version_parsing(batch: str, family: str, version: int) -> None:
    assert batch_family(batch) == family
    assert batch_version(batch) == version


def test_dedupe_batch_versions() -> None:
    batches = [
        "dlc_ferguson_ver01",
        "dlc_ferguson_ver02",
        "ak_albatross_ver01",
        "ak_albatross_ver02",
    ]
    assert dedupe_batch_versions(batches) == [
        "ak_albatross_ver02",
        "dlc_ferguson_ver02",
    ]


def test_tarball_batch_name() -> None:
    assert tarball_batch_name("batch_dlc_ferguson_ver01.tar.bz2") == "dlc_ferguson_ver01"
    assert tarball_batch_name("invalid.txt") is None


@pytest.mark.parametrize(
    ("candidate", "is_lccn"),
    [
        ("sn83045462", True),
        ("mn99999999", True),
        ("2010218500", True),
        ("batch_notes", False),
        ("index.html", False),
    ],
)
def test_lccn_regex(candidate: str, is_lccn: bool) -> None:
    from text_preparation.importers.chronicling_america.bulk import LCCN_RE

    assert bool(LCCN_RE.match(candidate)) == is_lccn


def test_parse_directory_links_ignores_parent() -> None:
    from text_preparation.importers.chronicling_america.bulk import _parse_directory_links

    html = """
    <html><body><table>
    <tr><td><a href="../">Parent Directory</a></td></tr>
    <tr><td><a href="00280600854/">00280600854</a></td></tr>
    </table></body></html>
    """
    assert _parse_directory_links(html) == ["00280600854"]


def test_batches_for_lccns() -> None:
    index = {
        "dlc_ferguson_ver01": ["sn83045462", "sn830030214"],
        "dlc_ferguson_ver02": ["sn83045462"],
        "ak_albatross_ver01": ["sn830030214"],
    }
    batches = batches_for_lccns(index, {"sn83045462"})
    assert batches == ["dlc_ferguson_ver02"]


def test_issue_in_date_range() -> None:
    title = TitleSpec(
        lccn="sn83045462",
        alias="eveningstar",
        start_date=date(1930, 1, 1),
        end_date=date(1935, 12, 31),
    )
    assert issue_in_date_range("1932-06-20", title)
    assert not issue_in_date_range("1920-01-01", title)


def test_dedupe_issues_keeps_latest_batch() -> None:
    issues = [
        IssueInfo(
            batch="dlc_old_ver01",
            lccn="sn83045462",
            alias="eveningstar",
            date="1932-06-20",
            edition="ed-1",
            issue_dir_name="1932062001",
            url="http://example/old/",
        ),
        IssueInfo(
            batch="dlc_old_ver02",
            lccn="sn83045462",
            alias="eveningstar",
            date="1932-06-20",
            edition="ed-1",
            issue_dir_name="1932062001",
            url="http://example/new/",
        ),
    ]
    deduped = dedupe_issues(issues)
    assert len(deduped) == 1
    assert deduped[0].batch == "dlc_old_ver02"


def test_parse_mets_alto_filenames() -> None:
    assert parse_mets_alto_filenames(SAMPLE_METS) == ["0567.xml", "0568.xml"]


def test_extract_alto_members(tmp_path) -> None:
    members = {
        "sn83045462/1932/06/20/ed-1/seq-1/ocr.xml": b"<alto seq='1'/>",
        "sn83045462/1932/06/20/ed-1/seq-2/ocr.xml": b"<alto seq='2'/>",
        "sn99999999/1932/06/20/ed-1/seq-1/ocr.xml": b"<alto other='1'/>",
    }
    tarball_path = tmp_path / "sample.tar.bz2"
    tarball_path.write_bytes(_make_tarball_bytes(members))

    extracted = extract_alto_members(str(tarball_path), {"sn83045462"})
    assert extracted["sn83045462/1932-06-20/ed-1"][1] == b"<alto seq='1'/>"
    assert extracted["sn83045462/1932-06-20/ed-1"][2] == b"<alto seq='2'/>"


def test_extract_alto_members_skips_unexpected_paths(tmp_path) -> None:
    """Paths that are shallower or deeper than the expected layout must not crash."""
    members = {
        "sn83045462/1932/06/20/ed-1/seq-1/ocr.xml": b"<alto seq='1'/>",
        "sn83045462/1932/06/20/ed-1/notes.txt": b"shallow",
        "sn83045462/1932/06/20/ed-1/seq-1/extra/ocr.xml": b"too deep",
        "readme.txt": b"top-level",
    }
    tarball_path = tmp_path / "sample.tar.bz2"
    tarball_path.write_bytes(_make_tarball_bytes(members))

    extracted = extract_alto_members(str(tarball_path), {"sn83045462"})
    assert extracted == {"sn83045462/1932-06-20/ed-1": {1: b"<alto seq='1'/>"}}


def test_write_issue_layout_renames_seq_to_mets_hrefs(tmp_path) -> None:
    issue = IssueInfo(
        batch="dlc_test_ver01",
        lccn="sn83045462",
        alias="eveningstar",
        date="1932-06-20",
        edition="ed-1",
        issue_dir_name="1932062001",
        url="http://example/issue/",
    )
    alto_by_seq = {
        1: b"<alto id='1'/>",
        2: b"<alto id='2'/>",
    }
    write_issue_layout(str(tmp_path), issue, SAMPLE_METS, alto_by_seq)

    issue_dir = tmp_path / "eveningstar" / "1932" / "06" / "20" / "ed-1"
    assert (issue_dir / "1932062001.xml").exists()
    assert (issue_dir / "alto" / "0567.xml").read_bytes() == b"<alto id='1'/>"
    assert (issue_dir / "alto" / "0568.xml").read_bytes() == b"<alto id='2'/>"


def test_download_state_roundtrip(tmp_path) -> None:
    state_path = tmp_path / "state.json"
    state = DownloadState(
        completed_tarballs={"abc": "dlc_test_ver01"},
        completed_issues={"eveningstar/1932-06-20/ed-1"},
    )
    state.save(str(state_path))

    loaded = DownloadState.load(str(state_path))
    assert loaded.completed_tarballs == {"abc": "dlc_test_ver01"}
    assert loaded.completed_issues == {"eveningstar/1932-06-20/ed-1"}


def test_verify_sha1(tmp_path) -> None:
    payload = b"hello chronicling america"
    file_path = tmp_path / "sample.bin"
    file_path.write_bytes(payload)

    import hashlib

    digest = hashlib.sha1(payload).hexdigest()
    assert verify_sha1(str(file_path), digest)
    assert not verify_sha1(str(file_path), "0" * 40)


def test_build_download_plan_uses_cached_index(tmp_path) -> None:
    index_path = tmp_path / "batch_index.json"
    index_path.write_text(
        json.dumps({"dlc_test_ver01": ["sn83045462"]}),
        encoding="utf-8",
    )
    client = MagicMock()
    client.request.return_value.json.return_value = {
        "ocr": [
            {
                "name": "batch_dlc_test_ver01.tar.bz2",
                "url": "https://example/batch_dlc_test_ver01.tar.bz2",
                "sha1": "abc",
                "size": 100,
            }
        ]
    }

    with patch(
        "text_preparation.importers.chronicling_america.bulk.list_issues_in_batch",
        return_value=[
            IssueInfo(
                batch="dlc_test_ver01",
                lccn="sn83045462",
                alias="eveningstar",
                date="1932-06-20",
                edition="ed-1",
                issue_dir_name="1932062001",
                url="http://example/issue/",
            )
        ],
    ):
        plan = build_download_plan(
            client,
            [TitleSpec(lccn="sn83045462", alias="eveningstar")],
            str(index_path),
        )

    assert plan.batches == ["dlc_test_ver01"]
    assert len(plan.tarballs) == 1
    assert len(plan.issues) == 1
    assert plan.total_tarball_bytes == 100


def test_print_dry_run_report(capsys) -> None:
    from text_preparation.importers.chronicling_america.bulk import DownloadPlan, TarballInfo

    plan = DownloadPlan(
        titles=[TitleSpec(lccn="sn83045462", alias="eveningstar")],
        batches=["dlc_test_ver01"],
        tarballs=[
            TarballInfo(
                batch="dlc_test_ver01",
                url="https://example/batch.tar.bz2",
                sha1="abc",
                size=1024 * 1024,
            )
        ],
        issues=[],
        total_tarball_bytes=1024 * 1024,
    )
    print_dry_run_report(plan)
    output = capsys.readouterr().out
    assert "eveningstar" in output
    assert "Tarballs: 1" in output
