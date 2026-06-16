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
    TarballInfo,
    batch_family,
    batch_version,
    batches_for_lccns,
    build_download_plan,
    build_or_load_batch_index,
    dedupe_batch_versions,
    dedupe_issues,
    download_file,
    extract_alto_members,
    index_from_tarball_manifest,
    issue_in_date_range,
    parse_ocr_manifest,
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
    assert tarball_batch_name("dlc_ferguson_ver01.tar.bz2") == "dlc_ferguson_ver01"
    assert tarball_batch_name("invalid.txt") is None


def test_parse_ocr_manifest_legacy_shape() -> None:
    tarballs = parse_ocr_manifest(
        {
            "ocr": [
                {
                    "name": "batch_dlc_test_ver01.tar.bz2",
                    "url": "https://example/batch_dlc_test_ver01.tar.bz2",
                    "sha1": "abc",
                    "size": 100,
                }
            ]
        }
    )
    assert len(tarballs) == 1
    assert tarballs[0].batch == "dlc_test_ver01"


def test_parse_ocr_manifest_migration_shape() -> None:
    tarballs = parse_ocr_manifest(
        [
            {
                "archive_name": "ak_albatross_ver01.tar.bz2",
                "batch": "ak_albatross_ver01",
                "url": "https://chroniclingamerica.loc.gov/data/ocr/ak_albatross_ver01.tar.bz2",
                "sha1": "abc",
                "size": 768843638,
            }
        ]
    )
    assert len(tarballs) == 1
    assert tarballs[0].batch == "ak_albatross_ver01"


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


def test_index_from_tarball_manifest() -> None:
    tarballs = [
        TarballInfo(
            batch="dlc_test_ver01",
            url="https://example/batch.tar.bz2",
            sha1="abc",
            size=100,
            lccns=("sn83045462", "sn99999999"),
        )
    ]
    assert index_from_tarball_manifest(tarballs) == {
        "dlc_test_ver01": ["sn83045462", "sn99999999"],
    }


def test_build_or_load_batch_index_uses_manifest_without_crawl(tmp_path) -> None:
    index_path = tmp_path / "batch_index.json"
    client = MagicMock()

    index = build_or_load_batch_index(
        client,
        str(index_path),
        batches=["dlc_test_ver01", "ak_other_ver01"],
        manifest_index={
            "dlc_test_ver01": ["sn83045462"],
            "ak_other_ver01": ["sn99999999"],
        },
    )

    assert index == {
        "dlc_test_ver01": ["sn83045462"],
        "ak_other_ver01": ["sn99999999"],
    }
    client.request.assert_not_called()
    assert index_path.exists()


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
    ) as mock_crawl:
        plan = build_download_plan(
            client,
            [TitleSpec(lccn="sn83045462", alias="eveningstar")],
            str(index_path),
        )

    mock_crawl.assert_not_called()
    assert plan.batches == ["dlc_test_ver01"]
    assert len(plan.tarballs) == 1
    assert plan.issues == []
    assert plan.total_tarball_bytes == 100


def test_build_download_plan_dry_run_estimates_from_manifest(tmp_path) -> None:
    """Dry-run must estimate issues from manifest issue_count, never crawl reels."""
    index_path = tmp_path / "batch_index.json"
    index_path.write_text(
        json.dumps({"dlc_test_ver01": ["sn83045462"]}),
        encoding="utf-8",
    )
    client = MagicMock()
    client.request.return_value.json.return_value = [
        {
            "archive_name": "dlc_test_ver01.tar.bz2",
            "batch": "dlc_test_ver01",
            "url": "https://chroniclingamerica.loc.gov/data/ocr/dlc_test_ver01.tar.bz2",
            "sha1": "abc",
            "size": 100,
            "lccns": ["sn83045462"],
            "issue_count": 312,
        }
    ]

    with patch(
        "text_preparation.importers.chronicling_america.bulk.list_issues_in_batch",
    ) as mock_crawl:
        plan = build_download_plan(
            client,
            [TitleSpec(lccn="sn83045462", alias="eveningstar")],
            str(index_path),
            dry_run=True,
        )

    mock_crawl.assert_not_called()
    assert plan.estimated_issues == 312
    assert plan.issues == []
    assert plan.batches == ["dlc_test_ver01"]


def test_parse_ocr_manifest_captures_issue_count() -> None:
    tarballs = parse_ocr_manifest(
        [
            {
                "archive_name": "dlc_test_ver01.tar.bz2",
                "batch": "dlc_test_ver01",
                "url": "https://example/dlc_test_ver01.tar.bz2",
                "sha1": "abc",
                "size": 100,
                "lccns": ["sn83045462"],
                "issue_count": 312,
            }
        ]
    )
    assert tarballs[0].issue_count == 312
    assert tarballs[0].lccns == ("sn83045462",)


def test_print_dry_run_report_shows_estimate(capsys) -> None:
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
                issue_count=312,
            )
        ],
        issues=[],
        total_tarball_bytes=1024 * 1024,
        estimated_issues=312,
    )
    print_dry_run_report(plan)
    output = capsys.readouterr().out
    assert "~312" in output


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
    assert "ALTO (OCR tarballs): 1" in output
    assert "METS (per-issue crawl)" in output


def test_http_client_retries_transient_5xx() -> None:
    """Cloudflare-style 525 errors from tile.loc.gov must be retried, not returned."""
    from text_preparation.importers.chronicling_america.bulk import HttpClient

    bad_response = MagicMock(status_code=525)
    good_response = MagicMock(status_code=200)

    session = MagicMock()
    session.get.side_effect = [bad_response, good_response]

    client = HttpClient(delay=0, session=session)
    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        response = client.request("http://example/0017.xml")

    assert response is good_response
    assert session.get.call_count == 2


def test_http_client_raises_after_persistent_5xx() -> None:
    from text_preparation.importers.chronicling_america.bulk import HttpClient

    session = MagicMock()
    session.get.return_value = MagicMock(status_code=525)

    client = HttpClient(delay=0, max_retries=3, session=session)
    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        with pytest.raises(RuntimeError, match="Failed to fetch"):
            client.request("http://example/0017.xml")

    assert session.get.call_count == 3


def test_http_client_does_not_retry_404() -> None:
    """404s are meaningful to callers (batch/LCCN probing) and must pass through."""
    from text_preparation.importers.chronicling_america.bulk import HttpClient

    session = MagicMock()
    session.get.return_value = MagicMock(status_code=404)

    client = HttpClient(delay=0, session=session)
    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        response = client.request("http://example/missing/")

    assert response.status_code == 404
    assert session.get.call_count == 1


def test_http_client_enforces_minimum_delay_between_requests() -> None:
    from text_preparation.importers.chronicling_america.bulk import HttpClient

    session = MagicMock()
    session.get.return_value = MagicMock(status_code=200)
    client = HttpClient(delay=1.0, session=session)

    sleeps: list[float] = []
    with patch(
        "text_preparation.importers.chronicling_america.bulk.time.monotonic",
        side_effect=[0.0, 0.0, 0.2, 0.2],
    ):
        with patch(
            "text_preparation.importers.chronicling_america.bulk.time.sleep",
            side_effect=lambda seconds: sleeps.append(seconds),
        ):
            client.request("http://example/a/")
            client.request("http://example/b/")

    assert sleeps[0] == pytest.approx(1.0)
    assert sleeps[1] == pytest.approx(0.8)


def test_http_client_429_waits_one_hour() -> None:
    from text_preparation.importers.chronicling_america.bulk import (
        HttpClient,
        LOC_RATE_LIMIT_BLOCK_SECONDS,
    )

    bad_response = MagicMock(status_code=429, headers={})
    good_response = MagicMock(status_code=200)
    session = MagicMock()
    session.get.side_effect = [bad_response, good_response]

    client = HttpClient(delay=0, session=session)
    sleeps: list[float] = []
    with patch(
        "text_preparation.importers.chronicling_america.bulk.time.sleep",
        side_effect=lambda seconds: sleeps.append(seconds),
    ):
        response = client.request("http://example/")

    assert response is good_response
    assert sleeps[0] >= LOC_RATE_LIMIT_BLOCK_SECONDS


def test_is_challenge_response_detects_cloudflare_html() -> None:
    from text_preparation.importers.chronicling_america.bulk import is_challenge_response

    response = MagicMock(
        status_code=403,
        headers={"Content-Type": "text/html; charset=UTF-8"},
    )
    response.text = "<html><title>Just a moment...</title></html>"
    assert is_challenge_response(response)

    ok_response = MagicMock(status_code=403, headers={"Content-Type": "application/json"})
    ok_response.text = '{"error": "forbidden"}'
    assert not is_challenge_response(ok_response)


def test_http_client_retries_captcha_403() -> None:
    from text_preparation.importers.chronicling_america.bulk import HttpClient

    bad_response = MagicMock(
        status_code=403,
        headers={"Content-Type": "text/html"},
    )
    bad_response.text = "<html>Just a moment...</html>"
    good_response = MagicMock(status_code=200)
    session = MagicMock()
    session.get.side_effect = [bad_response, good_response]

    client = HttpClient(delay=0, session=session)
    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        response = client.request("http://example/item")

    assert response is good_response
    assert session.get.call_count == 2


def test_issue_dir_name_from_date_edition() -> None:
    from text_preparation.importers.chronicling_america.bulk import (
        issue_dir_name_from_date_edition,
        issue_info_from_tarball_key,
    )

    assert issue_dir_name_from_date_edition("1932-06-20", "ed-1") == "1932062001"
    issue = issue_info_from_tarball_key(
        "dlc_test_ver01",
        "sn83045462/1932-06-20/ed-1",
        {"sn83045462": "eveningstar"},
    )
    assert issue.issue_dir_name == "1932062001"
    assert issue.alias == "eveningstar"


def test_enumerate_issue_urls_uses_cache_and_stops_early(tmp_path) -> None:
    from text_preparation.importers.chronicling_america.bulk import (
        enumerate_issue_urls,
        issue_url_cache_path,
    )

    cache_path = issue_url_cache_path(str(tmp_path), "vi_test_ver01", "sn84024738")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as handle:
        json.dump({"1932062001": "http://example/reel/1932062001/"}, handle)

    client = MagicMock()
    title = TitleSpec(lccn="sn84024738", alias="dailydispatch")
    mapping = enumerate_issue_urls(
        client,
        "vi_test_ver01",
        title,
        cache_path,
        needed={"1932062001"},
    )

    client.request.assert_not_called()
    assert mapping["1932062001"].startswith("http://example/")


def test_download_file_retries_on_chunked_error(tmp_path) -> None:
    import requests

    dest = tmp_path / "out.xml"

    good_response = MagicMock()
    good_response.raise_for_status.return_value = None
    good_response.iter_content.return_value = iter([b"<alto/>"])

    bad_response = MagicMock()
    bad_response.raise_for_status.return_value = None
    bad_response.iter_content.side_effect = requests.exceptions.ChunkedEncodingError(
        "Response ended prematurely"
    )

    client = MagicMock()
    client.request.side_effect = [bad_response, good_response]

    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        download_file(client, "http://example/ocr.xml", str(dest), max_retries=3)

    assert dest.read_bytes() == b"<alto/>"
    assert client.request.call_count == 2
    # No leftover partial file
    assert not (tmp_path / "out.xml.part").exists()


def test_download_file_raises_after_max_retries(tmp_path) -> None:
    import requests

    dest = tmp_path / "out.xml"
    bad_response = MagicMock()
    bad_response.raise_for_status.return_value = None
    bad_response.iter_content.side_effect = requests.exceptions.ConnectionError("boom")

    client = MagicMock()
    client.request.return_value = bad_response

    with patch("text_preparation.importers.chronicling_america.bulk.time.sleep"):
        with pytest.raises(RuntimeError, match="Failed to download"):
            download_file(client, "http://example/ocr.xml", str(dest), max_retries=2)

    assert not dest.exists()
    assert not (tmp_path / "out.xml.part").exists()
