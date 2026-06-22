"""Unit tests for the Chronicling America loc.gov API downloader."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from text_preparation.importers.chronicling_america.api import (
    ApiIssueFiles,
    build_collection_search_url,
    build_item_url,
    discover_issue_item_urls,
    fetch_issue_files,
    issue_dir_name_from_parts,
    parse_item_url,
    run_api_download,
)
from text_preparation.importers.chronicling_america.bulk import TitleSpec


SAMPLE_ITEM_JSON = {
    "item": {
        "item": {
            "batch": ["dlc_1arp_ver01"],
            "other_formats": [
                "https://tile.loc.gov/storage-services/service/ndnp//dlc/batch_dlc_1arp_ver01/data/sn83045462/00280601081/1932062001/1932062001_1.xml",
            ],
        }
    },
    "resources": [
        {
            "files": [
                [
                    {
                        "mimetype": "text/xml",
                        "url": "https://tile.loc.gov/storage-services/service/ndnp/dlc/batch_dlc_1arp_ver01/data/sn83045462/00280601081/1932062001/0567.xml",
                    },
                    {
                        "mimetype": "application/pdf",
                        "url": "https://tile.loc.gov/storage-services/service/ndnp/dlc/batch_dlc_1arp_ver01/data/sn83045462/00280601081/1932062001/0567.pdf",
                    },
                ],
                [
                    {
                        "mimetype": "text/xml",
                        "url": "https://tile.loc.gov/storage-services/service/ndnp/dlc/batch_dlc_1arp_ver01/data/sn83045462/00280601081/1932062001/0568.xml",
                    }
                ],
            ]
        }
    ],
}


def test_build_item_url() -> None:
    assert (
        build_item_url("sn83045462", "1932-06-20")
        == "https://www.loc.gov/item/sn83045462/1932-06-20/ed-1/"
    )


def test_issue_dir_name_from_parts() -> None:
    assert issue_dir_name_from_parts("1932-06-20", "ed-1") == "1932062001"


def test_parse_item_url() -> None:
    parsed = parse_item_url("http://www.loc.gov/item/sn83045462/1932-06-20/ed-1/")
    assert parsed == ("sn83045462", "1932-06-20", "ed-1")


def test_build_collection_search_url() -> None:
    from datetime import date

    url = build_collection_search_url(
        "sn83045462",
        start_date=date(1932, 6, 20),
        end_date=date(1932, 6, 20),
    )
    assert "fa=number_lccn:sn83045462" in url
    assert "dl=issue" in url
    assert "start_date=1932-06-20" in url


def test_discover_issue_item_urls_with_date() -> None:
    title = TitleSpec(lccn="sn83045462", alias="eveningstar")
    client = MagicMock()
    urls = discover_issue_item_urls(
        client,
        title,
        issue_date="1932-06-20",
        limit=1,
    )
    assert urls == ["https://www.loc.gov/item/sn83045462/1932-06-20/ed-1/"]
    client.request.assert_not_called()


def test_discover_issue_item_urls_from_search() -> None:
    title = TitleSpec(lccn="sn83045462", alias="eveningstar")
    client = MagicMock()
    client.request.return_value.json.return_value = {
        "results": [
            {"id": "http://www.loc.gov/item/sn83045462/1932-06-20/ed-1/"},
            {"id": "http://www.loc.gov/item/sn83045462/1932-06-21/ed-1/"},
            {"id": "http://www.loc.gov/collections/chronicling-america/"},
        ],
        "pagination": {"next": None},
    }

    urls = discover_issue_item_urls(client, title, limit=1)
    assert urls == ["http://www.loc.gov/item/sn83045462/1932-06-20/ed-1/"]


def test_fetch_issue_files_prefers_standard_mets_name() -> None:
    client = MagicMock()
    client.request.return_value.json.return_value = SAMPLE_ITEM_JSON

    files = fetch_issue_files(
        client,
        "https://www.loc.gov/item/sn83045462/1932-06-20/ed-1/",
    )
    assert files.issue_dir_name == "1932062001"
    assert files.mets_filename == "1932062001.xml"
    assert files.mets_url.endswith("/1932062001.xml")
    assert files.alto_files == (
        ("0567.xml", SAMPLE_ITEM_JSON["resources"][0]["files"][0][0]["url"]),
        ("0568.xml", SAMPLE_ITEM_JSON["resources"][0]["files"][1][0]["url"]),
    )


def test_run_api_download(tmp_path) -> None:
    title = TitleSpec(lccn="sn83045462", alias="eveningstar")
    client = MagicMock()
    files = ApiIssueFiles(
        item_url="https://www.loc.gov/item/sn83045462/1932-06-20/ed-1/",
        lccn="sn83045462",
        date="1932-06-20",
        edition="ed-1",
        issue_dir_name="1932062001",
        batch="dlc_1arp_ver01",
        mets_url="https://example/1932062001.xml",
        mets_filename="1932062001.xml",
        alto_files=(("0567.xml", "https://example/0567.xml"),),
    )

    with (
        patch(
            "text_preparation.importers.chronicling_america.api.discover_issue_item_urls",
            return_value=[files.item_url],
        ),
        patch(
            "text_preparation.importers.chronicling_america.api.fetch_issue_files",
            return_value=files,
        ),
        patch(
            "text_preparation.importers.chronicling_america.api.download_file",
        ) as mock_download,
    ):
        downloaded = run_api_download(
            client,
            title,
            str(tmp_path),
            issue_date="1932-06-20",
            limit=1,
        )

    assert len(downloaded) == 1
    assert mock_download.call_count == 2
