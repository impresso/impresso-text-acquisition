import copy
import json
from pathlib import Path

from text_preparation.importers.kbr.classes import KbrNewspaperIssue
from text_preparation.importers.kbr.detect import (
    analyze_duplicates,
    detect_issues,
    select_issues,
)
from text_preparation.utils import validate_issue_schema, validate_page_schema

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_DATA_DIR = REPO_ROOT / "text_preparation" / "data" / "sample_data" / "KBR"
FIXTURE_PATH = REPO_ROOT / "text_preparation" / "data" / "kbr_test_output" / "sample_output.json"
SAMPLE_ISSUE_KEY = ("Lynx", "1831-04-25", "a")
EXPECTED_ISSUES = [
    ("Bruxellois", "1917-08-07", "a", "16777935_19170807_136489"),
    ("Bruxellois", "1917-08-08", "a", "16777935_19170808_767971"),
    ("Bruxellois", "1917-08-08", "b", "16777935_19170808_136456"),
    ("Lynx", "1831-04-25", "a", "17142621_18310425_457798"),
    ("Lynx", "1831-04-26", "a", "17142621_18310426_457815"),
    ("Lynx", "1831-05-06", "a", "17142621_18310506_457729"),
]


def _issue_key(issue_dir) -> tuple[str, str, str]:
    return (issue_dir.alias, issue_dir.date.isoformat(), issue_dir.edition)


def _summarize_page(page_data: dict) -> dict:
    content_items = []
    seen_content_items = set()
    mapped_region_count = 0
    token_count = 0

    for region in page_data["r"]:
        content_item_id = region.get("pOf")
        if content_item_id:
            mapped_region_count += 1
            if content_item_id not in seen_content_items:
                seen_content_items.add(content_item_id)
                content_items.append(content_item_id)

        for paragraph in region.get("p", []):
            for line in paragraph.get("l", []):
                token_count += len(line.get("t", []))

    return {
        "id": page_data["id"],
        "cdt": "<created>",
        "ts": "<timestamp>",
        "st": page_data["st"],
        "sm": page_data["sm"],
        "cc": page_data["cc"],
        "iiif_img_base_uri": page_data["iiif_img_base_uri"],
        "fw": page_data["fw"],
        "fh": page_data["fh"],
        "region_count": len(page_data["r"]),
        "mapped_region_count": mapped_region_count,
        "token_count": token_count,
        "content_items": content_items,
    }


def _build_sample_output() -> dict:
    sample_issue_dir = next(
        issue_dir
        for issue_dir in detect_issues(str(SAMPLE_DATA_DIR))
        if _issue_key(issue_dir) == SAMPLE_ISSUE_KEY
    )

    issue = KbrNewspaperIssue(sample_issue_dir)
    validate_issue_schema(issue.issue_data)

    issue_data = copy.deepcopy(issue.issue_data)
    issue_data["ts"] = "<timestamp>"

    pages = []
    for page in issue.pages:
        page.add_issue(issue)
        page.parse()
        validate_page_schema(page.page_data)
        pages.append(_summarize_page(page.page_data))

    return {"issue": issue_data, "pages": pages}


def test_detect_issues_assigns_expected_editions():
    issues = detect_issues(str(SAMPLE_DATA_DIR))

    assert [
        (issue.alias, issue.date.isoformat(), issue.edition, Path(issue.path).name)
        for issue in issues
    ] == EXPECTED_ISSUES


def test_analyze_duplicates_prefers_best_bruxellois_copy():
    duplicates = analyze_duplicates(str(SAMPLE_DATA_DIR), alias="Bruxellois")

    assert duplicates == {
        "Bruxellois_1917-08-08": {
            "count": 2,
            "recommended": "16777935_19170808_767971",
            "all_copies": [
                {
                    "path": str(
                        SAMPLE_DATA_DIR / "Bruxellois" / "16777935_19170808_767971"
                    ),
                    "dir_name": "16777935_19170808_767971",
                    "unique_id": 767971,
                    "processing_date": None,
                },
                {
                    "path": str(
                        SAMPLE_DATA_DIR / "Bruxellois" / "16777935_19170808_136456"
                    ),
                    "dir_name": "16777935_19170808_136456",
                    "unique_id": 136456,
                    "processing_date": None,
                },
            ],
        }
    }


def test_sample_output_matches_fixture():
    with open(FIXTURE_PATH, "r", encoding="utf-8") as fixture_file:
        expected_output = json.load(fixture_file)

    assert _build_sample_output() == expected_output


def test_select_issues_filters_titles():
    config = {
        "titles": {"Bruxellois": []},
        "exclude_titles": [],
        "year_only": False,
    }

    issues = select_issues(str(SAMPLE_DATA_DIR), config=config)

    assert issues is not None
    assert [
        (issue.alias, issue.date.isoformat(), issue.edition)
        for issue in issues
    ] == [
        ("Bruxellois", "1917-08-07", "a"),
        ("Bruxellois", "1917-08-08", "a"),
        ("Bruxellois", "1917-08-08", "b"),
    ]
