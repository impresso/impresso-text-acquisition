"""Unit tests for the generalized Chronicling America canonicalization helpers.

These cover the variation axes that differ across the CA corpus (measurement
unit / DPI scaling, image-block flavors, language normalization) and the
layout-agnostic, CA-gated discovery. They use small synthetic METS/ALTO
fragments so they run fast and need no network or bundled data.
"""

import os
from datetime import date

import pytest
from bs4 import BeautifulSoup

from text_preparation.importers.chronicling_america.classes import (
    measurement_divisor,
    normalize_language,
    collect_image_elements,
    TARGET_DPI,
)
from text_preparation.importers.chronicling_america import canonicalize as C


# --------------------------------------------------------------------------- #
# coordinate scaling
# --------------------------------------------------------------------------- #
def test_measurement_divisor_inch1200_is_three_at_400dpi():
    assert TARGET_DPI == 400
    assert measurement_divisor("inch1200") == pytest.approx(3.0)


def test_measurement_divisor_mm10():
    # 1 inch = 254 units of 1/10 mm; at 400 dpi -> 0.635 px per unit
    assert measurement_divisor("mm10") == pytest.approx(254.0 / 400.0)


def test_measurement_divisor_pixel_is_identity():
    assert measurement_divisor("pixel") == 1.0
    assert measurement_divisor("pixels") == 1.0


def test_measurement_divisor_unknown_and_missing_default_to_inch1200():
    assert measurement_divisor(None) == pytest.approx(3.0)
    assert measurement_divisor("") == pytest.approx(3.0)
    assert measurement_divisor("furlongs") == pytest.approx(3.0)


# --------------------------------------------------------------------------- #
# language normalization
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "value,expected",
    [
        ("en", "en"),
        ("ENG", "eng"),
        ("fr", "fr"),
        ("French", "fr"),
        ("german", "de"),
        ("", None),
        (None, None),
        ("not-a-language", None),
    ],
)
def test_normalize_language(value, expected):
    assert normalize_language(value) == expected


# --------------------------------------------------------------------------- #
# image-block detection (the bug your data exposed)
# --------------------------------------------------------------------------- #
IMAGE_PRINTSPACE = """
<PrintSpace>
  <Illustration ID="ILL1" HPOS="0" VPOS="0" WIDTH="10" HEIGHT="10"/>
  <ComposedBlock ID="CB_IMG" TYPE="Illustration" HPOS="0" VPOS="0" WIDTH="20" HEIGHT="20">
    <GraphicalElement ID="CB_IMG_SUB" HPOS="1" VPOS="1" WIDTH="18" HEIGHT="18"/>
  </ComposedBlock>
  <ComposedBlock ID="CB_AD" TYPE="Advertisement" HPOS="0" VPOS="0" WIDTH="30" HEIGHT="30">
    <TextBlock ID="TB1"><TextLine/></TextBlock>
  </ComposedBlock>
  <GraphicalElement ID="GE_LOOSE" HPOS="5" VPOS="5" WIDTH="5" HEIGHT="5"/>
  <TextBlock ID="TB_BODY"><TextLine/></TextBlock>
</PrintSpace>
"""


def test_collect_image_elements_captures_all_flavors_without_double_count():
    ps = BeautifulSoup(IMAGE_PRINTSPACE, "xml").find("PrintSpace")
    ids = {el.get("ID") for el in collect_image_elements(ps)}
    # native Illustration, the image ComposedBlock, and the loose GraphicalElement
    assert ids == {"ILL1", "CB_IMG", "GE_LOOSE"}
    # the GraphicalElement nested inside the image ComposedBlock must NOT appear
    assert "CB_IMG_SUB" not in ids
    # advertisement / body text blocks are not pictures
    assert "CB_AD" not in ids and "TB_BODY" not in ids


# --------------------------------------------------------------------------- #
# alias slug + date/edition derivation + batch version
# --------------------------------------------------------------------------- #
def test_slugify_alias():
    assert C.slugify_alias("The San Francisco Call") == "thesanfranciscocall"
    assert C.slugify_alias(None) is None
    assert C.slugify_alias("12345") is None  # must start with a letter


def test_date_edition_from_name():
    assert C.date_edition_from_name("1912050201.xml") == (date(1912, 5, 2), 1)
    assert C.date_edition_from_name("mets.xml") == (None, None)
    assert C.date_edition_from_name("1912133201.xml") == (None, None)  # invalid date


def test_date_edition_from_path():
    assert C.date_edition_from_path("/x/sn1/reel/1912050201") == (date(1912, 5, 2), 1)
    assert C.date_edition_from_path("/x/sncall/1912/05/02/ed-2") == (date(1912, 5, 2), 2)
    assert C.date_edition_from_path("/x/sncall/1912/05/02") == (date(1912, 5, 2), 1)
    assert C.date_edition_from_path("/x/nothing/here") == (None, None)


def test_batch_version():
    assert C.batch_version("/data/dlc_ferguson_ver02/data/sn1/1912050201") == 2
    assert C.batch_version("/data/no_version/here") == -1


# --------------------------------------------------------------------------- #
# discovery: CA gate + version-preferring dedup
# --------------------------------------------------------------------------- #
def _write_issue(root: str, name: str, lccn: str | None, label: str) -> None:
    os.makedirs(os.path.join(root, "alto"), exist_ok=True)
    lccn_xml = f'<MODS:identifier type="lccn">{lccn}</MODS:identifier>' if lccn else ""
    mets = (
        '<mets xmlns:MODS="http://www.loc.gov/mods/v3" '
        'xmlns:xlink="http://www.w3.org/1999/xlink" '
        f'LABEL="{label}"><dmdSec><mdWrap><xmlData><MODS:mods>'
        f"<MODS:relatedItem>{lccn_xml}</MODS:relatedItem>"
        "</MODS:mods></xmlData></mdWrap></dmdSec>"
        '<structMap><div TYPE="np:issue">'
        '<div TYPE="np:page"><fptr FILEID="ocrFile1"/></div></structMap></mets>'
    )
    with open(os.path.join(root, name), "w", encoding="utf-8") as f:
        f.write(mets)


def test_discover_issues_ca_gate_and_dedup(tmp_path):
    base = str(tmp_path)
    # a real CA issue (has LCCN), in two batch versions
    _write_issue(
        os.path.join(base, "dlc_a_ver01", "1912050201"),
        "1912050201.xml",
        "sn85066387",
        "The San Francisco call, 1912-05-02",
    )
    _write_issue(
        os.path.join(base, "dlc_a_ver03", "1912050201"),
        "1912050201.xml",
        "sn85066387",
        "The San Francisco call, 1912-05-02",
    )
    # a non-CA METS (date-shaped name, but no LCCN) — must be ignored by the gate
    _write_issue(os.path.join(base, "1850010101"), "1850010101.xml", None, "Some BL paper")

    issues, warnings = C.discover_issues(base, {"sn85066387": "sanfranciscocall"})
    assert len(issues) == 1
    iss = issues[0]
    assert iss.alias == "sanfranciscocall"
    assert iss.date == date(1912, 5, 2)
    assert iss.edition == "a"
    # dedup kept the higher batch version
    assert "ver03" in iss.path
    # the non-CA METS was reported as skipped
    assert any("no LCCN" in w for w in warnings)


def test_discover_issues_title_slug_for_unmapped(tmp_path):
    base = str(tmp_path)
    _write_issue(
        os.path.join(base, "1900010101"),
        "1900010101.xml",
        "sn99999999",
        "The Daily Example, 1900-01-01",
    )
    # without a mapping and without title_slug -> skipped
    issues, _ = C.discover_issues(base, {})
    assert issues == []
    # with title_slug -> alias derived from the title
    issues, warnings = C.discover_issues(base, {}, title_slug=True)
    assert len(issues) == 1
    assert issues[0].alias == "thedailyexample"
    assert any("NOT a registered" in w for w in warnings)
