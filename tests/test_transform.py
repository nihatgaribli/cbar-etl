"""Unit tests for the transform step, using real CBAR bulletins and small synthetic ones."""
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from etl.extract import RawBulletin
from etl.transform import (
    Currency,
    PreDenominationError,
    Transformer,
    TransformError,
    parse_bulletin,
    parse_nominal,
    parse_value,
)

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def make_xml(valutes: str, bulletin_date: str = "22.10.2026", section: str = "Xarici valyutalar"):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ValCurs Date="{bulletin_date}" Name="AZN məzənnələri">
  <ValType Type="{section}">{valutes}</ValType>
</ValCurs>""".encode()


def valute(code: str, nominal: str, name: str, value: str) -> str:
    return (f'<Valute Code="{code}"><Nominal>{nominal}</Nominal>'
            f"<Name>{name}</Name><Value>{value}</Value></Valute>")


USD = valute("USD", "1", "1 ABŞ dolları", "1.7")


# --- real bulletins -------------------------------------------------------------------------

def test_real_bulletin_is_parsed_completely():
    bulletin = parse_bulletin(fixture("bulletin_2026-10-22.xml"))

    assert bulletin.rate_date == date(2026, 10, 22)
    assert len(bulletin.rates) == 42
    assert bulletin.skipped_rows == 0
    categories = [c.category for c in bulletin.currencies]
    assert categories.count("currency") == 38
    assert categories.count("metal") == 4


def test_real_bulletin_values():
    bulletin = parse_bulletin(fixture("bulletin_2026-10-22.xml"))
    rates = {rate.currency_code: rate for rate in bulletin.rates}
    currencies = {c.code: c for c in bulletin.currencies}

    assert rates["USD"].value == Decimal("1.7")
    assert rates["RUB"].nominal == 100
    assert rates["RUB"].rate_per_unit == rates["RUB"].value / 100
    assert currencies["RUB"].name_az == "Rusiya rublu"
    assert currencies["XAU"] == Currency("XAU", "Qızıl", "metal", "troy_ounce")


def test_weekend_request_returns_friday_bulletin():
    # finding #1: requested 17.10.2026 (Saturday), received 16.10.2026
    bulletin = parse_bulletin(fixture("bulletin_2026-10-16_via_saturday.xml"))
    assert bulletin.rate_date == date(2026, 10, 16)


def test_old_manat_bulletin_is_rejected():
    # finding #6: USD was 4593 old manat on 29.12.2005
    with pytest.raises(PreDenominationError, match="denomination"):
        parse_bulletin(fixture("bulletin_2005-12-29_old_manat.xml"))


# --- field parsers -------------------------------------------------------------------------

@pytest.mark.parametrize("text, expected", [
    ("1", (1, "unit")),
    ("100", (100, "unit")),
    ("10000", (10000, "unit")),
    ("1 t.u.", (1, "troy_ounce")),
    (" 1 t.u. ", (1, "troy_ounce")),
])
def test_parse_nominal(text, expected):
    assert parse_nominal(text) == expected


@pytest.mark.parametrize("text", ["", None, "0", "abc", "1.5", "-1"])
def test_parse_nominal_rejects_garbage(text):
    with pytest.raises(ValueError):
        parse_nominal(text)


def test_parse_value_keeps_decimal_precision():
    assert parse_value("1.7") == Decimal("1.7")
    assert parse_value(" 6995.4575 ") == Decimal("6995.4575")


@pytest.mark.parametrize("text", ["", None, "abc", "0", "-1.2", "NaN", "Infinity", "1,7"])
def test_parse_value_rejects_garbage(text):
    with pytest.raises(ValueError):
        parse_value(text)


# --- row and bulletin validation -----------------------------------------------------------

def test_empty_value_row_is_skipped():
    # finding #5: old bulletins contain <Value></Value>
    content = make_xml(USD + valute("EUR", "1", "1 Avro", ""))
    bulletin = parse_bulletin(content)

    assert [r.currency_code for r in bulletin.rates] == ["USD"]
    assert bulletin.skipped_rows == 1


def test_bad_currency_code_is_skipped():
    bulletin = parse_bulletin(make_xml(USD + valute("usd1", "1", "x", "1.0")))
    assert bulletin.skipped_rows == 1


def test_bulletin_without_usable_rates_is_rejected():
    with pytest.raises(TransformError, match="no usable rates"):
        parse_bulletin(make_xml(valute("EUR", "1", "1 Avro", "")))


def test_duplicate_codes_are_rejected():
    with pytest.raises(TransformError, match="duplicate"):
        parse_bulletin(make_xml(USD + USD))


def test_unknown_section_is_rejected():
    with pytest.raises(TransformError, match="unknown ValType"):
        parse_bulletin(make_xml(USD, section="Kriptovalyutalar"))


@pytest.mark.parametrize("content, message", [
    (b"<html><body>not found</body></html>", "unexpected root"),
    (b"<ValCurs", "malformed XML"),
    (make_xml(USD, bulletin_date="2026-10-22"), "bad ValCurs Date"),
])
def test_invalid_documents_are_rejected(content, message):
    with pytest.raises(TransformError, match=message):
        parse_bulletin(content)


# --- stream ------------------------------------------------------------------------------

def test_transformer_deduplicates_and_counts():
    raw = [
        RawBulletin(date(2026, 10, 16), fixture("bulletin_2026-10-16_via_saturday.xml")),
        RawBulletin(date(2026, 10, 17), fixture("bulletin_2026-10-16_via_saturday.xml")),
        RawBulletin(date(2026, 10, 18), fixture("bulletin_2026-10-16_via_saturday.xml")),
        RawBulletin(date(2026, 10, 22), fixture("bulletin_2026-10-22.xml")),
        RawBulletin(date(2005, 12, 30), fixture("bulletin_2005-12-29_old_manat.xml")),
    ]
    transformer = Transformer()
    bulletins = list(transformer.transform(raw))

    assert [b.rate_date for b in bulletins] == [date(2026, 10, 16), date(2026, 10, 22)]
    stats = transformer.stats
    assert (stats.bulletins, stats.duplicates, stats.rejected, stats.rows) == (2, 2, 1, 84)
    assert stats.rejected_dates == [date(2005, 12, 30)]
