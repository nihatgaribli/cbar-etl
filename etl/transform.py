"""Transform step: parse raw CBAR XML into validated, de-duplicated records.

Each rule below maps to a finding in the README's "API behaviour" table.
"""
import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from etl.config import DENOMINATION_DATE
from etl.extract import RawBulletin

log = logging.getLogger(__name__)

# finding #4: two sections per bulletin
CATEGORIES = {
    "Xarici valyutalar": "currency",
    "Bank metalları": "metal",
}
CODE_PATTERN = re.compile(r"^[A-Z]{3}$")
# finding #3: "1", "100", "10000" or "1 t.u." (troy ounce)
NOMINAL_PATTERN = re.compile(r"^(\d+)\s*(t\.u\.)?$")
# "100 Rusiya rublu" -> "Rusiya rublu"
NAME_PREFIX = re.compile(r"^\d+\s+")


class TransformError(Exception):
    """A whole bulletin is unusable."""


class PreDenominationError(TransformError):
    """Expected around 2006-01-01: CBAR still serves the last old-manat bulletin."""


@dataclass(frozen=True)
class Currency:
    code: str
    name_az: str
    category: str
    unit: str


@dataclass(frozen=True)
class Rate:
    rate_date: date
    currency_code: str
    nominal: int
    value: Decimal

    @property
    def rate_per_unit(self) -> Decimal:
        return self.value / self.nominal


@dataclass(frozen=True)
class Bulletin:
    rate_date: date
    currencies: list[Currency]
    rates: list[Rate]
    skipped_rows: int = 0


@dataclass
class TransformStats:
    bulletins: int = 0
    duplicates: int = 0
    rejected: int = 0
    pre_denomination: int = 0  # subset of rejected that is expected, see PreDenominationError
    rows: int = 0
    skipped_rows: int = 0
    rejected_dates: list[date] = field(default_factory=list)


def parse_nominal(text: str | None) -> tuple[int, str]:
    """Return (nominal, unit). Raises ValueError for anything unexpected."""
    match = NOMINAL_PATTERN.match((text or "").strip())
    if not match or int(match.group(1)) == 0:
        raise ValueError(f"unexpected Nominal {text!r}")
    unit = "troy_ounce" if match.group(2) else "unit"
    return int(match.group(1)), unit


def parse_value(text: str | None) -> Decimal:
    """Parse a rate with Decimal: binary floats would distort values such as 1.7."""
    try:
        value = Decimal((text or "").strip())
    except InvalidOperation:
        raise ValueError(f"unexpected Value {text!r}") from None
    if not value.is_finite() or value <= 0:
        raise ValueError(f"unexpected Value {text!r}")
    return value


def parse_bulletin(content: bytes) -> Bulletin:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise TransformError(f"malformed XML: {exc}") from exc
    if root.tag != "ValCurs":
        raise TransformError(f"unexpected root element <{root.tag}>")

    # finding #1: the attribute, not the requested date, says which day the rates belong to
    try:
        rate_date = datetime.strptime(root.get("Date", ""), "%d.%m.%Y").date()
    except ValueError:
        raise TransformError(f"bad ValCurs Date {root.get('Date')!r}") from None
    # finding #6: values before the 2006 denomination are in old manat
    if rate_date < DENOMINATION_DATE:
        raise PreDenominationError(
            f"bulletin {rate_date} predates the 2006 denomination (old manat)"
        )

    currencies: list[Currency] = []
    rates: list[Rate] = []
    skipped = 0
    for section in root.findall("ValType"):
        category = CATEGORIES.get(section.get("Type", ""))
        if category is None:
            raise TransformError(f"unknown ValType {section.get('Type')!r} on {rate_date}")
        for valute in section.findall("Valute"):
            code = (valute.get("Code") or "").strip()
            try:
                if not CODE_PATTERN.match(code):
                    raise ValueError(f"bad currency code {code!r}")
                nominal, unit = parse_nominal(valute.findtext("Nominal"))
                value = parse_value(valute.findtext("Value"))  # finding #5: may be empty
            except ValueError as exc:
                log.warning("%s %s: row skipped, %s", rate_date, code or "?", exc)
                skipped += 1
                continue
            name = NAME_PREFIX.sub("", (valute.findtext("Name") or code).strip())
            currencies.append(Currency(code, name, category, unit))
            rates.append(Rate(rate_date, code, nominal, value))

    codes = [rate.currency_code for rate in rates]
    if len(codes) != len(set(codes)):
        raise TransformError(f"duplicate currency codes in bulletin {rate_date}")
    if not rates:
        raise TransformError(f"bulletin {rate_date} contains no usable rates")
    return Bulletin(rate_date, currencies, rates, skipped)


class Transformer:
    """Turns a stream of raw bulletins into unique, validated ones.

    Weekends and holidays repeat the previous bulletin (finding #1), so every bulletin date
    is emitted only once per run.
    """

    def __init__(self) -> None:
        self.stats = TransformStats()
        self._seen: set[date] = set()

    def transform(self, raw_bulletins: Iterable[RawBulletin]) -> Iterator[Bulletin]:
        for raw in raw_bulletins:
            try:
                bulletin = parse_bulletin(raw.content)
            except TransformError as exc:
                expected = isinstance(exc, PreDenominationError)
                log.log(logging.WARNING if expected else logging.ERROR,
                        "Rejected bulletin requested for %s: %s", raw.requested_date, exc)
                self.stats.rejected += 1
                self.stats.pre_denomination += expected
                self.stats.rejected_dates.append(raw.requested_date)
                continue

            if bulletin.rate_date in self._seen:
                log.debug("%s repeats bulletin %s", raw.requested_date, bulletin.rate_date)
                self.stats.duplicates += 1
                continue

            self._seen.add(bulletin.rate_date)
            self.stats.bulletins += 1
            self.stats.rows += len(bulletin.rates)
            self.stats.skipped_rows += bulletin.skipped_rows
            yield bulletin
