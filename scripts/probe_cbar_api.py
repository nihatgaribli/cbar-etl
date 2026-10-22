"""Probe the CBAR currency XML endpoint for edge cases before building the ETL.

Usage:
    python scripts/probe_cbar_api.py

The results are summarised in the "API behaviour verified before development" table in README.md.
"""
import time
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import date, timedelta

import requests

BASE_URL = "https://www.cbar.az/currencies/{}.xml"
session = requests.Session()


def fmt(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def fetch(date_str: str) -> tuple[requests.Response, float]:
    started = time.perf_counter()
    response = session.get(BASE_URL.format(date_str), timeout=15)
    return response, time.perf_counter() - started


def parse(content: bytes) -> tuple[str, list[tuple]]:
    root = ET.fromstring(content)
    rows = []
    for val_type in root.findall("ValType"):
        for valute in val_type.findall("Valute"):
            rows.append((
                val_type.get("Type"),
                valute.get("Code"),
                valute.findtext("Nominal"),
                valute.findtext("Name"),
                valute.findtext("Value"),
            ))
    return root.get("Date"), rows


def rates(date_str: str) -> dict[str, str]:
    return {code: value for _, code, _, _, value in parse(fetch(date_str)[0].content)[1]}


def show(label: str, date_str: str) -> list[tuple] | None:
    response, elapsed = fetch(date_str)
    info = (f"HTTP {response.status_code} {response.headers.get('content-type')} "
            f"{len(response.content)}B {elapsed:.2f}s")
    rows = None
    try:
        bulletin_date, rows = parse(response.content)
        info += f" | ValCurs.Date={bulletin_date} rows={len(rows)}"
    except ET.ParseError:
        info += " | NOT XML"
    print(f"{label:24} {date_str:12} {info}")
    return rows


def main() -> None:
    today = date.today()
    # Most recent Saturday/Sunday before today
    saturday = today - timedelta(days=(today.weekday() - 5) % 7 or 7)
    sunday = saturday + timedelta(days=1)
    friday = saturday - timedelta(days=1)
    monday = saturday + timedelta(days=2)

    print("--- 1. requested date vs bulletin date ---")
    show("today", fmt(today))
    show("Saturday", fmt(saturday))
    show("Sunday", fmt(sunday))
    show("tomorrow", fmt(today + timedelta(days=1)))
    show("future +30d", fmt(today + timedelta(days=30)))
    show("New Year", f"01.01.{today.year}")

    print("\n--- 2. invalid requests ---")
    show("invalid date", "31.02.2026")
    show("ISO format", "2026-10-21")
    show("garbage", "abc")

    print("\n--- 3. structure of today's bulletin ---")
    rows = show("today", fmt(today))
    print("ValTypes:", dict(Counter(r[0] for r in rows)))
    print("Nominals:", dict(Counter(r[2] for r in rows)))
    print("Non-1 nominals:", [(r[1], r[2]) for r in rows if r[2] != "1"])
    print("Empty values:", [r[1] for r in rows if not r[4]])

    print("\n--- 4. weekend repeats Friday's bulletin ---")
    fri, sat, mon = rates(fmt(friday)), rates(fmt(saturday)), rates(fmt(monday))
    print("Fri == Sat:", fri == sat, "| Sat == Mon:", sat == mon)

    print("\n--- 5. history: availability, denomination, currency set ---")
    for year in (1993, 1994, 2000, 2005):
        show(str(year), f"15.06.{year}")
    print("USD 30.12.2005 (old manat):", rates("30.12.2005")["USD"])
    print("USD 15.06.2006 (new manat):", rates("15.06.2006")["USD"])
    print("Bulletin returned for 03.01.2006:", parse(fetch("03.01.2006")[0].content)[0])
    old_codes, new_codes = set(rates("15.06.2010")), {r[1] for r in rows}
    print("Removed since 2010:", sorted(old_codes - new_codes))
    print("Added since 2010:", sorted(new_codes - old_codes))
    print("2015 devaluations, USD:",
          [(d, rates(d)["USD"]) for d in ("20.02.2015", "21.02.2015", "20.12.2015", "21.12.2015")])

    print("\n--- 6. burst: 30 sequential requests ---")
    started = time.perf_counter()
    statuses = Counter(fetch(fmt(today - timedelta(days=i)))[0].status_code for i in range(30))
    print("Statuses:", dict(statuses), f"in {time.perf_counter() - started:.1f}s")


if __name__ == "__main__":
    main()
