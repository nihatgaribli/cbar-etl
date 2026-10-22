"""Integration tests for the load step and the full pipeline against a real PostgreSQL.

Each test runs in its own throw-away schema. The tests are skipped when no database is reachable
(start one with: docker compose up -d postgres).
"""
import os
import uuid
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
import requests

from etl.config import Settings
from etl.extract import CbarClient
from etl.load import PostgresLoader, apply_schema, connect
from etl.main import run_pipeline
from etl.transform import parse_bulletin

FIXTURES = Path(__file__).parent / "fixtures"
SETTINGS = Settings.from_env()


def bulletin(name: str):
    return parse_bulletin((FIXTURES / name).read_bytes())


@pytest.fixture
def conn():
    try:
        connection = connect(SETTINGS)
    except psycopg.OperationalError as exc:
        if os.environ.get("REQUIRE_DB"):  # set in CI, so a missing database fails the build
            pytest.fail(f"PostgreSQL not available: {exc}")
        pytest.skip(f"PostgreSQL not available: {exc}")
    schema = f"test_{uuid.uuid4().hex[:8]}"
    connection.execute(f"CREATE SCHEMA {schema}")
    connection.execute(f"SET search_path TO {schema}")
    apply_schema(connection)
    yield connection
    connection.execute(f"DROP SCHEMA {schema} CASCADE")
    connection.close()


def scalar(conn, query, params=None):
    return conn.execute(query, params).fetchone()[0]


def test_schema_is_idempotent(conn):
    apply_schema(conn)
    apply_schema(conn)
    assert scalar(conn, "SELECT count(*) FROM exchange_rates") == 0


def test_load_inserts_rates_and_currencies(conn):
    stats = PostgresLoader(conn).load([
        bulletin("bulletin_2026-10-16_via_saturday.xml"),
        bulletin("bulletin_2026-10-22.xml"),
    ])

    assert (stats.bulletins, stats.inserted, stats.updated, stats.unchanged) == (2, 84, 0, 0)
    assert scalar(conn, "SELECT count(*) FROM currencies") == 42
    assert conn.execute(
        "SELECT first_seen, last_seen, unit FROM currencies WHERE code = 'XAU'"
    ).fetchone() == (date(2026, 10, 16), date(2026, 10, 22), "troy_ounce")


def test_rate_per_unit_is_generated(conn):
    PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])
    value, nominal, per_unit = conn.execute(
        "SELECT value, nominal, rate_per_unit FROM exchange_rates WHERE currency_code = 'RUB'"
    ).fetchone()
    assert nominal == 100
    assert per_unit == value / 100


def test_reload_is_idempotent(conn):
    PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])
    loaded_at = scalar(conn, "SELECT max(loaded_at) FROM exchange_rates")

    stats = PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])

    assert (stats.inserted, stats.updated, stats.unchanged) == (0, 0, 42)
    assert scalar(conn, "SELECT count(*) FROM exchange_rates") == 42
    assert scalar(conn, "SELECT max(loaded_at) FROM exchange_rates") == loaded_at


def test_changed_value_is_updated(conn):
    PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])
    conn.execute("UPDATE exchange_rates SET value = 9.99 WHERE currency_code = 'USD'")

    stats = PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])

    assert (stats.inserted, stats.updated, stats.unchanged) == (0, 1, 41)
    assert scalar(conn, "SELECT value FROM exchange_rates WHERE currency_code = 'USD'") == \
        Decimal("1.700000")


def test_backfill_extends_first_seen_but_keeps_latest_name(conn):
    PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])
    conn.execute("UPDATE currencies SET name_az = 'ABŞ dolları (yeni)' WHERE code = 'USD'")

    PostgresLoader(conn).load([bulletin("bulletin_2026-10-16_via_saturday.xml")])

    assert conn.execute(
        "SELECT name_az, first_seen, last_seen FROM currencies WHERE code = 'USD'"
    ).fetchone() == ("ABŞ dolları (yeni)", date(2026, 10, 16), date(2026, 10, 22))


def test_failed_transaction_leaves_no_data(conn):
    with pytest.raises(RuntimeError), conn.transaction():
        PostgresLoader(conn).load([bulletin("bulletin_2026-10-22.xml")])
        raise RuntimeError("crash after load")
    assert scalar(conn, "SELECT count(*) FROM exchange_rates") == 0


def test_run_log(conn):
    loader = PostgresLoader(conn)
    run_id = loader.start_run(date(2026, 10, 1), date(2026, 10, 22))
    assert scalar(conn, "SELECT status FROM etl_runs WHERE run_id = %s", (run_id,)) == "running"

    loader.finish_run(run_id, "success", days_requested=22, rows_inserted=5)

    row = conn.execute(
        "SELECT status, days_requested, rows_inserted, finished_at IS NOT NULL "
        "FROM etl_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row == ("success", 22, 5, True)


# --- full pipeline with a fake CBAR server ------------------------------------------------

class FixtureSession:
    """Serves fixture files by date; unknown dates get the HTML error page CBAR would send."""

    def __init__(self, routes: dict[str, str]):
        self.routes = routes
        self.headers = {}

    def get(self, url, timeout):
        response = requests.Response()
        response.status_code = 200
        name = self.routes.get(url.rsplit("/", 1)[-1])
        if name:
            response._content = (FIXTURES / name).read_bytes()
            response.headers["Content-Type"] = "application/xml"
        else:
            response._content = b"<!DOCTYPE html><html></html>"
            response.headers["Content-Type"] = "text/html; charset=UTF-8"
        return response


def make_client(routes):
    return CbarClient(replace(SETTINGS, request_delay=0), session=FixtureSession(routes))


WEEK = {
    "16.10.2026.xml": "bulletin_2026-10-16_via_saturday.xml",
    "17.10.2026.xml": "bulletin_2026-10-16_via_saturday.xml",
    "18.10.2026.xml": "bulletin_2026-10-16_via_saturday.xml",
}


def test_pipeline_success_and_rerun(conn):
    start, end = date(2026, 10, 16), date(2026, 10, 18)
    assert run_pipeline(conn, make_client(WEEK), start, end) == "success"
    assert run_pipeline(conn, make_client(WEEK), start, end) == "success"

    runs = conn.execute(
        "SELECT days_requested, bulletins_loaded, duplicates, rows_inserted, rows_unchanged "
        "FROM etl_runs ORDER BY run_id"
    ).fetchall()
    assert runs == [(3, 1, 2, 42, 0), (3, 1, 2, 0, 42)]
    assert scalar(conn, "SELECT count(*) FROM exchange_rates") == 42


def test_pipeline_partial_when_days_fail(conn):
    routes = {"16.10.2026.xml": "bulletin_2026-10-16_via_saturday.xml"}  # 17th is "broken"
    status = run_pipeline(conn, make_client(routes), date(2026, 10, 16), date(2026, 10, 17))

    assert status == "partial"
    assert conn.execute("SELECT status, days_failed FROM etl_runs").fetchone() == ("partial", 1)


def test_pipeline_failure_is_logged_and_rolled_back(conn, monkeypatch):
    def explode(self, batch):
        raise psycopg.DataError("simulated database error")

    monkeypatch.setattr(PostgresLoader, "_upsert_rates", explode)
    status = run_pipeline(conn, make_client(WEEK), date(2026, 10, 16), date(2026, 10, 18))

    assert status == "failed"
    assert conn.execute("SELECT status, error_message FROM etl_runs").fetchone() == (
        "failed", "DataError: simulated database error",
    )
    assert scalar(conn, "SELECT count(*) FROM currencies") == 0


# --- analytical views ---------------------------------------------------------------------

@pytest.fixture
def two_bulletins(conn):
    PostgresLoader(conn).load([
        bulletin("bulletin_2026-10-16_via_saturday.xml"),
        bulletin("bulletin_2026-10-22.xml"),
    ])
    return conn


def test_daily_change_compares_with_previous_bulletin(two_bulletins):
    conn = two_bulletins
    prev_date, prev_rate, rate, change = conn.execute(
        "SELECT prev_date, prev_rate, rate_per_unit, change_pct FROM v_daily_change "
        "WHERE currency_code = 'EUR' AND rate_date = '2026-10-22'"
    ).fetchone()
    assert prev_date == date(2026, 10, 16)
    assert change == round(100 * (rate / prev_rate - 1), 4)


def test_latest_rates_show_newest_bulletin_only(two_bulletins):
    rows = two_bulletins.execute("SELECT DISTINCT rate_date, count(*) OVER () FROM v_latest_rates")
    assert rows.fetchall() == [(date(2026, 10, 22), 42)]


def test_usd_cross_rates(two_bulletins):
    eur_usd = scalar(two_bulletins, "SELECT price_in_usd FROM v_usd_cross_rates "
                                    "WHERE currency_code = 'EUR' AND rate_date = '2026-10-22'")
    assert eur_usd == round(Decimal("1.9113") / Decimal("1.7"), 6)


def test_monthly_summary_open_close(two_bulletins):
    row = two_bulletins.execute(
        "SELECT open, close, bulletins FROM v_monthly_summary "
        "WHERE currency_code = 'USD' AND month = '2026-10-01'"
    ).fetchone()
    assert row == (Decimal("1.7"), Decimal("1.7"), 2)


def test_shocks_detect_large_moves(two_bulletins):
    conn = two_bulletins
    shocks = "SELECT currency_code, change_pct FROM v_shocks ORDER BY currency_code"
    # real data: palladium fell 7.07 % between the two bulletins
    assert conn.execute(shocks).fetchall() == [("XPD", Decimal("-7.0652"))]

    conn.execute("UPDATE exchange_rates SET value = value * 1.5 "
                 "WHERE currency_code = 'USD' AND rate_date = '2026-10-22'")
    assert conn.execute(shocks).fetchall() == [
        ("USD", Decimal("50.0000")),
        ("XPD", Decimal("-7.0652")),
    ]


def test_daily_change_is_redenomination_aware(two_bulletins):
    # BYN: 10,000 BYR = 1 BYN on 2016-07-01 (real CBAR values around the switch)
    conn = two_bulletins
    conn.execute("""INSERT INTO exchange_rates (currency_code, rate_date, nominal, value)
                    VALUES ('BYN', '2016-06-30', 100, 0.0077), ('BYN', '2016-07-01', 1, 0.7674)""")
    change, flag = conn.execute(
        "SELECT change_pct, across_redenomination FROM v_daily_change "
        "WHERE currency_code = 'BYN' AND rate_date = '2016-07-01'"
    ).fetchone()
    assert change == round(100 * (Decimal("0.7674") / Decimal("0.77") - 1), 4)  # -0.34 %
    assert flag is True
    assert scalar(conn, "SELECT count(*) FROM v_shocks "
                        "WHERE currency_code = 'BYN' AND rate_date = '2016-07-01'") == 0


def insert_series(conn, code, values, start=date(2020, 1, 1)):
    conn.execute(
        "INSERT INTO currencies VALUES (%s, %s, 'currency', 'unit', %s, %s) "
        "ON CONFLICT DO NOTHING", (code, code, start, start + timedelta(days=len(values))),
    )
    conn.execute(
        "INSERT INTO exchange_rates (currency_code, rate_date, nominal, value) "
        "SELECT %s, %s::date + i::int, 1, v FROM unnest(%s::numeric[]) WITH ORDINALITY AS t(v, i)",
        (code, start - timedelta(days=1), values),
    )


def test_isolated_spike_is_flagged_and_excluded(conn):
    # like DKK on 2015-08-14: 1567 instead of ~0.156
    values = [Decimal("0.156")] * 12 + [Decimal("1567")] + [Decimal("0.157")] * 12
    insert_series(conn, "DKK", values)

    suspects = conn.execute("SELECT rate_date, published_rate FROM v_suspect_rates").fetchall()
    assert suspects == [(date(2020, 1, 13), Decimal("1567.0000000000"))]
    # the day after the spike is compared with the last clean rate, not with the spike
    assert scalar(conn, "SELECT max(abs(change_pct)) FROM v_daily_change") < 1
    assert scalar(conn, "SELECT count(*) FROM v_shocks") == 0


def test_shifted_plateau_is_flagged(conn):
    # like XAG in April 2009: a wrong value repeated over a weekend
    values = [Decimal("10")] * 12 + [Decimal("707")] * 3 + [Decimal("10.1")] * 12
    insert_series(conn, "XAG", values)
    assert scalar(conn, "SELECT count(*) FROM v_suspect_rates") == 3


def test_genuine_level_shift_is_kept(conn):
    # like USD on 2015-12-21: 1.05 -> 1.55 and it stays there
    values = [Decimal("1.05")] * 15 + [Decimal("1.55")] * 15
    insert_series(conn, "USD", values)

    assert scalar(conn, "SELECT count(*) FROM v_suspect_rates") == 0
    assert conn.execute("SELECT rate_date, change_pct FROM v_shocks").fetchall() == [
        (date(2020, 1, 16), Decimal("47.6190")),
    ]
