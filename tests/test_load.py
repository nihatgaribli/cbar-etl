"""Integration tests for the load step and the full pipeline against a real PostgreSQL.

Each test runs in its own throw-away schema. The tests are skipped when no database is reachable
(start one with: docker compose up -d postgres).
"""
import uuid
from dataclasses import replace
from datetime import date
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
