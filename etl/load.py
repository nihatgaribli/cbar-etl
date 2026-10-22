"""Load step: idempotent upserts into PostgreSQL and the etl_runs audit log.

The connection is used in autocommit mode: run bookkeeping is committed immediately, while the
data itself is written inside an explicit transaction opened by the caller.
"""
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from itertools import batched
from pathlib import Path

import psycopg

from etl.config import Settings
from etl.transform import Bulletin

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
# Files applied on every run; they must be idempotent (IF NOT EXISTS / CREATE OR REPLACE)
SCHEMA_FILES = ("01_schema.sql",)

UPSERT_CURRENCIES = """
INSERT INTO currencies (code, name_az, category, unit, first_seen, last_seen)
SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::date[], %s::date[])
ON CONFLICT (code) DO UPDATE SET
    -- descriptive fields follow the newest bulletin, so backfilling old data cannot revert them
    name_az    = CASE WHEN EXCLUDED.last_seen >= currencies.last_seen
                      THEN EXCLUDED.name_az ELSE currencies.name_az END,
    category   = CASE WHEN EXCLUDED.last_seen >= currencies.last_seen
                      THEN EXCLUDED.category ELSE currencies.category END,
    unit       = CASE WHEN EXCLUDED.last_seen >= currencies.last_seen
                      THEN EXCLUDED.unit ELSE currencies.unit END,
    first_seen = LEAST(currencies.first_seen, EXCLUDED.first_seen),
    last_seen  = GREATEST(currencies.last_seen, EXCLUDED.last_seen)
"""

# Rows whose values did not change are not touched at all, so a repeated run writes nothing.
# xmax = 0 identifies freshly inserted rows among those returned.
UPSERT_RATES = """
INSERT INTO exchange_rates (rate_date, currency_code, nominal, value)
SELECT * FROM unnest(%s::date[], %s::text[], %s::int[], %s::numeric[])
ON CONFLICT (currency_code, rate_date) DO UPDATE SET
    nominal   = EXCLUDED.nominal,
    value     = EXCLUDED.value,
    loaded_at = now()
WHERE (exchange_rates.nominal, exchange_rates.value)
      IS DISTINCT FROM (EXCLUDED.nominal, EXCLUDED.value)
RETURNING (xmax = 0) AS inserted
"""


@dataclass
class LoadStats:
    bulletins: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(**settings.conninfo(), autocommit=True, connect_timeout=10)


def apply_schema(conn: psycopg.Connection) -> None:
    for name in SCHEMA_FILES:
        conn.execute((SQL_DIR / name).read_text(encoding="utf-8"))
        log.debug("Applied %s", name)


class PostgresLoader:
    def __init__(self, conn: psycopg.Connection, batch_size: int = 100) -> None:
        self.conn = conn
        self.batch_size = batch_size
        self.stats = LoadStats()

    def start_run(self, date_from: date, date_to: date) -> int:
        row = self.conn.execute(
            "INSERT INTO etl_runs (date_from, date_to) VALUES (%s, %s) RETURNING run_id",
            (date_from, date_to),
        ).fetchone()
        return row[0]

    def finish_run(self, run_id: int, status: str, error: str | None = None, **counts: int) -> None:
        columns = ", ".join(f"{name} = %({name})s" for name in counts)
        self.conn.execute(
            f"""UPDATE etl_runs
                SET status = %(status)s, finished_at = now(), error_message = %(error)s
                    {", " + columns if columns else ""}
                WHERE run_id = %(run_id)s""",
            {"run_id": run_id, "status": status, "error": error, **counts},
        )

    def load(self, bulletins: Iterable[Bulletin]) -> LoadStats:
        """Upsert bulletins in batches. Call inside conn.transaction() for all-or-nothing."""
        for batch in batched(bulletins, self.batch_size):
            self._upsert_currencies(batch)
            self._upsert_rates(batch)
            self.stats.bulletins += len(batch)
            log.info("Loaded %d bulletins up to %s", self.stats.bulletins, batch[-1].rate_date)
        return self.stats

    def _upsert_currencies(self, batch: tuple[Bulletin, ...]) -> None:
        latest: dict[str, tuple] = {}
        first_seen: dict[str, date] = {}
        for bulletin in sorted(batch, key=lambda b: b.rate_date):
            for currency in bulletin.currencies:
                first_seen.setdefault(currency.code, bulletin.rate_date)
                latest[currency.code] = (currency, bulletin.rate_date)

        codes = list(latest)
        self.conn.execute(UPSERT_CURRENCIES, (
            codes,
            [latest[code][0].name_az for code in codes],
            [latest[code][0].category for code in codes],
            [latest[code][0].unit for code in codes],
            [first_seen[code] for code in codes],
            [latest[code][1] for code in codes],
        ))

    def _upsert_rates(self, batch: tuple[Bulletin, ...]) -> None:
        rates = [rate for bulletin in batch for rate in bulletin.rates]
        cursor = self.conn.execute(UPSERT_RATES, (
            [rate.rate_date for rate in rates],
            [rate.currency_code for rate in rates],
            [rate.nominal for rate in rates],
            [rate.value for rate in rates],
        ))
        written = [inserted for (inserted,) in cursor.fetchall()]
        inserted = sum(written)
        self.stats.inserted += inserted
        self.stats.updated += len(written) - inserted
        self.stats.unchanged += len(rates) - len(written)
