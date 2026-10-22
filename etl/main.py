"""Command-line entry point: python -m etl.main [--days N | --from DATE [--to DATE]]"""
import argparse
import logging
import sys
import time
from datetime import date, timedelta

import psycopg

from etl.config import DENOMINATION_DATE, Settings
from etl.extract import CbarClient
from etl.load import PostgresLoader, apply_schema, connect
from etl.transform import Transformer

log = logging.getLogger("etl.main")

DEFAULT_DAYS = 30
EXIT_CODES = {"success": 0, "partial": 1, "failed": 2}


def parse_args(argv: list[str] | None = None, today: date | None = None) -> tuple[date, date]:
    today = today or date.today()
    parser = argparse.ArgumentParser(description="Load official CBAR AZN exchange rates.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--days", type=int, help=f"load the last N days (default {DEFAULT_DAYS})")
    group.add_argument("--from", dest="date_from", type=date.fromisoformat,
                       help="first day, YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat,
                        help="last day, YYYY-MM-DD (default: today)")
    args = parser.parse_args(argv)

    if args.date_to and not args.date_from:
        parser.error("--to requires --from")
    if args.days is not None and args.days < 1:
        parser.error("--days must be at least 1")

    if args.date_from:
        start, end = args.date_from, args.date_to or today
    else:
        end = today
        start = end - timedelta(days=(args.days or DEFAULT_DAYS) - 1)

    if start < DENOMINATION_DATE:
        parser.error(f"dates before {DENOMINATION_DATE} are in old manat (AZM) and not supported")
    if end > today:
        parser.error(f"--to {end} is in the future; CBAR would just repeat the latest bulletin")
    if start > end:
        parser.error(f"--from {start} is after --to {end}")
    return start, end


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)-12s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_pipeline(conn: psycopg.Connection, client: CbarClient, start: date, end: date) -> str:
    """Extract -> transform -> load one date range. Returns the final run status."""
    started = time.perf_counter()
    apply_schema(conn)
    loader = PostgresLoader(conn)
    transformer = Transformer()
    run_id = loader.start_run(start, end)
    log.info("Run #%d started", run_id)

    try:
        # All data of a run is committed together, or not at all
        with conn.transaction():
            loader.load(transformer.transform(client.fetch_range(start, end)))
    except Exception as exc:
        log.exception("Run #%d failed, all changes rolled back", run_id)
        loader.finish_run(run_id, "failed", error=f"{type(exc).__name__}: {exc}")
        return "failed"

    extracted, transformed, loaded = client.stats, transformer.stats, loader.stats
    unexpected_rejects = transformed.rejected - transformed.pre_denomination
    status = "partial" if extracted.failed_dates or unexpected_rejects else "success"
    loader.finish_run(
        run_id, status,
        days_requested=extracted.requested,
        days_failed=len(extracted.failed_dates),
        bulletins_loaded=loaded.bulletins,
        duplicates=transformed.duplicates,
        rejected=transformed.rejected,
        rows_inserted=loaded.inserted,
        rows_updated=loaded.updated,
        rows_unchanged=loaded.unchanged,
        rows_skipped=transformed.skipped_rows,
    )

    log.info("Downloaded %d/%d days", extracted.downloaded, extracted.requested)
    log.info("%d unique bulletins, %d duplicates (weekends/holidays), %d rejected, "
             "%d rows skipped", transformed.bulletins, transformed.duplicates,
             transformed.rejected, transformed.skipped_rows)
    log.info("Rows: %d inserted, %d updated, %d unchanged",
             loaded.inserted, loaded.updated, loaded.unchanged)
    if extracted.failed_dates:
        log.error("Failed days: %s", ", ".join(map(str, extracted.failed_dates)))
    log.info("Run #%d finished: %s (%.1fs)", run_id, status, time.perf_counter() - started)
    return status


def main(argv: list[str] | None = None) -> int:
    start, end = parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    try:
        conn = connect(settings)
    except psycopg.OperationalError as exc:
        log.error("Cannot connect to PostgreSQL at %s:%s: %s",
                  settings.postgres_host, settings.postgres_port, exc)
        return EXIT_CODES["failed"]

    with conn:
        status = run_pipeline(conn, CbarClient(settings), start, end)
    return EXIT_CODES[status]


if __name__ == "__main__":
    sys.exit(main())
