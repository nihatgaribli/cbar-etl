"""Command-line entry point: python -m etl.main [--days N | --from DATE [--to DATE]]"""
import argparse
import logging
import sys
import time
from datetime import date, timedelta

from etl.config import DENOMINATION_DATE, Settings
from etl.extract import CbarClient
from etl.transform import Transformer

log = logging.getLogger("etl.main")

DEFAULT_DAYS = 30


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


def main(argv: list[str] | None = None) -> int:
    start, end = parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    started = time.perf_counter()
    client = CbarClient(settings)
    transformer = Transformer()
    bulletins = list(transformer.transform(client.fetch_range(start, end)))

    extracted, transformed = client.stats, transformer.stats
    log.info("Downloaded %d/%d days", extracted.downloaded, extracted.requested)
    log.info("%d unique bulletins (%d rates), %d duplicates (weekends/holidays), "
             "%d rejected, %d rows skipped",
             transformed.bulletins, transformed.rows, transformed.duplicates,
             transformed.rejected, transformed.skipped_rows)
    if bulletins:
        log.info("Bulletin dates: %s -> %s", bulletins[0].rate_date, bulletins[-1].rate_date)
    log.info("Finished in %.1fs", time.perf_counter() - started)
    if extracted.failed_dates:
        log.error("Failed days: %s", ", ".join(map(str, extracted.failed_dates)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
