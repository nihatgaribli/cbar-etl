"""Tests for command-line argument handling."""
from datetime import date

import pytest

from etl.main import parse_args

TODAY = date(2026, 10, 22)


def test_default_is_last_30_days():
    assert parse_args([], today=TODAY) == (date(2026, 9, 23), TODAY)


def test_days_argument():
    assert parse_args(["--days", "1"], today=TODAY) == (TODAY, TODAY)


def test_explicit_range():
    assert parse_args(["--from", "2015-01-01", "--to", "2015-12-31"], today=TODAY) == (
        date(2015, 1, 1), date(2015, 12, 31),
    )


def test_from_without_to_runs_until_today():
    assert parse_args(["--from", "2026-10-01"], today=TODAY) == (date(2026, 10, 1), TODAY)


@pytest.mark.parametrize("argv", [
    ["--from", "2005-12-31"],                       # old manat (finding #6)
    ["--from", "2026-10-01", "--to", "2026-10-23"], # future
    ["--from", "2026-10-10", "--to", "2026-10-01"], # reversed
    ["--to", "2026-10-01"],                         # --to without --from
    ["--days", "0"],
    ["--days", "5", "--from", "2026-10-01"],        # mutually exclusive
    ["--from", "22.10.2026"],                       # not ISO
])
def test_invalid_arguments_exit(argv):
    with pytest.raises(SystemExit):
        parse_args(argv, today=TODAY)
