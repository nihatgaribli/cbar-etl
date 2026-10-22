# CBAR AZN Exchange Rates — ETL Pipeline

An end-to-end data engineering project that **extracts** the official daily exchange rates of the
Azerbaijani manat (AZN) from the Central Bank of the Republic of Azerbaijan (CBAR), **transforms** and
validates them with Python, **loads** them into PostgreSQL, and exposes **SQL analytics** on top —
all reproducible with a single `docker compose up`.

> Course: *Practicum: Data Engineering with Python (Git, Docker, SQL)* — AzTU, Master's program
> Status: 🚧 in development — see [Roadmap](#roadmap)

---

## Table of contents

1. [Architecture](#architecture)
2. [Tech stack](#tech-stack)
3. [Data source: CBAR API](#data-source-cbar-api)
4. [Data model](#data-model)
5. [Pipeline logic](#pipeline-logic)
6. [Project structure](#project-structure)
7. [Getting started](#getting-started)
8. [Usage](#usage)
9. [SQL analytics](#sql-analytics)
10. [Testing](#testing)
11. [Git workflow](#git-workflow)
12. [Roadmap](#roadmap)

---

## Architecture

```text
                ┌──────────────────────────────── docker compose ───────────────────────────────┐
                │                                                                               │
 cbar.az  ──────┼──► Extract ──► Transform ──► Load ──────────► PostgreSQL 16 ◄──── SQL analytics │
 (daily XML)    │    requests    validation    psycopg           (volume)            (views)      │
                │    + retry     + pandas      UPSERT                │                            │
                │                                                    └──────► Streamlit dashboard │
                │                 etl service                                 (optional)        │
                └───────────────────────────────────────────────────────────────────────────────┘
```

Every run is logged to the `etl_runs` table, and the pipeline is **idempotent**: running it twice over
the same date range leaves the database unchanged.

## Tech stack

| Area           | Tools                                                     |
|----------------|-----------------------------------------------------------|
| Language       | Python 3.12                                               |
| Extract        | `requests` (session, timeout, retry with backoff)         |
| Transform      | `xml.etree.ElementTree`, `pandas`                         |
| Load           | `psycopg` 3, PostgreSQL 16                                |
| Orchestration  | Docker, Docker Compose                                    |
| Quality        | `pytest`, `ruff`, GitHub Actions                          |
| Visualization  | Streamlit *(optional)*                                    |
| Version control| Git, GitHub (feature branches, Conventional Commits)      |

---

## Data source: CBAR API

### Endpoint

```text
GET https://www.cbar.az/currencies/{DD.MM.YYYY}.xml
```

No API key, no authentication, no registration.

### Response example (trimmed)

```xml
<ValCurs Date="21.10.2026" Name="AZN məzənnələri" Description="...">
  <ValType Type="Bank metalları">
    <Valute Code="XAU">
      <Nominal>1 t.u.</Nominal>
      <Name>Qızıl</Name>
      <Value>6995.4575</Value>
    </Valute>
  </ValType>
  <ValType Type="Xarici valyutalar">
    <Valute Code="USD">
      <Nominal>1</Nominal>
      <Name>1 ABŞ dolları</Name>
      <Value>1.7</Value>
    </Valute>
    <Valute Code="RUB">
      <Nominal>100</Nominal>
      <Name>100 Rusiya rublu</Name>
      <Value>2.0619</Value>
    </Valute>
  </ValType>
</ValCurs>
```

`Value` is the price of `Nominal` units in AZN, so 1 RUB = 2.0619 / 100 AZN.

### API behaviour verified before development

The endpoint was probed on **2026-10-22** ([`scripts/probe_cbar_api.py`](scripts/probe_cbar_api.py)).
These findings drive the design of the pipeline:

| # | Finding | Evidence | How the pipeline handles it |
| --- | --- | --- | --- |
| 1 | **Weekends, holidays and future dates return the *last published* bulletin**, not an error | Request `18.10.2026` (Sun) → `ValCurs Date="16.10.2026"` (Fri). Request `21.11.2026` → latest bulletin | The authoritative date is the **`ValCurs/@Date` attribute**, never the requested date. Bulletins are de-duplicated by that date |
| 2 | **Invalid requests still return HTTP 200**, but with an HTML page | `31.02.2026`, `2026-10-21`, `abc`, `15.06.1990` → `200 text/html`, 141 KB | Validate `Content-Type` contains `xml` **and** the root tag is `ValCurs`; otherwise raise `InvalidResponseError` |
| 3 | **Three kinds of `Nominal`**: `1`, `100` (JPY, RUB, KZT, KRW, HUF, UZS, PKR) and `1 t.u.` (troy ounce, bank metals). Historical files also contain `10000` | Today: 31 × `1`, 7 × `100`, 4 × `1 t.u.` | Parse the leading integer, store `nominal` and compute `rate_per_unit = value / nominal`. Metals get unit `troy_ounce` |
| 4 | **Two categories** under `ValType`: `Xarici valyutalar` (38 currencies) and `Bank metalları` (4 metals: XAU, XAG, XPT, XPD) | 42 rows per bulletin | Stored as `category` = `currency` / `metal` |
| 5 | **Empty `Value`** in old bulletins (metals in 2000 and 2005) | `<Value></Value>` for XPD on 09.03.2005 | Row is skipped and counted in `rows_skipped` |
| 6 | **Manat denomination (2006): pre-2006 values are in old manat** (1 new AZN = 5000 old AZM) | USD `4593` on 30.12.2005 vs `0.8963` on 15.06.2006. Request `03.01.2006` still returns the `30.12.2005` bulletin | Bulletins with `ValCurs/@Date < 2006-01-01` are rejected. The check uses the **bulletin date**, not the requested date |
| 7 | **The currency list changes over time** | Since 2010 ARS, BRL, EGP, IDR, IRR, LBP, MXN, TJS, ZAR were removed and CZK, HUF, PKR, QAR, RON, RSD were added | The `currencies` dimension is upserted dynamically, with `first_seen` / `last_seen` |
| 8 | **Data availability**: from 1994. Usable range for this project: **2006 → today** | 1992–1993 → HTML; 1994 → 21 rows | `--from` earlier than 2006-01-01 is rejected by the CLI |
| 9 | **No rate limiting observed**. 30 sequential requests → 30 × HTTP 200 in ~3.5 s. `Cache-Control: no-store` | burst test | A polite 0.2 s delay between requests, a 15 s timeout and 3 retries with exponential backoff (network errors and 5xx) |
| 10 | Encoding is UTF-8 with Azerbaijani characters (`ə`, `ü`, `ş`) | `Name="AZN məzənnələri"` | Parse bytes (`response.content`) with the XML parser, not `response.text` |

Useful historical events for analysis: the AZN devaluations on **21.02.2015** (USD 0.7862 → 1.0500) and
**21.12.2015** (USD 1.0499 → 1.5500, transition to a floating rate).

---

## Data model

```text
┌──────────────────────────┐        ┌──────────────────────────────────┐
│ currencies               │        │ exchange_rates                   │
├──────────────────────────┤        ├──────────────────────────────────┤
│ code        CHAR(3)  PK  │◄───────│ currency_code CHAR(3)  PK, FK    │
│ name_az     TEXT         │        │ rate_date     DATE     PK        │
│ category    TEXT         │        │ nominal       INTEGER            │
│ unit        TEXT         │        │ value         NUMERIC(18,6)      │
│ first_seen  DATE         │        │ rate_per_unit NUMERIC(20,10)     │
│ last_seen   DATE         │        │               (generated)        │
└──────────────────────────┘        │ loaded_at     TIMESTAMPTZ        │
                                    └──────────────────────────────────┘
┌──────────────────────────────────────────────┐
│ etl_runs                                     │
├──────────────────────────────────────────────┤
│ run_id, started_at, finished_at, status,     │
│ date_from, date_to, days_requested,          │
│ bulletins_loaded, rows_upserted,             │
│ rows_skipped, error_message                  │
└──────────────────────────────────────────────┘
```

- `category`: `currency` | `metal`
- `unit`: `unit` | `troy_ounce`
- `rate_per_unit` is a generated column: `value / nominal`
- Index on `exchange_rates (currency_code, rate_date)` for time-series queries

DDL: [`sql/01_schema.sql`](sql/01_schema.sql)

---

## Pipeline logic

```text
for each calendar day in [date_from, date_to]:
    response = GET /currencies/{day}.xml         (retry ×3, timeout 15 s)
    assert response is XML with root <ValCurs>   (finding #2)
    bulletin_date = ValCurs/@Date                (finding #1)
    if bulletin_date already seen → skip         (weekends/holidays)
    if bulletin_date < 2006-01-01 → reject       (finding #6)
    parse rows: code, category, nominal, value   (findings #3, #4)
    drop rows with empty value                   (finding #5)

upsert currencies      ON CONFLICT (code) DO UPDATE last_seen
upsert exchange_rates  ON CONFLICT (currency_code, rate_date) DO UPDATE
write a row to etl_runs (success / failed)
```

One transaction per run, so a failure rolls back the whole batch and is still recorded in `etl_runs`.

---

## Project structure

```text
.
├── README.md
├── docker-compose.yml          # postgres + etl (+ dashboard)
├── Dockerfile                  # ETL image
├── .env.example                # copy to .env
├── .gitignore
├── .gitattributes              # LF line endings (Windows ↔ Linux containers)
├── pyproject.toml              # ruff + pytest configuration
├── requirements.txt
├── requirements-dev.txt        # pytest, ruff
├── etl/
│   ├── __init__.py
│   ├── config.py               # settings from environment variables
│   ├── extract.py              # HTTP client: retry, validation
│   ├── transform.py            # XML → validated records
│   ├── load.py                 # PostgreSQL upserts, run log
│   └── main.py                 # CLI entry point
├── sql/
│   ├── 01_schema.sql           # tables, indexes (auto-run by Postgres on first start)
│   └── 02_analytics.sql        # views and analytical queries
├── scripts/
│   └── probe_cbar_api.py       # API behaviour probe (see findings table)
├── tests/
│   ├── fixtures/               # real CBAR XML bulletins
│   ├── test_extract.py
│   ├── test_main.py            # CLI argument validation
│   └── test_transform.py
├── dashboard/                  # optional
│   └── app.py
└── .github/workflows/ci.yml    # lint + tests on every push
```

---

## Getting started

### Prerequisites

- Docker Desktop (Compose v2)
- Git
- *(for local development only)* Python 3.12+

### Run with Docker

```bash
git clone https://github.com/<your-username>/cbar-etl.git
cd cbar-etl
cp .env.example .env            # adjust passwords if needed
docker compose up -d postgres   # schema is created automatically
docker compose run --rm etl     # load the last 30 days
```

### Run locally (development)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt
docker compose up -d postgres
python -m etl.main --days 30
```

### Environment variables

| Variable            | Default       | Description                      |
|---------------------|---------------|----------------------------------|
| `POSTGRES_USER`     | `etl`         | Database user                    |
| `POSTGRES_PASSWORD` | `etl`         | Database password                |
| `POSTGRES_DB`       | `cbar`        | Database name                    |
| `POSTGRES_HOST`     | `postgres`    | `localhost` when running locally |
| `POSTGRES_PORT`     | `5432`        | Database port                    |
| `REQUEST_DELAY`     | `0.2`         | Seconds between API requests     |
| `LOG_LEVEL`         | `INFO`        | Python logging level             |

---

## Usage

```bash
# last N days (default 30)
python -m etl.main --days 90

# explicit range (dates in ISO format)
python -m etl.main --from 2015-01-01 --to 2015-12-31

# in Docker
docker compose run --rm etl --from 2024-01-01 --to 2024-12-31
```

Example output:

```text
2026-10-22 14:02:11 INFO  extract   Requesting 30 days: 2026-09-23 → 2026-10-22
2026-10-22 14:02:17 INFO  transform 22 unique bulletins, 8 duplicates (weekends/holidays) skipped
2026-10-22 14:02:17 INFO  load      924 rows upserted, 0 skipped
2026-10-22 14:02:17 INFO  main      Run #3 finished: success (6.1 s)
```

Connect to the database:

```bash
docker compose exec postgres psql -U etl -d cbar
```

---

## SQL analytics

Defined in [`sql/02_analytics.sql`](sql/02_analytics.sql):

| View / query            | Technique                          | Question it answers                                  |
|-------------------------|------------------------------------|------------------------------------------------------|
| `v_latest_rates`        | `DISTINCT ON`                      | What are today's official rates?                     |
| `v_daily_change`        | `LAG()` window function            | How much did each currency move day over day (%)?    |
| `v_moving_avg_7d`       | `AVG() OVER (ROWS 6 PRECEDING)`    | What is the smoothed trend?                          |
| `v_monthly_summary`     | `GROUP BY date_trunc`, `FIRST/LAST`| Monthly open / close / min / max                     |
| Volatility ranking      | `STDDEV()` of daily returns, `RANK()` | Which currency is the most volatile?              |
| Shock detection         | CTE + filter `abs(change) > 10%`   | Find devaluation days (e.g. 2015)                    |
| Cross rates             | self-join                          | EUR/USD, gold price in USD derived from AZN rates    |

---

## Testing

```bash
pytest -v
ruff check .
```

Unit tests use saved XML fixtures, so they do **not** call the real API:

- normal bulletin → 42 records with correct `rate_per_unit`
- `Nominal = 100` and `1 t.u.` parsing
- HTML response → `InvalidResponseError`
- empty `<Value>` → row skipped
- pre-2006 bulletin → rejected
- duplicate bulletin date → de-duplicated

---

## Git workflow

- `main` always works. Every feature is developed on a branch: `feature/extract`,
  `feature/transform`, `feature/load`, `feature/docker`, `feature/sql-analytics`, `feature/dashboard`
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/):
  `feat: add retry with backoff to extractor`, `fix: handle empty Value tag`, `docs: ...`, `test: ...`
- Merge with `git merge --no-ff` so the history shows each feature
- Release tags: `v0.1.0` (extract), `v0.5.0` (DB load), `v1.0.0` (final)
- Secrets (`.env`) are never committed. `.env.example` is committed instead

---

## Roadmap

- [x] **Phase 1 — API research.** Endpoint probing, edge cases, findings table
- [x] **Phase 0 — Setup.** Git repo, `.gitignore`, `.env.example`, `requirements.txt`, virtualenv
- [x] **Phase 2 — Extract.** HTTP session, timeout, retry/backoff, response validation, logging, CLI
- [ ] **Phase 3 — Transform.** XML parsing, nominal normalization, de-duplication, denomination guard, unit tests
- [ ] **Phase 4 — Load.** `01_schema.sql`, psycopg upserts, transactions, `etl_runs` log, idempotency check
- [ ] **Phase 5 — Docker.** `Dockerfile`, `docker-compose.yml`, healthcheck, named volume, init scripts
- [ ] **Phase 6 — SQL analytics.** Views and analytical queries, historical backfill 2015 → today
- [ ] **Phase 7 — Dashboard** *(optional)*. Streamlit charts on top of the views
- [ ] **Phase 8 — Polish.** GitHub Actions CI, final README, screenshots, `v1.0.0` tag

---

## License

Educational project. Exchange rate data © Central Bank of the Republic of Azerbaijan ([cbar.az](https://www.cbar.az)).
