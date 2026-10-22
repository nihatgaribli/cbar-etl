-- Core tables. Idempotent: the ETL applies this file at the start of every run.

CREATE TABLE IF NOT EXISTS currencies (
    code        CHAR(3) PRIMARY KEY CHECK (code ~ '^[A-Z]{3}$'),
    name_az     TEXT    NOT NULL,
    category    TEXT    NOT NULL CHECK (category IN ('currency', 'metal')),
    unit        TEXT    NOT NULL CHECK (unit IN ('unit', 'troy_ounce')),
    first_seen  DATE    NOT NULL,
    last_seen   DATE    NOT NULL,
    CHECK (first_seen <= last_seen)
);

COMMENT ON TABLE currencies IS 'Currencies and bank metals that appear in CBAR bulletins';
COMMENT ON COLUMN currencies.unit IS 'troy_ounce for bank metals (Nominal "1 t.u.")';

CREATE TABLE IF NOT EXISTS exchange_rates (
    currency_code  CHAR(3)         NOT NULL REFERENCES currencies (code),
    rate_date      DATE            NOT NULL CHECK (rate_date >= DATE '2006-01-01'),
    nominal        INTEGER         NOT NULL CHECK (nominal > 0),
    value          NUMERIC(18, 6)  NOT NULL CHECK (value > 0),
    rate_per_unit  NUMERIC(20, 10) GENERATED ALWAYS AS (value / nominal) STORED,
    loaded_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (currency_code, rate_date)
);

COMMENT ON TABLE exchange_rates IS 'Official AZN rates: value AZN for nominal units of the currency';
COMMENT ON COLUMN exchange_rates.rate_date IS 'ValCurs/@Date of the bulletin, not the requested date';

-- The primary key already serves per-currency time series; this index serves per-day lookups.
CREATE INDEX IF NOT EXISTS ix_exchange_rates_rate_date ON exchange_rates (rate_date);

-- Currencies that CBAR kept under the same code across a redenomination. Rates published before
-- effective_date are in old units: new unit = factor old units.
CREATE TABLE IF NOT EXISTS currency_redenominations (
    currency_code   CHAR(3) PRIMARY KEY,
    effective_date  DATE    NOT NULL,
    factor          INTEGER NOT NULL CHECK (factor > 1),
    note            TEXT
);

INSERT INTO currency_redenominations (currency_code, effective_date, factor, note) VALUES
    ('BYN', DATE '2016-07-01', 10000, 'Belarusian ruble: 10,000 BYR = 1 BYN'),
    ('TMT', DATE '2009-01-13',  5000, 'Turkmen manat: 5,000 TMM = 1 TMT (first CBAR bulletin in new units)')
ON CONFLICT (currency_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    status            TEXT        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'partial', 'failed')),
    date_from         DATE        NOT NULL,
    date_to           DATE        NOT NULL,
    days_requested    INTEGER,
    days_failed       INTEGER,
    bulletins_loaded  INTEGER,
    duplicates        INTEGER,
    rejected          INTEGER,
    rows_inserted     INTEGER,
    rows_updated      INTEGER,
    rows_unchanged    INTEGER,
    rows_skipped      INTEGER,
    error_message     TEXT
);

COMMENT ON TABLE etl_runs IS 'One row per pipeline run, written even when the run fails';
