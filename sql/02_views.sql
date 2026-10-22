-- Analytical views. Idempotent: the ETL applies this file at the start of every run.
-- All prices are rate_per_unit: AZN for ONE unit of the currency (or one troy ounce of metal),
-- so currencies quoted per 100 units (RUB, JPY, ...) are directly comparable.

-- Day-over-day movement. Windows run per currency over published bulletins only, so Monday
-- is compared with Friday; prev_date shows the gap.
CREATE OR REPLACE VIEW v_daily_change AS
SELECT
    r.currency_code,
    r.rate_date,
    r.rate_per_unit,
    LAG(r.rate_date)     OVER w AS prev_date,
    LAG(r.rate_per_unit) OVER w AS prev_rate,
    ROUND(100 * (r.rate_per_unit / LAG(r.rate_per_unit) OVER w - 1), 4) AS change_pct
FROM exchange_rates AS r
WINDOW w AS (PARTITION BY r.currency_code ORDER BY r.rate_date);

-- Rates of the most recent bulletin, with the change against the previous one.
CREATE OR REPLACE VIEW v_latest_rates AS
SELECT
    c.code,
    c.name_az,
    c.category,
    c.unit,
    d.rate_date,
    d.rate_per_unit,
    d.prev_rate,
    d.change_pct
FROM v_daily_change AS d
JOIN currencies AS c ON c.code = d.currency_code
WHERE d.rate_date = (SELECT MAX(rate_date) FROM exchange_rates);

-- Smoothed trend over calendar windows: RANGE (not ROWS) makes "7 days" mean 7 days even
-- across weekends and holidays.
CREATE OR REPLACE VIEW v_moving_averages AS
SELECT
    currency_code,
    rate_date,
    rate_per_unit,
    ROUND(AVG(rate_per_unit) OVER (
        PARTITION BY currency_code ORDER BY rate_date
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW), 6)  AS ma_7d,
    ROUND(AVG(rate_per_unit) OVER (
        PARTITION BY currency_code ORDER BY rate_date
        RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW), 6) AS ma_30d
FROM exchange_rates;

-- Monthly candles: open / high / low / close.
CREATE OR REPLACE VIEW v_monthly_summary AS
SELECT
    currency_code,
    DATE_TRUNC('month', rate_date)::date                      AS month,
    (ARRAY_AGG(rate_per_unit ORDER BY rate_date))[1]          AS open,
    MAX(rate_per_unit)                                        AS high,
    MIN(rate_per_unit)                                        AS low,
    (ARRAY_AGG(rate_per_unit ORDER BY rate_date DESC))[1]     AS close,
    ROUND(AVG(rate_per_unit), 6)                              AS average,
    COUNT(*)                                                  AS bulletins
FROM exchange_rates
GROUP BY currency_code, DATE_TRUNC('month', rate_date);

-- Volatility = standard deviation of daily % changes, ranked within each year.
CREATE OR REPLACE VIEW v_yearly_volatility AS
SELECT
    d.currency_code,
    EXTRACT(YEAR FROM d.rate_date)::int                        AS year,
    c.category,
    ROUND(STDDEV_SAMP(d.change_pct), 4)                        AS volatility_pct,
    ROUND(MAX(ABS(d.change_pct)), 4)                           AS max_abs_change_pct,
    COUNT(*)                                                   AS observations,
    RANK() OVER (
        PARTITION BY EXTRACT(YEAR FROM d.rate_date)
        ORDER BY STDDEV_SAMP(d.change_pct) DESC NULLS LAST)    AS volatility_rank
FROM v_daily_change AS d
JOIN currencies AS c ON c.code = d.currency_code
WHERE d.change_pct IS NOT NULL
GROUP BY d.currency_code, EXTRACT(YEAR FROM d.rate_date), c.category;

-- Shocks: single-bulletin moves of 5 % or more (e.g. the 2015 AZN devaluations).
CREATE OR REPLACE VIEW v_shocks AS
SELECT
    d.rate_date,
    d.currency_code,
    c.category,
    d.prev_date,
    d.prev_rate,
    d.rate_per_unit,
    d.change_pct
FROM v_daily_change AS d
JOIN currencies AS c ON c.code = d.currency_code
WHERE ABS(d.change_pct) >= 5;

-- Cross rates through AZN: the price of one unit of each currency / metal in US dollars.
-- e.g. EUR -> EUR/USD, XAU -> gold price in USD per troy ounce.
CREATE OR REPLACE VIEW v_usd_cross_rates AS
SELECT
    r.rate_date,
    r.currency_code,
    ROUND(r.rate_per_unit / usd.rate_per_unit, 6) AS price_in_usd
FROM exchange_rates AS r
JOIN exchange_rates AS usd
  ON usd.rate_date = r.rate_date
 AND usd.currency_code = 'USD'
WHERE r.currency_code <> 'USD';

-- Pipeline monitoring.
CREATE OR REPLACE VIEW v_etl_runs AS
SELECT
    run_id,
    status,
    started_at,
    finished_at - started_at AS duration,
    date_from,
    date_to,
    days_requested,
    days_failed,
    bulletins_loaded,
    rows_inserted,
    rows_updated,
    rows_unchanged,
    rows_skipped,
    error_message
FROM etl_runs
ORDER BY run_id DESC;
