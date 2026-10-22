-- Analytical views. Idempotent: the ETL applies this file at the start of every run.
-- All prices are rate_per_unit: AZN for ONE unit of the currency (or one troy ounce of metal),
-- so currencies quoted per 100 units (RUB, JPY, ...) are directly comparable.
--
-- Layers:  exchange_rates (raw, as published)
--            -> v_rate_checks  (redenomination-adjusted rate + spike detection)
--            -> v_clean_rates  (suspect rows removed; every analytical view reads from here)
--            -> v_suspect_rates (the removed rows, for data-quality review)

-- rate_new_units: the rate in today's units of a currency that CBAR kept under the same code
-- across a redenomination (BYN 2016, TMT 2009).
--
-- is_suspect flags isolated spikes in the source data, such as values shifted to the neighbouring
-- currency on 2007-07-04, or DKK = 1567 instead of 0.1567 on 2015-08-14. A rate is suspect when it
-- is more than 50 % away from BOTH the rate k bulletins before and the rate k bulletins after,
-- while those two agree within 25 %. k = 3 catches single spikes, and k = 10 catches plateaus of
-- up to 10 bulletins. A genuine level shift (the 2015 devaluations) fails the "neighbours agree"
-- condition, so it is kept.
CREATE OR REPLACE VIEW v_rate_checks AS
WITH adjusted AS (
    SELECT
        r.currency_code,
        r.rate_date,
        r.nominal,
        r.value,
        r.rate_per_unit,
        rd.effective_date AS redenomination_date,
        r.rate_per_unit * CASE WHEN r.rate_date < rd.effective_date THEN rd.factor ELSE 1 END
            AS rate_new_units
    FROM exchange_rates AS r
    LEFT JOIN currency_redenominations AS rd ON rd.currency_code = r.currency_code
),
neighbours AS (
    SELECT
        a.*,
        LAG(rate_new_units, 3)   OVER w AS before_3,
        LEAD(rate_new_units, 3)  OVER w AS after_3,
        LAG(rate_new_units, 10)  OVER w AS before_10,
        LEAD(rate_new_units, 10) OVER w AS after_10
    FROM adjusted AS a
    WINDOW w AS (PARTITION BY currency_code ORDER BY rate_date)
)
SELECT
    currency_code,
    rate_date,
    nominal,
    value,
    rate_per_unit,
    rate_new_units,
    redenomination_date,
    COALESCE(
        (ABS(rate_new_units / before_3 - 1) > 0.5
         AND ABS(rate_new_units / after_3 - 1) > 0.5
         AND ABS(after_3 / before_3 - 1) < 0.25)
        OR
        (ABS(rate_new_units / before_10 - 1) > 0.5
         AND ABS(rate_new_units / after_10 - 1) > 0.5
         AND ABS(after_10 / before_10 - 1) < 0.25),
        FALSE
    ) AS is_suspect,
    COALESCE(after_3, after_10, before_3, before_10) AS reference_rate
FROM neighbours;

CREATE OR REPLACE VIEW v_clean_rates AS
SELECT currency_code, rate_date, nominal, value, rate_per_unit, rate_new_units, redenomination_date
FROM v_rate_checks
WHERE NOT is_suspect;

CREATE OR REPLACE VIEW v_suspect_rates AS
SELECT
    currency_code,
    rate_date,
    nominal,
    value,
    rate_new_units                                             AS published_rate,
    reference_rate                                             AS typical_rate,
    ROUND(100 * (rate_new_units / reference_rate - 1), 1)      AS deviation_pct
FROM v_rate_checks
WHERE is_suspect;

-- Day-over-day movement. Windows run per currency over published bulletins only, so Monday
-- is compared with Friday; prev_date shows the gap.
-- rate_per_unit and prev_rate are shown as published. change_pct compares both rates in the
-- new units of a redenominated currency, so a redenomination is not reported as a +1,000,000 % move.
CREATE OR REPLACE VIEW v_daily_change AS
SELECT
    currency_code,
    rate_date,
    rate_per_unit,
    LAG(rate_date)     OVER w AS prev_date,
    LAG(rate_per_unit) OVER w AS prev_rate,
    ROUND(100 * (rate_new_units / LAG(rate_new_units) OVER w - 1), 4) AS change_pct,
    COALESCE(LAG(rate_date) OVER w < redenomination_date AND rate_date >= redenomination_date,
             FALSE) AS across_redenomination
FROM v_clean_rates
WINDOW w AS (PARTITION BY currency_code ORDER BY rate_date);

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
FROM v_clean_rates;

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
FROM v_clean_rates
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
FROM v_clean_rates AS r
JOIN v_clean_rates AS usd
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
