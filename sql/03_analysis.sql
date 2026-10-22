-- Analytical queries for the report. Not applied automatically; run them with:
--   docker compose exec -T postgres psql -U etl -d cbar < sql/03_analysis.sql
-- or open the file in any SQL client (DBeaver, pgAdmin, VS Code).

\echo '=== 1. Latest official rates: major currencies and gold ==='
SELECT code, name_az, rate_date, rate_per_unit, change_pct
FROM v_latest_rates
WHERE code IN ('USD', 'EUR', 'GBP', 'RUB', 'TRY', 'GEL', 'CNY', 'XAU')
ORDER BY code;


\echo '=== 2. The 10 largest one-day moves of the US dollar (2015 devaluations) ==='
SELECT rate_date, prev_date, prev_rate, rate_per_unit, change_pct
FROM v_daily_change
WHERE currency_code = 'USD' AND change_pct IS NOT NULL
ORDER BY ABS(change_pct) DESC
LIMIT 10;


\echo '=== 3. USD/AZN by year: average, range and year-over-year change of the closing rate ==='
WITH yearly AS (
    SELECT
        EXTRACT(YEAR FROM rate_date)::int                         AS year,
        ROUND(AVG(rate_per_unit), 4)                              AS average,
        MIN(rate_per_unit)                                        AS low,
        MAX(rate_per_unit)                                        AS high,
        (ARRAY_AGG(rate_per_unit ORDER BY rate_date DESC))[1]     AS close
    FROM exchange_rates
    WHERE currency_code = 'USD'
    GROUP BY 1
)
SELECT
    year, average, low, high, close,
    ROUND(100 * (close / LAG(close) OVER (ORDER BY year) - 1), 2) AS close_change_pct
FROM yearly
ORDER BY year;


\echo '=== 4. Most volatile currencies over the last 365 days (metals excluded) ==='
SELECT
    d.currency_code,
    c.name_az,
    ROUND(STDDEV_SAMP(d.change_pct), 4)   AS volatility_pct,
    ROUND(MIN(d.change_pct), 2)           AS worst_day_pct,
    ROUND(MAX(d.change_pct), 2)           AS best_day_pct
FROM v_daily_change AS d
JOIN currencies AS c ON c.code = d.currency_code
WHERE c.category = 'currency'
  AND d.rate_date > (SELECT MAX(rate_date) FROM exchange_rates) - INTERVAL '365 days'
GROUP BY d.currency_code, c.name_az
ORDER BY volatility_pct DESC
LIMIT 10;


\echo '=== 5. Year-to-date performance against AZN (positive = currency got more expensive) ==='
WITH bounds AS (
    SELECT
        MAX(rate_date)                                     AS last_date,
        MIN(rate_date) FILTER (
            WHERE rate_date >= DATE_TRUNC('year', (SELECT MAX(rate_date) FROM exchange_rates))
        )                                                  AS first_date
    FROM exchange_rates
)
SELECT
    s.currency_code,
    s.rate_per_unit                                          AS start_rate,
    e.rate_per_unit                                          AS current_rate,
    ROUND(100 * (e.rate_per_unit / s.rate_per_unit - 1), 2)  AS ytd_pct,
    RANK() OVER (ORDER BY e.rate_per_unit / s.rate_per_unit DESC) AS rank
FROM bounds AS b
JOIN exchange_rates AS s ON s.rate_date = b.first_date
JOIN exchange_rates AS e ON e.rate_date = b.last_date AND e.currency_code = s.currency_code
ORDER BY ytd_pct DESC;


\echo '=== 6. EUR/USD cross rate (derived through AZN), yearly average ==='
SELECT
    EXTRACT(YEAR FROM rate_date)::int  AS year,
    ROUND(AVG(price_in_usd), 4)        AS eur_usd_avg,
    MIN(price_in_usd)                  AS low,
    MAX(price_in_usd)                  AS high
FROM v_usd_cross_rates
WHERE currency_code = 'EUR'
GROUP BY 1
ORDER BY 1;


\echo '=== 7. Gold: price per troy ounce in AZN and USD at each year end ==='
SELECT DISTINCT ON (EXTRACT(YEAR FROM r.rate_date))
    EXTRACT(YEAR FROM r.rate_date)::int  AS year,
    r.rate_date,
    r.rate_per_unit                      AS azn_per_oz,
    x.price_in_usd                       AS usd_per_oz
FROM exchange_rates AS r
JOIN v_usd_cross_rates AS x
  ON x.rate_date = r.rate_date AND x.currency_code = r.currency_code
WHERE r.currency_code = 'XAU'
ORDER BY EXTRACT(YEAR FROM r.rate_date), r.rate_date DESC;


\echo '=== 8. Currencies that CBAR stopped publishing ==='
SELECT code, name_az, first_seen, last_seen
FROM currencies
WHERE last_seen < (SELECT MAX(rate_date) FROM exchange_rates)
ORDER BY last_seen;


\echo '=== 9. Currencies whose quoting nominal changed (why rate_per_unit is needed) ==='
SELECT
    currency_code,
    ARRAY_AGG(DISTINCT nominal ORDER BY nominal) AS nominals,
    MIN(rate_date)                               AS first_date,
    MAX(rate_date)                               AS last_date
FROM exchange_rates
GROUP BY currency_code
HAVING COUNT(DISTINCT nominal) > 1;


\echo '=== 10. Data quality: the largest gaps between consecutive bulletins ==='
WITH bulletins AS (
    SELECT DISTINCT rate_date FROM exchange_rates
)
SELECT
    LAG(rate_date) OVER (ORDER BY rate_date)               AS previous_bulletin,
    rate_date                                              AS next_bulletin,
    rate_date - LAG(rate_date) OVER (ORDER BY rate_date)   AS gap_days
FROM bulletins
ORDER BY gap_days DESC NULLS LAST
LIMIT 10;


\echo '=== 11. Data quality: bulletins per year and average currencies per bulletin ==='
SELECT
    EXTRACT(YEAR FROM rate_date)::int                      AS year,
    COUNT(DISTINCT rate_date)                              AS bulletins,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT rate_date), 1) AS avg_rates_per_bulletin
FROM exchange_rates
GROUP BY 1
ORDER BY 1;


\echo '=== 12. Pipeline run history ==='
SELECT run_id, status, duration, date_from, date_to, bulletins_loaded,
       rows_inserted, rows_updated, rows_unchanged, rows_skipped
FROM v_etl_runs
LIMIT 10;
