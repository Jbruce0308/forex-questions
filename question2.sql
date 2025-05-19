WITH daily_changes AS (
    SELECT
        currency_symbol,
        rate_date,
        exchange_rate,
        LAG(exchange_rate) OVER (PARTITION BY currency_symbol ORDER BY rate_date) AS prev_rate
    FROM exchange_rates
),
percent_changes AS (
    SELECT
        currency_symbol,
        100.0 * (exchange_rate - prev_rate) / NULLIF(prev_rate, 0) AS pct_change,
        exchange_rate
    FROM daily_changes
    WHERE prev_rate IS NOT NULL
),
stats AS (
    SELECT
        currency_symbol,
        STDDEV(pct_change) AS volatility,
        MAX(exchange_rate) / MIN(exchange_rate) AS range_ratio
    FROM percent_changes
    GROUP BY currency_symbol
)
-- clustering
SELECT *,
    CASE 
        WHEN volatility > 2 AND range_ratio > 1.5 THEN 'volatile_high_range'
        WHEN volatility < 1 AND range_ratio < 1.2 THEN 'stable_low_range'
        ELSE 'moderate'
    END AS cluster_label
FROM stats;
