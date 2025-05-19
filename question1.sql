WITH ranked_rates AS (
    SELECT
        currency_symbol,
        rate_date,
        exchange_rate,
        LAG(exchange_rate) OVER (PARTITION BY currency_symbol ORDER BY rate_date) AS prev_rate
    FROM exchange_rates
),
diffs AS (
    SELECT *,
        CASE WHEN exchange_rate > prev_rate THEN 1 ELSE 0 END AS is_up
    FROM ranked_rates
),
streaks AS (
    SELECT *,
        SUM(CASE WHEN is_up = 0 THEN 1 ELSE 0 END)
        OVER (PARTITION BY currency_symbol ORDER BY rate_date) AS streak_group
    FROM diffs
),
grouped AS (
    SELECT
        currency_symbol,
        streak_group,
        COUNT(*) AS streak_len,
        MIN(rate_date) AS start_date,
        MAX(rate_date) AS end_date,
        MAX(exchange_rate)/MIN(exchange_rate) - 1 AS perc_change
    FROM streaks
    WHERE is_up = 1
    GROUP BY currency_symbol, streak_group
    HAVING COUNT(*) >= 2
),
agg_metrics AS (
    SELECT
        currency_symbol,
        AVG(streak_len) AS avg_cons_pos_days,
        AVG(perc_change) * 100 AS avg_cons_perc_change
    FROM grouped
    GROUP BY currency_symbol
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY avg_cons_pos_days DESC) AS avg_cons_pos_days_rank,
        RANK() OVER (ORDER BY avg_cons_perc_change DESC) AS avg_cons_perc_change_rank
    FROM agg_metrics
)
SELECT * FROM ranked
WHERE avg_cons_pos_days_rank <= 5 OR avg_cons_perc_change_rank <= 5
LIMIT 10;