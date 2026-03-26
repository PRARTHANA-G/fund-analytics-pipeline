-- models/marts/fct_daily_returns.sql
-- Calculate daily returns and rolling averages
 
WITH prices AS (
    SELECT * FROM {{ ref('stg_fund_prices') }}
),
 
with_returns AS (
    SELECT
        trade_date,
        ticker,
        adj_close,
        LAG(adj_close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS prev_close,
        volume
    FROM prices
)
 
SELECT
    trade_date,
    ticker,
    adj_close,
    prev_close,
    SAFE_DIVIDE(
        (adj_close - prev_close), prev_close
    ) * 100 AS daily_return_pct,
    volume,
    AVG(adj_close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d,
    AVG(adj_close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30d
FROM with_returns
WHERE prev_close IS NOT NULL
