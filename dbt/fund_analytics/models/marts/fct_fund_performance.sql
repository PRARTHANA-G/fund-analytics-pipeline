-- models/marts/fct_fund_performance.sql
-- Monthly aggregated performance metrics
 
WITH daily AS (
    SELECT * FROM {{ ref('fct_daily_returns') }}
)
 
SELECT
    ticker,
    DATE_TRUNC(trade_date, MONTH) AS month,
    COUNT(*) AS trading_days,
    AVG(daily_return_pct) AS avg_daily_return,
    STDDEV(daily_return_pct) AS volatility,
    MIN(adj_close) AS month_low,
    MAX(adj_close) AS month_high,
    SUM(volume) AS total_volume,
    SAFE_DIVIDE(
        AVG(daily_return_pct),
        STDDEV(daily_return_pct)
    ) AS return_risk_ratio
FROM daily
GROUP BY ticker, DATE_TRUNC(trade_date, MONTH)
