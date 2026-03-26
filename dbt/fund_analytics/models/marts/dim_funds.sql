-- models/marts/dim_funds.sql
-- Fund dimension table with metadata
 
SELECT
    ticker,
    CASE ticker
        WHEN 'SPY' THEN 'SPDR S&P 500 ETF'
        WHEN 'IVV' THEN 'iShares Core S&P 500'
        WHEN 'EEM' THEN 'iShares MSCI Emerging Markets'
        WHEN 'AGG' THEN 'iShares Core US Aggregate Bond'
        WHEN 'VGK' THEN 'Vanguard FTSE Europe ETF'
    END AS fund_name,
    CASE ticker
        WHEN 'SPY' THEN 'US Large Cap Equity'
        WHEN 'IVV' THEN 'US Large Cap Equity'
        WHEN 'EEM' THEN 'Emerging Markets Equity'
        WHEN 'AGG' THEN 'US Aggregate Bond'
        WHEN 'VGK' THEN 'Europe Equity'
    END AS asset_class,
    CASE ticker
        WHEN 'AGG' THEN 'Fixed Income'
        ELSE 'Equity'
    END AS instrument_type
FROM UNNEST(['SPY','IVV','EEM','AGG','VGK']) AS ticker
