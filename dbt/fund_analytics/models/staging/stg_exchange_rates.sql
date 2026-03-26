-- models/staging/stg_exchange_rates.sql
-- Clean and deduplicate exchange rate data
 
WITH source AS (
    SELECT * FROM {{ source('raw', 'exchange_rates') }}
),
 
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, currency
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM source
)
 
SELECT
    date AS rate_date,
    currency,
    CAST(rate AS FLOAT64) AS exchange_rate,
    ingested_at
FROM deduplicated
WHERE row_num = 1
