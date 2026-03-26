WITH source AS (
    SELECT * FROM {{ source('raw', 'fund_prices') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, Date
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM source
)

SELECT
    Date AS trade_date,
    ticker,
    CAST(Open AS FLOAT64) AS open_price,
    CAST(High AS FLOAT64) AS high_price,
    CAST(Low AS FLOAT64) AS low_price,
    CAST(Close AS FLOAT64) AS close_price,
    CAST(Close AS FLOAT64) AS adj_close,
    CAST(Volume AS INT64) AS volume,
    ingested_at
FROM deduplicated
WHERE row_num = 1
