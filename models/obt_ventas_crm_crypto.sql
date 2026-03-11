{{ config(materialized='table') }}

WITH pipeline AS (
    SELECT * FROM {{ source('crm_raw', 'sales_pipeline') }}
),
accounts AS (
    SELECT * FROM {{ source('crm_raw', 'accounts') }}
),
products AS (
    SELECT * FROM {{ source('crm_raw', 'products') }}
),
crypto_prices AS (
    -- CORRECCIÓN: Usamos date_added y extraemos el precio de la estructura 'quote'
    -- Basado en el error de dbt: Candidate bindings: "date_added", "quote", etc.
    SELECT 
        CAST(date_added AS DATE) as crypto_date,
        -- Asumiendo que quote es un JSON/Struct con el precio en USD para BTC
        CAST(quote->'USD'->>'price' AS NUMERIC) as btc_price
    FROM {{ source('external_data', 'crypto') }}
    WHERE name = 'Bitcoin' OR symbol = 'BTC'
)

SELECT 
    p.opportunity_id,
    p.sales_agent,
    p.account,
    p.product,
    p.deal_stage,
    p.close_value as value_usd,
    p.close_date,
    a.sector,
    a.revenue as account_annual_revenue,
    prod.series as product_series,
    -- Calculamos el valor de la venta en BTC al momento del cierre
    COALESCE(p.close_value / NULLIF(c.btc_price, 0), 0) as value_in_btc,
    c.btc_price as btc_price_at_close
FROM pipeline p
LEFT JOIN accounts a ON p.account = a.account
LEFT JOIN products prod ON p.product = prod.product
LEFT JOIN crypto_prices c ON CAST(p.close_date AS DATE) = c.crypto_date
WHERE p.deal_stage = 'Won'