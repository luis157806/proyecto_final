{{ config(materialized='view') }}

with source as (
    select * from {{ source('airbyte_raw', 'sales_pipeline') }}
),

renamed as (
    select
        cast(opportunity_id as string) as id_oportunidad,
        cast(account as string) as cuenta,
        cast(product as string) as producto,
        cast(close_value as float) as valor_cierre,
        -- Cambiamos created_date por engage_date o close_date
        cast(engage_date as date) as fecha_contacto 
    from source
)

select * from renamed