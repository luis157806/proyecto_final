{{ config(materialized='view') }}

with source as (
    select * from {{ source('airbyte_raw', 'accounts') }}
),

renamed as (
    select
        cast(account as string) as nombre_cuenta,
        cast(sector as string) as sector,
        cast(revenue as float) as ingresos_anuales
    from source
)

select * from renamed