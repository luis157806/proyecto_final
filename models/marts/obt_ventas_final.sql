{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('stg_sales_pipeline') }}
), -- <--- Asegúrate de que esta coma esté aquí

accounts as (
    select * from {{ ref('stg_accounts') }}
), -- <--- Y esta coma también

final_join as (
    select
        s.id_oportunidad,
        s.cuenta,
        s.producto,
        s.valor_cierre,
        s.fecha_contacto,
        a.sector as sector_cuenta,
        a.ingresos_anuales as ingresos_empresa
    from sales s
    left join accounts a on s.cuenta = a.nombre_cuenta
)

select * from final_join