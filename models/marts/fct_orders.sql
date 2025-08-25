{{ config(
    materialized='table',
    schema='STAR_SCHEMA'
) }}

select
    d.ORDER_ID::int as ORDER_ID,
    d.ITEM_ID::int as ITEM_ID,
    d.QTY_SOLD,
    d.UNIT_PRICE,
    d.UNIT_COST,
    d.DISCOUNT,
    t.TIME_ID,
    h.CUSTOMER_ID::int as CUSTOMER_ID,
    p.product_key
from {{ source('postgres_public','ORDER_DETAIL') }} d
join {{ source('postgres_public','ORDER_HEADER') }} h 
    on d.ORDER_ID = h.ORDER_ID
left join {{ ref('dim_time') }} t 
    on cast(h.ORDER_DATE as date) = t.date
left join {{ ref('dim_products') }} p
    on d.ITEM_ID = p.product_id
left join {{ ref('dim_customers') }} c
    on h.CUSTOMER_ID = c.customer_id
