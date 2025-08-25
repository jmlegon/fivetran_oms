{{ config(
    materialized='table',
    schema='STAR_SCHEMA',
    sort='product_key'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id::int                as product_id,
    product_name,
    sub_category,
    brand,
    supplier_id::int                as supplier_id,
    supplier_name,
    unit_price,
    unit_cost,
    product_long_desc,
    upc,
    current_timestamp()             as start_date,
    null                            as end_date,
    true                            as is_current
from {{ ref('stg_products') }}
