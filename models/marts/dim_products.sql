{{ config(
    materialized='table',
    schema='STAR_SCHEMA',
    sort='product_key'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    current_timestamp as start_date,
    null as end_date,
    true as is_current
from {{ ref('stg_products') }}

