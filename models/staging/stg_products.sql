{{ config(materialized='view') }}

with product_src as (
    select
        ITEM_ID        as product_id,
        ITEM_NAME      as product_name,
        ITEM_LONG_DESC as product_long_desc,
        ITEM_UPC       as upc,
        WARRANTY,
        UNIT_PRICE     as unit_price,
        UNIT_COST      as unit_cost,
        SUBCAT_ID      as subcat_id,
        SUPPLIER_ID    as supplier_id,
        BRAND_ID       as brand_id
    from {{ source('postgres_public', 'PRODUCT') }}
)

select
    p.product_id,
    p.product_name,
    p.product_long_desc,
    p.upc,
    p.warranty,
    p.unit_price,
    p.unit_cost,
    p.subcat_id,
    coalesce(sub.subcat_name, null)    as sub_category,
    p.brand_id,
    coalesce(b.brand_name, null)       as brand,
    p.supplier_id,
    coalesce(s.supplier_name, null)    as supplier_name
from product_src p
left join {{ source('postgres_public', 'LU_SUBCATEGORY') }} sub
    on p.subcat_id = sub.subcat_id
left join {{ source('postgres_public', 'LU_BRAND') }} b
    on p.brand_id = b.brand_id
left join {{ source('postgres_public', 'LU_SUPPLIER') }} s
    on p.supplier_id = s.supplier_id
