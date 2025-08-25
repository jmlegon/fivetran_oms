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
    sc.subcat_desc   as sub_category,    -- libellé sous-catégorie (via LU_SUBCATEG)
    sc.category_id   as category_id,     -- clé étrangère vers category
    c.category_desc  as category_name,         -- libellé catégorie (via LU_CATEGORY)
    p.brand_id,
    b.brand_name     as brand_name,            -- libellé marque
    p.supplier_id,
    s.supplier_name  as supplier_name     -- libellé fournisseur
from product_src p
left join {{ source('postgres_public', 'LU_SUBCATEG') }} sc
    on p.subcat_id = sc.subcat_id
left join {{ source('postgres_public', 'LU_CATEGORY') }} c
    on sc.category_id = c.category_id
left join {{ source('postgres_public', 'LU_BRAND') }} b
    on p.brand_id = b.brand_id
left join {{ source('postgres_public', 'LU_SUPPLIER') }} s
    on p.supplier_id = s.supplier_id
