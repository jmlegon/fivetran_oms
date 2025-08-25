{{ config(materialized='view') }}

select
  ITEM_ID,
  ITEM_NAME,
  ITEM_LONG_DESC,
  ITEM_UPC,
  WARRANTY,
  UNIT_PRICE,
  UNIT_COST,
  SUBCAT_ID,
  SUPPLIER_ID,
  BRAND_ID
from {{ source('postgres_public','PRODUCT') }}
