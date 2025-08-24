select * from {{ source('postgres','PRODUCT_DIM') }}
