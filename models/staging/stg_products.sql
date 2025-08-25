select * from {{ source('postgres_public','PRODUCT_DIM') }}
