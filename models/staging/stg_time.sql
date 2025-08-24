select * from {{ source('postgres','TIME_DIM') }}
