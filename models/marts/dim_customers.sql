{{ config(
    materialized='table',
    schema='STAR_SCHEMA',
    sort='customer_key'
) }}

with ranked as (
    select
        CUSTOMER_ID as customer_id_source,
        LAST_NAME,
        FIRST_NAME,
        GENDER,
        EMAIL,
        ADDRESS,
        ZIPCODE,
        CITY,
        BIRTHDATE,
        SYNCED_AT,
        row_number() over (
            partition by CUSTOMER_ID
            order by SYNCED_AT
        ) as version_num
    from {{ ref('stg_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id_source','version_num']) }} as customer_key,
    customer_id_source,
    last_name,
    first_name,
    gender,
    email,
    address,
    zipcode,
    city,
    birthdate,
    version_num,
    current_timestamp as start_date,
    null as end_date,
    true as isurrent
from ranked
