{{ config(
    materialized='table',
    schema='STAR_SCHEMA',
    sort='customer_key'
) }}

with ranked as (
    select
        CUSTOMER_ID as customer_id_source,
        CUST_LAST_NAME,
        CUST_FIRST_NAME,
        GENDER,
        EMAIL,
        ADDRESS,
        ZIPCODE,
        CITY,
        CUST_BIRTHDATE,
        _FIVETRAN_SYNCED,
        row_number() over (
            partition by CUSTOMER_ID
            order by _FIVETRAN_SYNCED
        ) as version_num
    from {{ ref('stgustomers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id_source','version_num']) }} as customer_key,
    customer_id_source,
    cust_last_name,
    cust_first_name,
    gender,
    email,
    address,
    zipcode,
    city,
    cust_birthdate,
    version_num,
    current_timestamp as start_date,
    null as end_date,
    true as isurrent
from ranked
