with ranked as (
    select
        CUSTOMER_ID_C as customer_id_source,
        CUST_LAST_NAME_C,
        CUST_FIRST_NAME_C,
        GENDER_C,
        EMAIL_C,
        ADDRESS_C,
        ZIPCODE_C,
        CITY_C,
        CUST_BIRTHDATE_C,
        _FIVETRAN_SYNCED,
        row_number() over (
            partition by CUSTOMER_ID_C
            order by _FIVETRAN_SYNCED
        ) as version_num
    from {{ ref('stg_customers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['customer_id_source','version_num']) }} as customer_key,
    customer_id_source,
    cust_last_name_c,
    cust_first_name_c,
    gender_c,
    email_c,
    address_c,
    zipcode_c,
    city_c,
    cust_birthdate_c,
    version_num,
    current_timestamp as start_date,
    null as end_date,
    true as is_current
from ranked
