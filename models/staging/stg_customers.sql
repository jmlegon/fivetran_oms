select
    id as customer_sk,
    customer_id_c as customer_id,
    cust_last_name_c as last_name,
    cust_first_name_c as first_name,
    gender_c as gender,
    email_c as email,
    address_c as address,
    zipcode_c as zipcode,
    city_c as city,
    cust_birthdate_c as birthdate,
    _fivetran_synced as synced_at,
    _fivetran_deleted as is_deleted
from {{ source('salesforce','MY_CUSTOMER_C') }}
