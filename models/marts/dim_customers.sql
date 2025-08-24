with base as (
    select * from {{ ref('stg_customers') }}
),
scd2 as (
    select
        customer_id,
        last_name,
        first_name,
        gender,
        email,
        address,
        zipcode,
        city,
        birthdate,
        synced_at as start_date,
        lead(synced_at) over (partition by customer_id order by synced_at) as end_date,
        case when lead(synced_at) over (partition by customer_id order by synced_at) is null then true else false end as is_current
    from base
    where is_deleted = false
)
select * from scd2
