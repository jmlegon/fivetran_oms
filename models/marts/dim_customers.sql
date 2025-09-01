{{ config(
    materialized='incremental',
    schema='STAR_SCHEMA',
    unique_key='customer_key'
) }}

with source as (
    select
        id as customer_sk,
        customer_id,
        last_name,
        first_name,
        gender,
        email,
        address,
        zipcode,
        city,
        birthdate,
        synced_at,
        is_deleted
    from {{ ref('stg_customers') }}
),

current_dim as (
    select *
    from {{ this }}
    where is_current = true
)

-- Cas 1 : nouveaux enregistrements ou modifications
select
    {{ dbt_utils.generate_surrogate_key(['s.customer_id','s.synced_at']) }} as customer_key,
    s.customer_id,
    s.last_name,
    s.first_name,
    s.gender,
    s.email,
    s.address,
    s.zipcode,
    s.city,
    s.birthdate,
    s.synced_at as effective_from,
    null as effective_to,
    true as is_current
from source s
left join current_dim d
    on s.customer_id = d.customer_id
where d.customer_id is null  -- nouveau client
   or (
        d.is_current
        and (
            s.last_name  <> d.last_name
         or s.first_name <> d.first_name
         or s.gender     <> d.gender
         or s.email      <> d.email
         or s.address    <> d.address
         or s.zipcode    <> d.zipcode
         or s.city       <> d.city
         or s.birthdate  <> d.birthdate
        )
    )
   or (s.is_deleted = true and d.is_current = true)

union all

-- Cas 2 : anciennes versions qu’on conserve et qu’on clôture si nécessaire
select
    d.customer_key,
    d.customer_id,
    d.last_name,
    d.first_name,
    d.gender,
    d.email,
    d.address,
    d.zipcode,
    d.city,
    d.birthdate,
    d.effective_from,
    case 
        when s.is_deleted = true
        then coalesce(d.effective_to, current_timestamp)
        when (
            s.customer_id = d.customer_id
            and (
                s.last_name  <> d.last_name
             or s.first_name <> d.first_name
             or s.gender     <> d.gender
             or s.email      <> d.email
             or s.address    <> d.address
             or s.zipcode    <> d.zipcode
             or s.city       <> d.city
             or s.birthdate  <> d.birthdate
            )
        )
        then coalesce(d.effective_to, current_timestamp)
        else d.effective_to
    end as effective_to,
    case 
        when s.is_deleted = true then false
        when (
            s.customer_id = d.customer_id
            and (
                s.last_name  <> d.last_name
             or s.first_name <> d.first_name
             or s.gender     <> d.gender
             or s.email      <> d.email
             or s.address    <> d.address
             or s.zipcode    <> d.zipcode
             or s.city       <> d.city
             or s.birthdate  <> d.birthdate
            )
        )
        then false
        else d.is_current
    end as is_current
from current_dim d
left join source s
    on s.customer_id = d.customer_id
