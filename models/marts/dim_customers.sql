{{ config(
    materialized='incremental',
    schema='STAR_SCHEMA',
    unique_key='customer_key'
) }}

with source as (
    select
        customer_id,       -- clé métier
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
)

{% if is_incremental() %}

, current_dim as (
    select *
    from {{ this }}
    where is_current = true
)

-- Incrémental : on insère les nouvelles versions si changement
select
    {{ dbt_utils.generate_surrogate_key(['s.customer_id','s.synced_at']) }} as customer_key,
    s.customer_id as customer_id_source,
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
    on s.customer_id = d.customer_id_source
where d.customer_id_source is null  -- nouveau client
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

-- Anciennes versions conservées / clôturées
select
    d.customer_key,
    d.customer_id_source,
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
            s.customer_id = d.customer_id_source
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
            s.customer_id = d.customer_id_source
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
    on s.customer_id = d.customer_id_source

{% else %}

-- Full refresh : état initial
select
    {{ dbt_utils.generate_surrogate_key(['s.customer_id','s.synced_at']) }} as customer_key,
    s.customer_id as customer_id_source,
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
where s.is_deleted = false

{% endif %}
