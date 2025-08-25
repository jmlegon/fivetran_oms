{{ config(
    materialized='table',
    schema='STAR_SCHEMA',
    sort='DATE_DAY'
) }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="'2020-01-01'",
        end_date="'2030-12-31'"
    ) }}

),

dim_time as (
    select
        row_number() over (order by date_day) as TIME_ID,  -- ✅ ID unique pour la dimension
        date_day as date,
        extract(year from date_day)::int as year,
        extract(month from date_day)::int as month,
        extract(day from date_day)::int as day,
        extract(week from date_day)::int as week,
        extract(quarter from date_day)::int as quarter,
        case 
            when dayofweekiso(date_day) in (6,7) then true
            else false
        end as is_weekend,
        to_char(date_day, 'DY') as day_name,
        to_char(date_day, 'MON') as month_name,
        date_trunc('month', date_day) as month_start,
        date_trunc('quarter', date_day) as quarter_start,
        date_trunc('year', date_day) as year_start
    from date_spine
)

select * from dim_time
