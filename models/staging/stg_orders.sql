{{ config(materialized='view') }}

with oh as (
    select
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        SHIP_DATE,
        ORDER_AMT,
        ORDER_COST,
        QTY_SOLD,
        FREIGHT,
        PYMT_TYPE,
        GROSS_DOLLAR_SALES
    from {{ source('postgres_public', 'ORDER_HEADER') }}
),
od as (
    select
        ORDER_ID,
        ITEM_ID,
        PROMOTION_ID,
        QTY_SOLD,
        UNIT_PRICE,
        UNIT_COST,
        DISCOUNT,
        ORDER_DATE
    from {{ source('postgres_public', 'ORDER_DETAIL') }}
)

select
    od.ORDER_ID,
    od.ITEM_ID,
    oh.CUSTOMER_ID,
    od.QTY_SOLD,
    od.UNIT_PRICE,
    od.UNIT_COST,
    od.DISCOUNT,
    oh.ORDER_DATE,
    oh.SHIP_DATE,
    oh.PYMT_TYPE,
    oh.GROSS_DOLLAR_SALES,
    oh.FREIGHT
from od
left join oh using (ORDER_ID)
