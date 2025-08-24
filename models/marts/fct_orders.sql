select
    o.order_id,
    o.item_id,
    o.qty_sold,
    o.unit_price,
    o.unit_cost,
    o.discount,
    o.time_id,
    o.customer_id
from {{ ref('stg_orders') }} o
