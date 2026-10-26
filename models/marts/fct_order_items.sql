select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    o.order_date,
    o.status as order_status,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    round(
        oi.quantity::numeric * oi.unit_price::numeric,
        2
    )::numeric(18, 2) as gross_item_amount
from {{ ref('stg_order_items') }} as oi
inner join {{ ref('stg_orders') }} as o
    on oi.order_id = o.order_id
