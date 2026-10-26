select *
from {{ source('warehouse_source', 'order_items') }}
