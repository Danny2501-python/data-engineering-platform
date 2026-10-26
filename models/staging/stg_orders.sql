select *
from {{ source('warehouse_source', 'orders') }}
