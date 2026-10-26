select *
from {{ source('warehouse_source', 'payments') }}
