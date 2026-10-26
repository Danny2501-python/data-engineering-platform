select *
from {{ source('warehouse_source', 'customers') }}
