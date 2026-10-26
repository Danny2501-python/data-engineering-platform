select *
from {{ source('warehouse_source', 'products') }}
