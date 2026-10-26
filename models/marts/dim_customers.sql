select
    customer_id,
    full_name,
    email,
    city,
    created_at,
    updated_at
from {{ ref('stg_customers') }}
