with order_item_agg as (
    select
        order_id,
        count(*)::integer as item_count,
        coalesce(sum(quantity), 0)::bigint as total_quantity,
        round(
            coalesce(
                sum(quantity::numeric * unit_price::numeric),
                0
            ),
            2
        ) as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
),

payment_agg as (
    select
        order_id,

        count(*)::integer as payment_count,

        count(*) filter (
            where lower(trim(status)) = 'success'
        )::integer as successful_payment_count,

        round(
            coalesce(
                sum(amount::numeric) filter (
                    where lower(trim(status)) = 'success'
                ),
                0
            ),
            2
        ) as successful_payment_amount,

        count(*) filter (
            where lower(trim(status)) = 'pending'
        )::integer as pending_payment_count,

        round(
            coalesce(
                sum(amount::numeric) filter (
                    where lower(trim(status)) = 'pending'
                ),
                0
            ),
            2
        ) as pending_payment_amount,

        count(*) filter (
            where lower(trim(status)) = 'refunded'
        )::integer as refunded_payment_count,

        round(
            coalesce(
                sum(amount::numeric) filter (
                    where lower(trim(status)) = 'refunded'
                ),
                0
            ),
            2
        ) as refunded_payment_amount,

        string_agg(
            distinct status,
            ', ' order by status
        ) as payment_statuses

    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status as order_status,

    coalesce(i.item_count, 0)::integer as item_count,
    coalesce(i.total_quantity, 0)::bigint as total_quantity,

    coalesce(
        i.order_amount, 0::numeric
    )::numeric(18, 2) as order_amount,

    coalesce(p.payment_count, 0)::integer
        as payment_count,

    coalesce(p.successful_payment_count, 0)::integer
        as successful_payment_count,

    coalesce(
        p.successful_payment_amount, 0::numeric
    )::numeric(18, 2) as successful_payment_amount,

    coalesce(p.pending_payment_count, 0)::integer
        as pending_payment_count,

    coalesce(
        p.pending_payment_amount, 0::numeric
    )::numeric(18, 2) as pending_payment_amount,

    coalesce(p.refunded_payment_count, 0)::integer
        as refunded_payment_count,

    coalesce(
        p.refunded_payment_amount, 0::numeric
    )::numeric(18, 2) as refunded_payment_amount,

    p.payment_statuses

from {{ ref('stg_orders') }} as o

left join order_item_agg as i
    on o.order_id = i.order_id

left join payment_agg as p
    on o.order_id = p.order_id
