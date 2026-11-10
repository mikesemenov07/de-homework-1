with source_totals as (
    select
        count(*) as orders_count,
        coalesce(sum(orders.total_amount), 0) as revenue
    from {{ source('shop', 'orders') }} as orders
    where orders.status = 'paid'
      and exists (
          select 1
          from {{ source('shop', 'payments') }} as payments
          where payments.order_id = orders.order_id
            and payments.payment_status = 'success'
            and payments.amount >= 0)),
mart_totals as (
    select
        count(*) as orders_count,
        coalesce(sum(order_amount), 0) as revenue
    from {{ ref('fct_orders') }}
    where is_confirmed)

select
    source_totals.orders_count as source_orders_count,
    mart_totals.orders_count as mart_orders_count,
    source_totals.revenue as source_revenue,
    mart_totals.revenue as mart_revenue
from source_totals
cross join mart_totals
where source_totals.orders_count <> mart_totals.orders_count
   or source_totals.revenue <> mart_totals.revenue