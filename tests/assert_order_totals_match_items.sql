with item_totals as (
    select
        order_id,
        sum(line_amount) as items_amount
    from {{ ref('fct_order_items') }}
    group by order_id)

select
    coalesce(orders.order_id, items.order_id) as order_id,
    orders.order_amount,
    items.items_amount
from {{ ref('fct_orders') }} as orders
full outer join item_totals as items
    on orders.order_id = items.order_id
where orders.order_amount is distinct from items.items_amount