select
    item.order_item_id,
    item.order_id,
    item.product_id,
    orders.customer_id,
    orders.order_date,
    item.quantity,
    item.unit_price,
    item.discount_amount,
    item.line_amount,
    orders.order_status,
    orders.is_confirmed
from {{ ref('stg_shop__order_items') }} as item
join {{ ref('int_orders_with_payments') }} as orders
    on item.order_id = orders.order_id
where not orders.is_test_order