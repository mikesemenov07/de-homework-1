{{ config(distributed_replicated=true) }}

select
    customer.customer_id,
    customer.registered_at,
    customer.registration_date,
    customer.city,
    customer.acquisition_channel,
    purchases.first_confirmed_order_id,
    purchases.first_confirmed_at,
    coalesce(purchases.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(purchases.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(purchases.confirmed_orders_count, 0) > 0 as is_buyer
from {{ ref('stg_shop__customers') }} as customer
left join {{ ref('int_customer_orders') }} as purchases
    on customer.customer_id = purchases.customer_id