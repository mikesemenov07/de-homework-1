select
    generate_series(
        min(order_date)::timestamp,
        max(order_date)::timestamp,
        interval '1 day')::date as date_day
from {{ ref('stg_shop__orders') }}