{{ config(distributed_replicated=true) }}

select
    product.product_id,
    product.product_name,
    product.brand,
    category.category_id,
    product.category_name,
    category.parent_category_name,
    category.root_category_name
from {{ ref('stg_shop__products') }} as product
left join {{ ref('int_product_categories') }} as category
    on product.category_name = category.category_name