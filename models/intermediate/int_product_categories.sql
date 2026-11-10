with categories as (select * from {{ ref('stg_shop__product_categories') }})

select
    category.category_id,
    category.category_name,
    parent.category_name as parent_category_name,
    coalesce(
        grandparent.category_name,
        parent.category_name,
        category.category_name) 
    as root_category_name
from categories as category
left join categories as parent
    on category.parent_category_id = parent.category_id
left join categories as grandparent
    on parent.parent_category_id = grandparent.category_id