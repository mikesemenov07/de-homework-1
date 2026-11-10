{% test non_negative(model, column_name) %}

select *
from {{ model }}
where {{ adapter.quote(column_name) }} < 0

{% endtest %}