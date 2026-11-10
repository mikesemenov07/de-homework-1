{% macro postgres__create_table_as(temporary, relation, sql) %}
    {% set distributed_by = config.get('distributed_by') %}
    {% set replicated = config.get('distributed_replicated', false) %}
    {% set orientation = config.get('orientation', 'row') %}
    {% set sql_header = config.get('sql_header') %}

    {% if distributed_by and replicated %}
        {{ exceptions.raise_compiler_error(
            "Укажите либо distributed_by, либо distributed_replicated."
        ) }}
    {% endif %}

    {% if sql_header %}
        {{ sql_header }}
    {% endif %}

    create {% if temporary %}temporary{% endif %} table {{ relation }}
    {% if orientation == 'column' %}
        with (appendoptimized = true, orientation = column)
    {% endif %}
    as (
        {{ sql }}
    )
    {% if replicated %}
        distributed replicated
    {% elif distributed_by %}
        distributed by ({{ adapter.quote(distributed_by) }})
    {% else %}
        distributed randomly
    {% endif %}
    ;
{% endmacro %}