{# PULSE-managed. Delta table materialization for Spark local and deployed runtimes. #}
{% materialization pulse_delta_table, adapter='spark' %}
    {%- set identifier = model['alias'] -%}
    {%- set location_root = config.get('location_root') -%}
    {%- if location_root is none or location_root | trim == '' -%}
        {{ exceptions.raise_compiler_error('pulse_delta_table requires config.location_root') }}
    {%- endif -%}
    {%- set target_location = location_root.rstrip('/') ~ '/' ~ identifier -%}
    {%- set target_relation = api.Relation.create(
        identifier=identifier,
        schema=schema,
        database=database,
        type='table'
    ) -%}

    {{ run_hooks(pre_hooks) }}

    {%- call statement('drop_relation') -%}
        DROP TABLE IF EXISTS {{ target_relation }}
    {%- endcall -%}

    {%- call statement('main') -%}
        CREATE TABLE {{ target_relation }}
        USING DELTA
        LOCATION '{{ target_location }}'
        AS
        {{ compiled_code }}
    {%- endcall -%}

    {{ run_hooks(post_hooks) }}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
