{# PULSE-managed. Adds standard audit columns to a SELECT. Use as: #}
{# SELECT {{ audit_columns() }}, * FROM ... #}
{% macro audit_columns() %}
    current_timestamp() AS _pulse_processed_at,
    '{{ invocation_id }}' AS _pulse_run_id
{% endmacro %}
