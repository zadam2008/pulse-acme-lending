{# PULSE-managed. Cast that returns NULL on failure instead of erroring. #}
{# Spark-native equivalent of try_cast. #}
{% macro safe_cast(field, target_type) %}
    try_cast({{ field }} AS {{ target_type }})
{% endmacro %}
