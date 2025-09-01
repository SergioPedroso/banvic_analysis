{% macro reais_to_centavos(column_name, factor=100) -%}

  CAST(ROUND(CAST({{ column_name }} AS NUMERIC) * {{ factor }}) AS INT64)

{%- endmacro %}
