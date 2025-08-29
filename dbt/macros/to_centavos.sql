{% macro to_centavos(expr, places=2) -%}

  CAST(ROUND(CAST({{ expr }} AS NUMERIC) * {{ 10 ** places }}) AS INT64)

{%- endmacro %}
