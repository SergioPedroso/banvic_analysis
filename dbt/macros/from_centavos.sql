{% macro from_centavos(expr) -%}

round(cast({{ expr }} as numeric)/10000, 2)

{%- endmacro %}