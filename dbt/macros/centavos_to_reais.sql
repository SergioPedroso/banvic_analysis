{% macro centavos_to_reais(column_name, factor=100, decimals=2) -%}

round(cast({{ column_name }} as numeric)/{{ factor }}, {{ decimals }})

{%- endmacro %}