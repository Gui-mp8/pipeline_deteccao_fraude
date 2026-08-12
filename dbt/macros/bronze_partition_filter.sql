{% macro bronze_partition_filter(partition_column) -%}
  {{ partition_column }} >= DATE('1900-01-01')
  AND {{ partition_column }} <= CURRENT_DATE()
{%- endmacro %}
