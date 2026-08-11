{% macro bronze_partition_filter(partition_column) -%}
  {%- set start_date = var('bronze_partition_start_date', '1900-01-01') -%}
  {%- set end_date = var('bronze_partition_end_date', '') -%}

  {{ partition_column }} >= DATE('{{ start_date }}')
  {%- if end_date %}
    AND {{ partition_column }} <= DATE('{{ end_date }}')
  {%- else %}
    AND {{ partition_column }} <= CURRENT_DATE()
  {%- endif %}
{%- endmacro %}
