{% macro glue__generate_database_name(custom_database_name=none, node=none) -%}
  {%- if custom_database_name is not none -%}
    {{ custom_database_name }}
  {%- else -%}
    {%- set default_database = target.schema -%}
    {{ default_database }}
  {%- endif -%}
{%- endmacro %}
