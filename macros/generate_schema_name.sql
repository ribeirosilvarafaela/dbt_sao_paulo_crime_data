{% macro default__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set custom_schema = custom_schema_name | trim if custom_schema_name is not none else none -%}

    {%- if custom_schema is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema }}
    {%- endif -%}
{%- endmacro %}
