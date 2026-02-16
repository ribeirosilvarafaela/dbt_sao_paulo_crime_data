{% macro normalize_null_and_cast(column_name, target_type) -%}
case
    when upper(trim(cast({{ column_name }} as string))) in ('', 'NULL') then null
    else safe_cast(trim(cast({{ column_name }} as string)) as {{ target_type }})
end
{%- endmacro %}
